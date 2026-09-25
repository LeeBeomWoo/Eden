"""
✨ [변경됨] voice_analysis.py — Cloud Run 분리 아키텍처 버전 (+ 인증방별 인스턴스 분산)

무거운 오디오 처리(포맷 변환, resemblyzer 화자 임베딩 추출, 피치 기반 성별
추정)는 더 이상 여기서 직접 하지 않고, 별도로 띄운 Cloud Run 서비스
(voice-service/app.py)에 HTTP로 위임합니다.

이 모듈(index.py 쪽)은:
  1. LINE에서 오디오 원본 바이트를 받아 그대로 Cloud Run에 넘기고
  2. (관리자 블랙리스트 업로드처럼 동기 흐름이 필요한 경우) 돌아온 결과를 Supabase에 저장하고
  3. 블랙리스트 음성들과 코사인 유사도로 대조
만 담당합니다 — 즉 가벼운 "연결 + 저장" 역할만 합니다.

✨ 방(room_id)별로 여러 Cloud Run 인스턴스에 부하를 나눠 맡기기 위해, VOICE_SERVICE_URLS에
등록된 URL 중 하나를 room_id 해시로 결정합니다(get_cloud_run_url). 신입 음성 접수는
submit_voice_analysis_job()이 '접수'만 시키고(Cloud Run의 /analyze-async), 실제 무거운 분석과
최종 상태 기록은 Cloud Run(app.py)이 백그라운드로 끝낸 뒤 Supabase에 직접 씁니다.
관리자 '/음성업로드'(블랙리스트 수동 등록)는 결과를 바로 써야 하므로 동기식
analyze_audio_via_cloud_run()(/analyze)을 그대로 사용합니다.

필요 환경변수:
  VOICE_SERVICE_URLS    - Cloud Run 서비스 URL 목록, 콤마(,)로 구분 (단일 URL만 있다면
                           VOICE_SERVICE_URL 하나만 넣어도 동작)
  VOICE_SERVICE_API_KEY - voice-service/app.py의 SERVICE_API_KEY와 동일한 값

필요 패키지 (requirements.txt에 추가 필요): requests
(librosa/numpy/resemblyzer 등 무거운 패키지는 전부 voice-service/requirements.txt 쪽에만
있으면 됩니다 — 여기엔 설치할 필요가 없습니다.)

⚠️ 설계 원칙: 이 모듈의 모든 분석 함수는 "실패해도 기존 수동 인증 흐름을 절대 막지
않는다"는 전제로 만들어졌습니다. 예외를 잡아서 부분 결과만 반환하고, 호출부(index.py)는
그 결과가 비어 있어도 신경 쓰지 않고 기존처럼 "운영진이 직접 듣고 판단"하는 흐름을
그대로 진행하면 됩니다.

⚠️ 법적 참고: 목소리는 개인정보보호법상 민감정보(생체인식정보)로 분류될 수 있습니다.
보관 기간, 삭제 요청 처리 방법을 함께 마련해 두는 것을 권장합니다.
"""

import os
import re
import time
import datetime
import threading

import requests


# 환경변수: 쉼표(,)로 구분된 여러 URL 지원 (단일 URL 환경변수와도 호환)
_URLS_ENV = os.environ.get("VOICE_SERVICE_URLS", os.environ.get("VOICE_SERVICE_URL", ""))
VOICE_SERVICE_URLS = [url.strip().rstrip("/") for url in _URLS_ENV.split(",") if url.strip()]
VOICE_SERVICE_API_KEY = os.environ.get("VOICE_SERVICE_API_KEY", "")
# Cloud Run이 무료 티어 안에서 스케일-투-제로로 동작하면 콜드스타트(첫 요청 시
# 모델 로딩)에 시간이 걸릴 수 있어서 넉넉하게 잡습니다.
VOICE_SERVICE_TIMEOUT = 90

# ✨ [추가됨] Cloud Run 인스턴스 하나당(같은 target_url끼리) 이 간격(초) 이상 벌려서
# 요청을 보냅니다. 신입이 한꺼번에 여러 명 들어와 음성을 동시에 보내면 같은 인스턴스로
# 분석 요청이 몰려 리소스(CPU/메모리) 부족으로 500이 나기 쉬운데, 그걸 완화합니다.
# 0으로 설정하면 기존처럼 제한 없이 즉시 보냅니다.
VOICE_SERVICE_MIN_INTERVAL_SEC = float(os.environ.get("VOICE_SERVICE_MIN_INTERVAL_SEC", "1.0"))

_rate_lock = threading.Lock()
_last_call_at = {}  # target_url -> 마지막 요청을 보낸 시각(time.monotonic())


def _throttle(target_url: str):
    """target_url로 나가는 요청 사이에 최소 VOICE_SERVICE_MIN_INTERVAL_SEC초 간격을 보장합니다.
    이 프로세스(Flask 앱)가 계속 떠 있는 서버이기 때문에 인메모리 락만으로도 효과가 있습니다.
    간격이 부족하면 그만큼만 짧게 sleep한 뒤 보냅니다 — 요청 자체를 취소하지는 않습니다."""
    if VOICE_SERVICE_MIN_INTERVAL_SEC <= 0 or not target_url:
        return
    with _rate_lock:
        now = time.monotonic()
        last = _last_call_at.get(target_url, 0.0)
        wait = VOICE_SERVICE_MIN_INTERVAL_SEC - (now - last)
        if wait > 0:
            time.sleep(wait)
        _last_call_at[target_url] = time.monotonic()


def safe_storage_key(text: str, fallback: str = "unknown") -> str:
    """Supabase Storage 경로(오브젝트 키)에 안전하게 쓸 수 있도록 문자열을 정제합니다.

    한글 등 비-ASCII 문자가 storage_path에 그대로 들어가면 supabase-py가 내부적으로
    이 경로를 HTTP 요청 헤더/URL에 실어 보내는 과정에서 인코딩 에러로 업로드가
    실패하는 문제가 있었습니다. 영숫자/일부 기호만 남기고 나머지는 언더스코어로 치환합니다."""
    if not text:
        return fallback
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", str(text)).strip("_")
    return cleaned or fallback


def get_cloud_run_url(identifier: str) -> str:
    """방 ID(또는 유저 ID)를 해싱하여 등록된 Cloud Run 인스턴스 중 하나를 고정 배정합니다.
    같은 identifier는 항상 같은 인스턴스로 가므로, 그 방(또는 유저)의 콜드스타트/캐시
    이점을 어느 정도 유지할 수 있습니다."""
    if not VOICE_SERVICE_URLS:
        return ""
    idx = sum(ord(c) for c in str(identifier)) % len(VOICE_SERVICE_URLS)
    return VOICE_SERVICE_URLS[idx]


# ✨ get_voice_service_url은 get_cloud_run_url의 별칭입니다. (다른 모듈/과거 코드에서
# 이 이름으로 참조하는 곳이 있어 하위 호환을 위해 남겨둡니다.)
get_voice_service_url = get_cloud_run_url


def download_line_audio(message_id: str, configuration) -> bytes:
    """LINE 메시징 API로 오디오 원본 바이트를 다운로드합니다."""
    from linebot.v3.messaging import ApiClient, MessagingApiBlob

    with ApiClient(configuration) as api_client:
        blob_api = MessagingApiBlob(api_client)
        return blob_api.get_message_content(message_id)


def mark_voice_analysis_state(supabase, user_id, state, *, result=None, error=None):
    """user_validations.voice_analysis_state(및 관련 컬럼)를 갱신합니다.
    Cloud Run(app.py)의 _write_analysis_state()와 반드시 같은 컬럼 계약을 써야 합니다:
      voice_analysis_state       ('처리중' | '완료' | '에러')
      voice_analysis_result      jsonb
      voice_analysis_error       text
      voice_analysis_synced_at   timestamptz
    """
    if not supabase or not user_id:
        return
    payload = {
        "voice_analysis_state": state,
        "voice_analysis_synced_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    if result is not None:
        payload["voice_analysis_result"] = result
    if error is not None:
        payload["voice_analysis_error"] = error
    try:
        supabase.table('user_validations').update(payload).eq('user_id', user_id).execute()
    except Exception as e:
        print(f"⚠️ voice_analysis_state 업데이트 실패: {e}")


def submit_voice_analysis_job(supabase, configuration, *, message_id, user_id, nickname="", room_id=""):
    """오디오 도착 시점(index.py의 handle_audio)에 호출됩니다. room_id로 결정된 Cloud Run
    인스턴스의 /analyze-async에 '접수'만 시키고 결과는 기다리지 않습니다 — 실제 분석/저장/
    최종 상태 기록은 Cloud Run(app.py)이 백그라운드로 전부 끝낸 뒤 Supabase에 직접 씁니다.

    이 함수는 예외를 던지지 않습니다(호출부의 즉시 응답 흐름을 막지 않기 위함). 접수 자체가
    실패하면(다운로드 실패, URL 미설정, 네트워크 에러 등) 상태를 바로 '에러'로 남겨서
    '문제없음' 답장 시 즉시 🔴로 안내되고 운영진 수동 확인으로 넘어가게 합니다.
    """
    mark_voice_analysis_state(supabase, user_id, "처리중")

    try:
        audio_bytes = download_line_audio(message_id, configuration)
    except Exception as e:
        mark_voice_analysis_state(supabase, user_id, "에러", error=f"LINE 오디오 다운로드 실패: {e}")
        return

    target_url = get_cloud_run_url(room_id or user_id)
    if not target_url:
        mark_voice_analysis_state(supabase, user_id, "에러", error="VOICE_SERVICE_URLS 미설정/매핑 실패")
        return

    headers = {"X-API-Key": VOICE_SERVICE_API_KEY} if VOICE_SERVICE_API_KEY else {}
    _throttle(target_url)
    try:
        requests.post(
            f"{target_url}/analyze-async",
            files={"audio": ("audio", audio_bytes)},
            data={"user_id": user_id, "nickname": nickname, "message_id": message_id},
            headers=headers,
            timeout=(3.0, 0.1),  # 연결 3초, 응답 대기 0.1초 컷 (접수만 확인하고 바로 끊음)
        )
    except requests.exceptions.ReadTimeout:
        pass  # 0.1초 만에 끊어서 발생하는 정상 동작 — 접수 자체는 이미 서버에 도달했다고 간주
    except Exception as e:
        mark_voice_analysis_state(supabase, user_id, "에러", error=f"클라우드런 접수 실패: {e}")
        return
    # 접수 성공 시엔 상태를 그대로 '처리중'으로 둔다. 최종 '완료'/'에러'는
    # Cloud Run(app.py)이 백그라운드 분석을 마친 뒤 Supabase에 직접 쓴다.


def analyze_audio_via_cloud_run(raw_audio_bytes: bytes, room_id: str = "") -> dict:
    """동기식 음성 분석 요청. room_id로 결정된 Cloud Run 인스턴스의 /analyze를 호출하고
    결과가 올 때까지 기다립니다 (관리자 '/음성업로드'처럼 결과를 바로 써야 하는 흐름용).

    반환: {"embedding": [...], "estimated_gender": "남"|"여"|None, "pitch_hz": float|None, ...}
    실패 시 예외를 던집니다 (호출부가 잡아서 처리).
    """
    target_url = get_cloud_run_url(room_id) or (VOICE_SERVICE_URLS[0] if VOICE_SERVICE_URLS else "")
    if not target_url:
        raise RuntimeError("VOICE_SERVICE_URLS 환경변수가 설정되지 않았습니다.")

    _throttle(target_url)
    resp = requests.post(
        f"{target_url}/analyze",
        files={"audio": ("audio", raw_audio_bytes)},
        headers={"X-API-Key": VOICE_SERVICE_API_KEY} if VOICE_SERVICE_API_KEY else {},
        timeout=VOICE_SERVICE_TIMEOUT,
    )
    if not resp.ok:
        # raise_for_status()만 쓰면 "500 Server Error"까지만 보이고 voice-service가 실제로
        # 왜 실패했는지(에러 본문)는 사라진다. 운영진이 원인을 바로 파악할 수 있도록 응답
        # 본문 일부를 예외 메시지에 함께 담는다.
        body_snippet = (resp.text or "")[:500]
        raise RuntimeError(
            f"voice-service 응답 오류 ({resp.status_code}) {target_url}/analyze — {body_snippet}"
        )
    return resp.json()


def upload_voice_sample(supabase, file_bytes: bytes, storage_path: str, bucket: str = "voice-samples") -> str:
    """Supabase Storage(비공개 버킷)에 원본 오디오를 업로드하고 저장 경로를 반환합니다.
    버킷은 미리 만들어져 있어야 합니다. 변환 없이 원본 그대로 저장합니다(변환은 Cloud Run
    쪽에서 분석용으로만 임시로 수행됨)."""
    supabase.storage.from_(bucket).upload(
        path=storage_path,
        file=file_bytes,
        file_options={"content-type": "audio/m4a", "upsert": "true"},
    )
    return storage_path


def insert_voice_profile(
    supabase,
    *,
    embedding,
    storage_path,
    nickname="",
    user_id=None,
    estimated_gender=None,
    pitch_hz=None,
    source="new_member",
):
    """voice_profiles 테이블에 한 건 저장합니다."""
    row = {
        "user_id": user_id,
        "nickname": nickname,
        "storage_path": storage_path,
        "embedding": embedding,
        "estimated_gender": estimated_gender,
        "pitch_hz": pitch_hz,
        "source": source,
    }
    return supabase.table("voice_profiles").insert(row).execute()


def match_blacklist_voices(supabase, embedding, match_count: int = 5):
    """블랙리스트로 등록된 음성들 중 이 임베딩과 가장 유사한 것들을 찾습니다.
    Postgres 쪽 match_voice_profiles() 함수(코사인 유사도, pgvector)를 RPC로 호출합니다.

    반환: [{"id":.., "nickname":.., "gender":.., "status":.., "similarity": 0.0~1.0}, ...] (유사도 내림차순)
    """
    try:
        res = supabase.rpc(
            "match_voice_profiles",
            {"query_embedding": embedding, "match_count": match_count},
        ).execute()
        return res.data or []
    except Exception as e:
        print(f"⚠️ 블랙리스트 음성 매칭 RPC 실패: {e}")
        return []


# ==========================================
# ✨ 관리자방 '/음성업로드' — 운영진이 직접 제출한 음성을 블랙리스트에 수동 등록
# ==========================================
def insert_blacklist_validation(supabase, *, nickname="", gender="", black_reason="", registered_by=""):
    """관리자가 '/음성업로드'로 수동 등록하는 블랙리스트 인물을 user_validations 테이블에 저장합니다.
    (별도의 members 테이블은 쓰지 않고, 기존 신입 검증에 쓰는 user_validations를 그대로 재사용합니다.)

    실제 LINE 유저가 아니므로 고유한 가짜 user_id(BL_타임스탬프_닉네임)를 발급해서 사용합니다.
    이렇게 하면 기존 1번 양식 제출 시 닉네임/블랙사유 중복검사 로직이 이 레코드를 그대로
    블랙리스트로 잡아냅니다.

    반환: 생성된 user_id (실패 시 None)
    """
    if not supabase:
        return None

    synthetic_user_id = f"BL_{int(time.time())}_{(nickname or 'unknown').strip()}"
    kst = datetime.timezone(datetime.timedelta(hours=9))
    row = {
        "user_id": synthetic_user_id,
        "nickname": nickname,
        "gender": gender,
        "black_reason": black_reason,
        "status": "블랙",
        "entry_date": datetime.datetime.now(kst).strftime("%Y-%m-%d"),
        "details": {"registered_by": registered_by, "source": "admin_voice_upload"},
    }
    try:
        supabase.table("user_validations").insert(row).execute()
        return synthetic_user_id
    except Exception as e:
        print(f"⚠️ 블랙리스트 user_validations 등록 실패: {e}")
        return None


def process_admin_blacklist_voice_upload(
    supabase, configuration, *, message_id, nickname, gender, black_reason="", registered_by="",
    storage_prefix="admin_blacklist", room_id=None,
):
    """운영진이 '/음성업로드'로 직접 제출한 음성을 분석해 블랙리스트(user_validations + voice_profiles)에
    등록하는 파이프라인입니다.
    - 가짜 user_id로 user_validations에 블랙 레코드를 새로 만들어서 연결하고
    - voice_profiles.source를 'admin_blacklist_upload'로 남깁니다.
    실패해도 예외를 던지지 않고 부분 결과(dict)를 돌려줍니다.
    """
    result = {
        "blacklist_user_id": None,
        "estimated_gender": None,
        "pitch_hz": None,
        "matches": [],
        "storage_path": None,
        "error": None,
    }
    try:
        raw_bytes = download_line_audio(message_id, configuration)
        analysis = analyze_audio_via_cloud_run(raw_bytes, room_id=room_id)
        embedding = analysis.get("embedding")
        result["estimated_gender"] = analysis.get("estimated_gender")
        result["pitch_hz"] = analysis.get("pitch_hz")

        # 등록 전에 기존 블랙리스트와 먼저 대조 (같은 인물 중복 등록 여부 확인용)
        if embedding:
            result["matches"] = match_blacklist_voices(supabase, embedding)

        blacklist_user_id = insert_blacklist_validation(
            supabase, nickname=nickname, gender=gender,
            black_reason=black_reason, registered_by=registered_by,
        )
        result["blacklist_user_id"] = blacklist_user_id

        # storage_path는 항상 ASCII로만 구성한다 (한글 닉네임 등이 섞이면 Supabase Storage
        # 업로드가 실패할 수 있음).
        storage_path = f"{storage_prefix}/{safe_storage_key(blacklist_user_id or nickname)}_{int(time.time())}.m4a"
        upload_voice_sample(supabase, raw_bytes, storage_path)
        result["storage_path"] = storage_path

        if embedding:
            insert_voice_profile(
                supabase, embedding=embedding, storage_path=storage_path,
                nickname=nickname, user_id=blacklist_user_id,
                estimated_gender=result["estimated_gender"], pitch_hz=result["pitch_hz"],
                source="admin_blacklist_upload",
            )
    except Exception as e:
        print(f"⚠️ 관리자 블랙리스트 음성 업로드 파이프라인 실패: {e}")
        result["error"] = str(e)

    return result

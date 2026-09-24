"""
✨ [변경됨] voice_analysis.py — Cloud Run 분리 아키텍처 버전

무거운 오디오 처리(포맷 변환, resemblyzer 화자 임베딩 추출, 피치 기반 성별
추정)는 더 이상 Vercel에서 직접 하지 않고, 별도로 띄운 Cloud Run 서비스
(voice-service/)에 HTTP로 위임합니다.

Vercel(index.py) 쪽은:
  1. LINE에서 오디오 원본 바이트를 받아 그대로 Cloud Run에 넘기고
  2. 돌아온 결과(임베딩/성별)를 Supabase에 저장하고
  3. 블랙리스트 음성들과 코사인 유사도로 대조
만 담당합니다 — 즉 가벼운 "연결 + 저장" 역할만 합니다.

필요 환경변수:
  VOICE_SERVICE_URL     - Cloud Run 서비스 URL (예: https://voice-service-xxxxx.a.run.app)
  VOICE_SERVICE_API_KEY - voice-service/app.py의 SERVICE_API_KEY와 동일한 값

필요 패키지 (requirements.txt에 추가 필요): requests
(librosa/numpy/imageio-ffmpeg 등 무거운 패키지는 이제 Vercel에 설치할 필요가
없습니다 — 전부 voice-service/requirements.txt 쪽으로 옮겨졌습니다.)

⚠️ 설계 원칙은 이전과 동일합니다: 이 모듈의 모든 분석 함수는 "실패해도 기존
수동 인증 흐름을 절대 막지 않는다"는 전제로 만들어졌습니다.
analyze_new_member_voice()는 내부에서 발생하는 모든 예외(Cloud Run 서비스가
잠들어 있다가 깨어나는 데 오래 걸리는 경우 포함)를 잡아서 부분 결과만
반환하고, 호출부(index.py)는 그 결과가 비어 있어도 신경 쓰지 않고 기존처럼
"운영진이 직접 듣고 판단"하는 흐름을 그대로 진행하면 됩니다.

⚠️ 법적 참고: 목소리는 개인정보보호법상 민감정보(생체인식정보)로 분류될
수 있습니다. 보관 기간, 삭제 요청 처리 방법을 함께 마련해 두는 것을
권장합니다.
"""

import os
import re
import time
import datetime
import hashlib  # 파일 상단 import 구역에 추가
import requests


def safe_storage_key(text: str, fallback: str = "unknown") -> str:
    """Supabase Storage 경로(오브젝트 키)에 안전하게 쓸 수 있도록 문자열을 정제합니다.

    ✨ [추가됨] 한글 등 비-ASCII 문자가 storage_path에 그대로 들어가면 supabase-py가
    내부적으로 이 경로를 HTTP 요청 헤더/URL에 실어 보내는 과정에서 인코딩 에러
    (latin-1/UnicodeEncodeError 등)로 업로드가 실패하는 문제가 있었습니다.
    한글(닉네임 등)은 영숫자/일부 기호만 남기고 전부 언더스코어로 치환합니다."""
    if not text:
        return fallback
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", str(text)).strip("_")
    return cleaned or fallback

# ✨ [변경됨] 단일 VOICE_SERVICE_URL → 콤마 구분 리스트로 확장 (하위호환: 리스트가 비어있으면
def _parse_url_list(raw: str) -> list:
    """콤마로 구분된 URL 목록을 파싱합니다. 앞뒤 공백/슬래시 제거."""
    return [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]

_raw_urls = os.environ.get("VOICE_SERVICE_URLS", "")
VOICE_SERVICE_URLS = _parse_url_list(_raw_urls)
if not VOICE_SERVICE_URLS:
    # ✨ [수정됨] 변수명을 VOICE_SERVICE_URL(기존 단수형)로 잘못 넣어도, 콤마가 섞여 있으면
    # 여기서도 동일하게 split해서 방어한다 (이전 버전의 버그: split 없이 통째로 넣어서
    # "url1,url2,url3"이 하나의 target_url로 취급되던 문제 수정)
    VOICE_SERVICE_URLS = _parse_url_list(os.environ.get("VOICE_SERVICE_URL", ""))

if not VOICE_SERVICE_URLS:
    print("⚠️ VOICE_SERVICE_URLS(또는 VOICE_SERVICE_URL) 환경변수가 설정되지 않았습니다.")
else:
    print(f"✅ voice-service 인스턴스 {len(VOICE_SERVICE_URLS)}개 로드됨: {VOICE_SERVICE_URLS}")
VOICE_SERVICE_API_KEY = os.environ.get("VOICE_SERVICE_API_KEY", "")
# Cloud Run이 무료 티어 안에서 스케일-투-제로로 동작하면 콜드스타트(첫 요청 시
# 모델 로딩)에 시간이 걸릴 수 있어서 넉넉하게 잡습니다.
VOICE_SERVICE_TIMEOUT = 90

def get_voice_service_url(room_id: str = None) -> str:
    """room_id를 해시해서 VOICE_SERVICE_URLS 중 하나를 결정적으로 골라 돌려줍니다.
    같은 방은 항상 같은 인스턴스로 가고(웜업 효율 유지), 방이 다르면 3대에 분산됩니다.
    room_id가 없으면(관리자방 수동 업로드 등) 항상 첫 번째 URL을 씁니다.
    설정된 URL이 하나도 없으면 빈 문자열을 돌려줍니다.
    """
    if not VOICE_SERVICE_URLS:
        return ""
    if len(VOICE_SERVICE_URLS) == 1 or not room_id:
        return VOICE_SERVICE_URLS[0]
    digest = hashlib.md5(str(room_id).encode("utf-8")).hexdigest()
    idx = int(digest, 16) % len(VOICE_SERVICE_URLS)
    return VOICE_SERVICE_URLS[idx]

def analyze_audio_via_cloud_run(raw_audio_bytes: bytes, room_id: str = None) -> dict:
    target_url = get_voice_service_url(room_id)
    if not target_url:
        raise RuntimeError("VOICE_SERVICE_URLS(또는 VOICE_SERVICE_URL) 환경변수가 설정되지 않았습니다.")
    """Cloud Run의 /analyze 엔드포인트에 원본 오디오를 보내고 결과를 받아옵니다.

    반환: {"embedding": [...], "estimated_gender": "남"|"여"|None, "pitch_hz": float|None}
    실패 시 예외를 던집니다 (호출부 analyze_new_member_voice가 잡아서 처리).
    """

    resp = requests.post(
        f"{target_url}/analyze",
        files={"audio": ("audio", raw_audio_bytes)},
        headers={"X-API-Key": VOICE_SERVICE_API_KEY} if VOICE_SERVICE_API_KEY else {},
        timeout=VOICE_SERVICE_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


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


def submit_voice_analysis_job(supabase, configuration, *, message_id, user_id, nickname="", room_id=None):
    mark_voice_analysis_state(supabase, user_id, "처리중")
    """오디오 도착 시점(index.py의 handle_audio)에 호출됩니다. Cloud Run의 /analyze-async에
    '접수'만 시키고 결과는 기다리지 않습니다 — 실제 분석/저장/최종 상태 기록은 Cloud Run(app.py)이
    백그라운드로 전부 끝낸 뒤 Supabase에 직접 씁니다.

    이 함수는 예외를 던지지 않습니다(호출부의 즉시 응답 흐름을 막지 않기 위함). 접수 자체가
    실패하면(다운로드 실패, URL 미설정, 네트워크 에러 등) 상태를 바로 '에러'로 남겨서
    '문제없음' 답장 시 즉시 🔴로 안내되고 운영진 수동 확인으로 넘어가게 합니다.
    """

    try:
        audio_bytes = download_line_audio(message_id, configuration)
    except Exception as e:
        mark_voice_analysis_state(supabase, user_id, "에러", error=f"LINE 오디오 다운로드 실패: {e}")
        return

    target_url = get_voice_service_url(room_id)
    if not target_url:
        mark_voice_analysis_state(supabase, user_id, "에러", error="VOICE_SERVICE_URLS 미설정")
        return

    headers = {"X-API-Key": VOICE_SERVICE_API_KEY} if VOICE_SERVICE_API_KEY else {}
    try:
        resp = requests.post(
            f"{target_url}/analyze-async",
            files={"audio": ("audio", audio_bytes)},
            data={"user_id": user_id, "nickname": nickname, "message_id": message_id},
            headers=headers,
            timeout=(10, 20),
        )
        resp.raise_for_status()
    except requests.exceptions.ReadTimeout:
        # ✅ [수정] 오디오 전송(요청 자체)은 성공적으로 끝났고, Cloud Run이 이어서 분석을 계속
        # 진행 중인 상태다. 접수 실패가 아니므로 '에러'로 남기지 않는다 — 상태는 '처리중' 그대로
        # 둔다. 최종 '완료'/'에러'는 Cloud Run이 분석을 마친 뒤 Supabase에 직접 기록한다.
        pass
    except Exception as e:
        # 연결 자체가 안 됐거나(URL 오류, 네트워크 문제 등) 4xx/5xx 응답을 받은 경우 — 진짜 접수 실패.
        mark_voice_analysis_state(supabase, user_id, "에러", error=f"클라우드런 접수 실패: {e}")
        return
    # 접수 성공 시엔 상태를 그대로 '처리중'으로 둔다. 최종 '완료'/'에러'는
    # Cloud Run(app.py)이 백그라운드 분석을 마친 뒤 Supabase에 직접 쓴다.


def upload_voice_sample(supabase, file_bytes: bytes, storage_path: str, bucket: str = "voice-samples") -> str:
    """Supabase Storage(비공개 버킷)에 원본 오디오를 업로드하고 저장 경로를 반환합니다.
    버킷은 미리 만들어져 있어야 합니다 (README 참고). 변환 없이 원본 그대로
    저장합니다 (변환은 Cloud Run 쪽에서 분석용으로만 임시로 수행됨)."""
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
    """voice_profiles 테이블에 한 건 저장합니다.
    ✨ [수정됨] member_id 필드 제거 — voice-service/app.py의 insert_voice_profile()이
    실제 스키마 기준(app.py가 백그라운드 분석 결과를 직접 저장하는 쪽이므로 이게 정본)이며,
    거기엔 member_id 컬럼이 없습니다. 여기서도 동일하게 맞춥니다."""
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


def analyze_new_member_voice(
    supabase,
    configuration,
    *,
    message_id,
    user_id,
    nickname,
    storage_prefix="new_member",
):
    """신입이 보낸 음성 메시지 한 건을 분석하는 전체 파이프라인입니다.

    실패해도 예외를 던지지 않고 부분 결과(dict)를 돌려줍니다 — 호출부는
    결과가 비어있어도 기존 수동 인증 흐름을 그대로 진행하면 됩니다.

    반환 예:
    {
        "estimated_gender": "여", "pitch_hz": 210.3,
        "matches": [{"nickname": "골드", "similarity": 0.93, ...}, ...],
        "storage_path": "new_member/Uxxxx_1234567890.m4a",
        "error": None,
    }
    """
    result = {
        "estimated_gender": None,
        "pitch_hz": None,
        "matches": [],
        "storage_path": None,
        "error": None,
    }
    try:
        raw_bytes = download_line_audio(message_id, configuration)

        analysis = analyze_audio_via_cloud_run(raw_bytes)
        embedding = analysis.get("embedding")
        result["estimated_gender"] = analysis.get("estimated_gender")
        result["pitch_hz"] = analysis.get("pitch_hz")

        # ✨ [수정됨] storage_path는 항상 ASCII로만 구성한다 (한글 닉네임 등이 섞이면
        # Supabase Storage 업로드가 실패할 수 있음). LINE user_id는 보통 이미 ASCII지만
        # 방어적으로 한 번 더 정제한다.
        storage_path = f"{storage_prefix}/{safe_storage_key(user_id)}_{int(time.time())}.m4a"
        upload_voice_sample(supabase, raw_bytes, storage_path)
        result["storage_path"] = storage_path

        if embedding:
            insert_voice_profile(
                supabase,
                embedding=embedding,
                storage_path=storage_path,
                nickname=nickname,
                user_id=user_id,
                estimated_gender=result["estimated_gender"],
                pitch_hz=result["pitch_hz"],
                source="new_member",
            )
            result["matches"] = match_blacklist_voices(supabase, embedding)
    except Exception as e:
        print(f"⚠️ 음성 자동분석 파이프라인 실패(수동 인증 흐름은 계속 진행됨): {e}")
        result["error"] = str(e)

    return result


# ==========================================
# ✨ [추가됨] 관리자방 '/음성업로드' — 운영진이 직접 제출한 음성을 블랙리스트에 수동 등록
# ==========================================
def insert_blacklist_validation(supabase, *, nickname="", gender="", black_reason="", registered_by=""):
    """관리자가 '/음성업로드'로 수동 등록하는 블랙리스트 인물을 user_validations 테이블에 저장합니다.
    (별도의 members 테이블은 쓰지 않고, 기존 신입 검증에 쓰는 user_validations를 그대로 재사용합니다.)

    실제 LINE 유저가 아니므로 고유한 가짜 user_id(BL_타임스탬프_닉네임)를 발급해서 사용합니다.
    이렇게 하면 기존 1번 양식 제출 시 닉네임/블랙사유 중복검사 로직(handle_message의
    "DB 중복/블랙리스트 조회" 구간)이 이 레코드를 그대로 블랙리스트로 잡아냅니다.

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
    등록하는 파이프라인입니다. analyze_new_member_voice()와 거의 동일하되,
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

        # ✨ [수정됨] blacklist_user_id(=BL_타임스탬프_닉네임)에 한글 닉네임이 그대로 들어있거나,
        # 그마저 없어 nickname을 바로 쓰는 경우 모두 storage_path에 한글이 섞여 Supabase
        # Storage 업로드가 실패했다. safe_storage_key로 한글/특수문자를 언더스코어로 치환한다.
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

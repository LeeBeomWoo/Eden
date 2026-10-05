"""
voice_analysis.py — 오라클 VM 분리 아키텍처 버전 (+ 인증방별 인스턴스 분산)

무거운 오디오 처리(포맷 변환, resemblyzer 화자 임베딩 추출, 피치/성별 추정)는 여기서 하지 않고
별도 서버(voice-service/app.py, 오라클 VM)에 HTTP로 위임합니다.

이 모듈(index.py 쪽)의 역할:
  1. LINE에서 오디오 원본 바이트를 받아 voice-service에 넘기고
  2. (관리자 블랙리스트 업로드처럼 동기 흐름이 필요한 경우) 돌아온 결과를 Supabase에 저장하고
  3. 블랙리스트 음성들과 코사인 유사도로 대조

접수/분석 방식
  - 신입 음성: submit_voice_analysis_job() 이 /analyze-async 에 '접수'만 시키면 서버가 즉시 202를
    돌려주고, 실제 분석/저장/최종 상태 기록은 서버가 백그라운드로 끝낸 뒤 Supabase에 직접 씁니다.
    서버가 대기 상한 초과(429)나 오류를 돌려주면 상태를 바로 '에러'로 남깁니다.
  - 관리자 '/음성업로드': 결과를 바로 써야 하므로 동기식 analyze_audio_via_cloud_run()(/analyze)을 사용합니다.

필요 환경변수:
  VOICE_SERVICE_URLS    - voice-service URL 목록, 콤마(,)로 구분 (하나뿐이면 VOICE_SERVICE_URL만 넣어도 동작)
  VOICE_SERVICE_API_KEY - voice-service의 VOICE_SERVICE_API_KEY와 동일한 값

필요 패키지: requests

⚠️ 설계 원칙: 이 모듈의 분석 함수는 "실패해도 기존 수동 인증 흐름을 절대 막지 않는다"는 전제입니다.

⚠️ 법적 참고: 목소리는 개인정보보호법상 민감정보(생체인식정보)로 분류될 수 있습니다.
보관 기간, 삭제 요청 처리 방법을 함께 마련해 두는 것을 권장합니다.
"""

import os
import re
import time
import zlib
import datetime

import requests


_URLS_ENV = os.environ.get("VOICE_SERVICE_URLS", os.environ.get("VOICE_SERVICE_URL", ""))
VOICE_SERVICE_URLS = [url.strip().rstrip("/") for url in _URLS_ENV.split(",") if url.strip()]
VOICE_SERVICE_API_KEY = os.environ.get("VOICE_SERVICE_API_KEY", "")

# 동기 분석(/analyze)용 읽기 타임아웃. 서버가 분석 슬롯을 기다리는 시간까지 포함해 넉넉히 잡습니다.
VOICE_SERVICE_TIMEOUT = 120

# 분석 상태가 이 시간(분) 넘게 '처리중'이면 서버가 죽었거나 작업이 유실된 것으로 보고 '에러'로 취급합니다.
VOICE_ANALYSIS_STALE_MINUTES = int(os.environ.get("VOICE_ANALYSIS_STALE_MINUTES", "5"))


def safe_storage_key(text: str, fallback: str = "unknown") -> str:
    """Supabase Storage 경로(오브젝트 키)에 안전하게 쓸 수 있도록 문자열을 정제합니다.
    영숫자/_/- 만 남기고 나머지는 언더스코어로 치환합니다."""
    if not text:
        return fallback
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "_", str(text)).strip("_")
    return cleaned or fallback


def get_cloud_run_url(identifier: str) -> str:
    """방 ID(또는 유저 ID)를 해싱하여 등록된 서버 URL 중 하나를 고정 배정합니다.
    (함수 이름은 하위 호환을 위해 그대로 둡니다.) crc32 해시라 분산이 고르고 프로세스를
    다시 시작해도 같은 identifier는 같은 서버로 갑니다."""
    if not VOICE_SERVICE_URLS:
        return ""
    idx = zlib.crc32(str(identifier).encode("utf-8")) % len(VOICE_SERVICE_URLS)
    return VOICE_SERVICE_URLS[idx]


# 다른 모듈/과거 코드에서 이 이름으로 참조하는 곳이 있어 하위 호환을 위해 남겨둡니다.
get_voice_service_url = get_cloud_run_url


def download_line_audio(message_id: str, configuration) -> bytes:
    """LINE 메시징 API로 오디오 원본 바이트를 다운로드합니다."""
    from linebot.v3.messaging import ApiClient, MessagingApiBlob

    with ApiClient(configuration) as api_client:
        blob_api = MessagingApiBlob(api_client)
        return blob_api.get_message_content(message_id)


def mark_voice_analysis_state(supabase, user_id, state, *, result=None, error=None):
    """user_validations.voice_analysis_state(및 관련 컬럼)를 갱신합니다.
    voice-service(app.py)의 _write_analysis_state()와 같은 컬럼 계약을 씁니다:
      voice_analysis_state       ('처리중' | '완료' | '에러')
      voice_analysis_result      jsonb
      voice_analysis_error       text
      voice_analysis_synced_at   timestamptz

    '처리중'으로 새로 시작할 때는 이전 시도의 결과/에러가 남아 있지 않도록 비웁니다.
    """
    if not supabase or not user_id:
        return
    payload = {
        "voice_analysis_state": state,
        "voice_analysis_synced_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    if state == "처리중":
        payload["voice_analysis_result"] = None
        payload["voice_analysis_error"] = None
    if result is not None:
        payload["voice_analysis_result"] = result
    if error is not None:
        payload["voice_analysis_error"] = error
    try:
        supabase.table('user_validations').update(payload).eq('user_id', user_id).execute()
    except Exception as e:
        print(f"⚠️ voice_analysis_state 업데이트 실패: {e}")


def is_voice_analysis_stale(state, synced_at, stale_minutes: int = None) -> bool:
    """'처리중'인데 마지막 갱신 후 너무 오래 지났는지 확인합니다.
    index.py에서 '문제없음' 응답 시 DB 조회 결과에 이 함수를 적용해서, True면 🟡(대기중) 대신
    🔴(에러)로 안내하세요. (서버 재시작/OOM 등으로 작업이 유실되면 '처리중'에 고착되기 때문)

    synced_at: Supabase가 돌려주는 ISO 문자열 또는 datetime
    """
    if state != "처리중" or not synced_at:
        return False
    limit = stale_minutes if stale_minutes is not None else VOICE_ANALYSIS_STALE_MINUTES
    try:
        if isinstance(synced_at, str):
            ts = datetime.datetime.fromisoformat(synced_at.replace("Z", "+00:00"))
        else:
            ts = synced_at
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        age = datetime.datetime.now(datetime.timezone.utc) - ts
        return age > datetime.timedelta(minutes=limit)
    except Exception:
        return False


def submit_voice_analysis_job(supabase, configuration, *, message_id, user_id, nickname="", room_id=""):
    """오디오 도착 시점(index.py의 handle_audio)에 호출됩니다. room_id로 결정된 서버의
    /analyze-async에 '접수'만 시키고 결과는 기다리지 않습니다 — 서버가 즉시 202를 돌려주고,
    실제 분석/저장/최종 상태 기록은 서버가 백그라운드로 끝낸 뒤 Supabase에 직접 씁니다.

    이 함수는 예외를 던지지 않습니다. 접수 자체가 실패하면(다운로드 실패, URL 미설정, 네트워크
    에러, 서버 대기 상한 429 등) 상태를 바로 '에러'로 남겨서 '문제없음' 답장 시 즉시 🔴로
    안내되고 운영진 수동 확인으로 넘어가게 합니다.
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
    try:
        resp = requests.post(
            f"{target_url}/analyze-async",
            files={"audio": ("audio", audio_bytes)},
            data={"user_id": user_id, "nickname": nickname, "message_id": message_id},
            headers=headers,
            timeout=(3.0, 15.0),  # 서버는 접수 즉시 202를 반환하므로 짧게 잡아도 됩니다
        )
        if not resp.ok:
            mark_voice_analysis_state(
                supabase, user_id, "에러",
                error=f"분석 서버 접수 실패 ({resp.status_code}): {(resp.text or '')[:200]}",
            )
            return
    except Exception as e:
        mark_voice_analysis_state(supabase, user_id, "에러", error=f"분석 서버 접수 실패: {e}")
        return

    # 접수 성공 시엔 상태를 그대로 '처리중'으로 둡니다. 최종 '완료'/'에러'는
    # 서버가 백그라운드 분석을 마친 뒤 Supabase에 직접 씁니다.


def analyze_audio_via_cloud_run(raw_audio_bytes: bytes, room_id: str = "") -> dict:
    """동기식 음성 분석 요청. room_id로 결정된 서버의 /analyze를 호출하고 결과가 올 때까지
    기다립니다 (관리자 '/음성업로드'처럼 결과를 바로 써야 하는 흐름용).

    반환: {"embedding": [...], "estimated_gender": "남"|"여"|None, "pitch_hz": float|None, ...}
    실패 시 예외를 던집니다 (호출부가 잡아서 처리). 서버가 바쁘면(429) 그 사실이 메시지에 담깁니다.
    """
    target_url = get_cloud_run_url(room_id or "") or (VOICE_SERVICE_URLS[0] if VOICE_SERVICE_URLS else "")
    if not target_url:
        raise RuntimeError("VOICE_SERVICE_URLS 환경변수가 설정되지 않았습니다.")

    resp = requests.post(
        f"{target_url}/analyze",
        files={"audio": ("audio", raw_audio_bytes)},
        headers={"X-API-Key": VOICE_SERVICE_API_KEY} if VOICE_SERVICE_API_KEY else {},
        timeout=(5.0, VOICE_SERVICE_TIMEOUT),
    )
    if not resp.ok:
        body_snippet = (resp.text or "")[:500]
        hint = " (서버가 다른 분석을 처리 중입니다. 잠시 후 다시 시도하세요)" if resp.status_code == 429 else ""
        raise RuntimeError(
            f"voice-service 응답 오류 ({resp.status_code}) {target_url}/analyze — {body_snippet}{hint}"
        )
    return resp.json()


def upload_voice_sample(supabase, file_bytes: bytes, storage_path: str, bucket: str = "voice-samples") -> str:
    """Supabase Storage(비공개 버킷)에 원본 오디오를 업로드하고 저장 경로를 반환합니다.
    버킷은 미리 만들어져 있어야 합니다."""
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

    ⚠️ 서버(app.py)는 반환 행의 user_id, source, similarity 컬럼을 사용합니다.
    match_voice_profiles()가 이 컬럼들을 반환하는지 꼭 확인하세요.

    반환: [{"user_id":.., "source":.., "nickname":.., "similarity": 0.0~1.0, ...}, ...] (유사도 내림차순)
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
# 관리자방 '/음성업로드' — 운영진이 직접 제출한 음성을 블랙리스트에 수동 등록
# ==========================================
def insert_blacklist_validation(supabase, *, nickname="", gender="", black_reason="", registered_by=""):
    """관리자가 '/음성업로드'로 수동 등록하는 블랙리스트 인물을 user_validations 테이블에 저장합니다.
    실제 LINE 유저가 아니므로 고유한 가짜 user_id(BL_타임스탬프_닉네임)를 발급해서 사용합니다.

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
    등록하는 파이프라인입니다. 실패해도 예외를 던지지 않고 부분 결과(dict)를 돌려줍니다.
    """
    result = {
        "blacklist_user_id": None,
        "estimated_gender": None,
        "pitch_hz": None,
        "gender_uncertain": False,
        "gender_score": None,
        "gender_signals": {},
        "gender_note": None,
        "matches": [],
        "storage_path": None,
        "error": None,
    }
    try:
        raw_bytes = download_line_audio(message_id, configuration)
        analysis = analyze_audio_via_cloud_run(raw_bytes, room_id=room_id or "")
        embedding = analysis.get("embedding")
        result["estimated_gender"] = analysis.get("estimated_gender")
        result["pitch_hz"] = analysis.get("pitch_hz")
        result["gender_uncertain"] = bool(analysis.get("gender_uncertain"))
        result["gender_score"] = analysis.get("gender_score")
        result["gender_signals"] = analysis.get("gender_signals") or {}
        result["gender_note"] = analysis.get("gender_note")

        # 등록 전에 기존 블랙리스트와 먼저 대조 (같은 인물 중복 등록 여부 확인용)
        if embedding:
            result["matches"] = match_blacklist_voices(supabase, embedding)

        blacklist_user_id = insert_blacklist_validation(
            supabase, nickname=nickname, gender=gender,
            black_reason=black_reason, registered_by=registered_by,
        )
        result["blacklist_user_id"] = blacklist_user_id

        # storage_path는 항상 ASCII로만 구성 (한글 닉네임 등이 섞이면 업로드가 실패할 수 있음)
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

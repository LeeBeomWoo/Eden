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


# 환경변수: 쉼표(,)로 구분된 여러 URL 지원 (단일 URL 환경변수와도 호환)
_URLS_ENV = os.environ.get("VOICE_SERVICE_URLS", os.environ.get("VOICE_SERVICE_URL", ""))
VOICE_SERVICE_URLS = [url.strip() for url in _URLS_ENV.split(",") if url.strip()]
VOICE_SERVICE_API_KEY = os.environ.get("VOICE_SERVICE_API_KEY", "")
VOICE_SERVICE_TIMEOUT = 90


def get_cloud_run_url(identifier: str) -> str:
    """유저 ID(혹은 방 ID)를 해싱하여 등록된 Cloud Run 계정 중 하나를 지정합니다."""
    if not VOICE_SERVICE_URLS:
        return ""
    idx = sum(ord(c) for c in str(identifier)) % len(VOICE_SERVICE_URLS)
    return VOICE_SERVICE_URLS[idx]


def download_line_audio(message_id: str, configuration) -> bytes:
    """LINE 메시징 API로 오디오 원본 바이트를 다운로드합니다."""
    from linebot.v3.messaging import ApiClient, MessagingApiBlob

    with ApiClient(configuration) as api_client:
        blob_api = MessagingApiBlob(api_client)
        return blob_api.get_message_content(message_id)


def mark_voice_analysis_state(supabase, user_id, state, *, result=None, error=None):
    """user_validations 테이블의 음성 분석 상태를 갱신합니다."""
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


def submit_voice_analysis_job(supabase, configuration, *, message_id, user_id, nickname=""):
    """Cloud Run에 분석을 비동기 접수하고, Vercel 과금 방어를 위해 0.1초 만에 응답 대기를 끊습니다."""
    mark_voice_analysis_state(supabase, user_id, "처리중")

    try:
        audio_bytes = download_line_audio(message_id, configuration)
    except Exception as e:
        mark_voice_analysis_state(supabase, user_id, "에러", error=f"LINE 오디오 다운로드 실패: {e}")
        return

    target_url = get_cloud_run_url(user_id)
    if not target_url:
        mark_voice_analysis_state(supabase, user_id, "에러", error="VOICE_SERVICE_URLS 미설정")
        return

    headers = {"X-API-Key": VOICE_SERVICE_API_KEY} if VOICE_SERVICE_API_KEY else {}
    try:
        requests.post(
            f"{target_url.rstrip('/')}/analyze-async",
            files={"audio": ("audio", audio_bytes)},
            data={"user_id": user_id, "nickname": nickname, "message_id": message_id},
            headers=headers,
            timeout=(3.0, 0.1),  # 연결 3초 대기, 전송 후 Read(응답 대기)는 0.1초 컷
        )
    except requests.exceptions.ReadTimeout:
        # Vercel 연결을 끊어서 발생하는 정상 동작이므로 예외 처리 후 정상 종료
        pass
    except Exception as e:
        mark_voice_analysis_state(supabase, user_id, "에러", error=f"클라우드런 접수 실패: {e}")
        return


def analyze_audio_via_cloud_run(raw_audio_bytes: bytes, user_id: str = "") -> dict:
    """동기식 음성 분석 요청 (필요 시 사용)"""
    target_url = get_cloud_run_url(user_id) or (VOICE_SERVICE_URLS[0] if VOICE_SERVICE_URLS else "")
    if not target_url:
        raise RuntimeError("VOICE_SERVICE_URLS 환경변수가 설정되지 않았습니다.")

    resp = requests.post(
        f"{target_url.rstrip('/')}/analyze",
        files={"audio": ("audio", raw_audio_bytes)},
        headers={"X-API-Key": VOICE_SERVICE_API_KEY} if VOICE_SERVICE_API_KEY else {},
        timeout=VOICE_SERVICE_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()

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

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
import time

import requests

VOICE_SERVICE_URL = os.environ.get("VOICE_SERVICE_URL", "")
VOICE_SERVICE_API_KEY = os.environ.get("VOICE_SERVICE_API_KEY", "")
# Cloud Run이 무료 티어 안에서 스케일-투-제로로 동작하면 콜드스타트(첫 요청 시
# 모델 로딩)에 시간이 걸릴 수 있어서 넉넉하게 잡습니다.
VOICE_SERVICE_TIMEOUT = 90


def analyze_audio_via_cloud_run(raw_audio_bytes: bytes) -> dict:
    """Cloud Run의 /analyze 엔드포인트에 원본 오디오를 보내고 결과를 받아옵니다.

    반환: {"embedding": [...], "estimated_gender": "남"|"여"|None, "pitch_hz": float|None}
    실패 시 예외를 던집니다 (호출부 analyze_new_member_voice가 잡아서 처리).
    """
    if not VOICE_SERVICE_URL:
        raise RuntimeError("VOICE_SERVICE_URL 환경변수가 설정되지 않았습니다.")

    resp = requests.post(
        f"{VOICE_SERVICE_URL.rstrip('/')}/analyze",
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
    member_id=None,
    estimated_gender=None,
    pitch_hz=None,
    source="new_member",
):
    """voice_profiles 테이블에 한 건 저장합니다."""
    row = {
        "user_id": user_id,
        "member_id": member_id,
        "nickname": nickname,
        "storage_path": storage_path,
        "embedding": embedding,
        "estimated_gender": estimated_gender,
        "pitch_hz": pitch_hz,
        "source": source,
    }
    return supabase.table("voice_profiles").insert(row).execute()


def match_blacklist_voices(supabase, embedding, match_count: int = 5):
    """블랙리스트(members)로 등록된 음성들 중 이 임베딩과 가장 유사한 것들을 찾습니다.
    Postgres 쪽 match_voice_profiles() 함수(코사인 유사도, pgvector)를 RPC로 호출합니다.
    반환: [{"id":..., "member_id":..., "nickname":..., "similarity": 0.8~1.0, "gender":..., "status":...}, ...] (유사도 내림차순)
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


def analyze_new_member_voice(supabase, audio_bytes, user_id, nickname):
    # 1. Cloud Run 서버로 음성 데이터를 보내서 분석 결과(임베딩, 성별 등)를 받아옵니다. (기존 코드 유지)
    # response = requests.post(...)
    # embedding = response.get("embedding")
    # gender = response.get("gender")

    if not embedding:
        return {"error": "임베딩 추출 실패"}

    # =================================================================
    # ✨ [추가/변경 위치] 스토리지에 올리기 전에 유사도를 먼저 검사합니다.
    # =================================================================
    # 기존에는 맨 마지막에 하던 대조 작업을 위로 끌어올립니다.
    matches = match_blacklist_voices(supabase, embedding, match_count=5)
    
    # 중복(유사도 98% 이상) 여부 판별
    is_duplicate = False
    if matches:
        highest_similarity = matches[0].get("similarity", 0)
        if highest_similarity >= 0.98: # 98% 이상 일치하면 동일/중복 음성으로 간주
            is_duplicate = True
            print(f"⚠️ 중복 음성 감지 (일치율: {highest_similarity*100:.1f}%). 스토리지 저장을 생략합니다.")

    # =================================================================
    # ✨ 중복이 아닐 때만(is_duplicate == False) 스토리지 업로드와 DB 인서트를 진행합니다.
    # =================================================================
    storage_path = None
    if not is_duplicate:
        try:
            # 2. Supabase Storage에 음성 파일 업로드 (기존 업로드 코드)
            # storage_path = upload_to_storage(supabase, audio_bytes, ...)
            
            # 3. voice_profiles 테이블에 데이터 인서트 (기존 DB 저장 코드)
            # insert_voice_profile(supabase, user_id, nickname, storage_path, embedding, gender)
            pass
        except Exception as e:
            print(f"저장 중 오류: {e}")
    else:
        # 중복일 경우, DB에 저장하지 않았음을 알기 위해 더미 경로나 안내 문구를 넣을 수 있습니다.
        storage_path = "duplicate_skipped"

    # 4. 최종 결과 반환 (기존 코드 유지)
    # 반환할 때 스토리지 경로나 매칭 결과를 함께 돌려줍니다.
    return {
        "status": "success",
        "matches": matches,         # index.py로 넘어가서 관리자 알림에 쓰일 데이터
        "is_duplicate": is_duplicate,
        "gender_estimated": gender
    }result

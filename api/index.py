import os
import random
import google.generativeai as genai
import sys
import re
import json
import time
import hashlib
import urllib.request
import requests
import urllib.error
import gspread
import threading  # 백그라운드 처리를 위한 스레드 모듈 추가
from flask import Flask, request, abort, jsonify
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta

# Supabase client
from supabase import create_client, Client

# LINE SDK imports
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import (
    MessageEvent,
    TextMessageContent,
    StickerMessageContent,
    ImageMessageContent,
    VideoMessageContent,
    AudioMessageContent,
    LocationMessageContent,
    MemberJoinedEvent,  # ✨ [추가됨] 방(그룹) 입장 이벤트
    MemberLeftEvent  # ✨ [추가됨] 방(그룹) 퇴장 이벤트
)

app = Flask(__name__)

# 공용 세션 및 모델 설정 (상단으로 이동)
session = requests.Session()
session.headers.update({"Content-Type": "application/json"})

MODELS = [
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash",
    "gemini-3.5-flash-lite",
    "gemini-3.1-flash-lite",
    "gemini-3.1-pro-preview",
    "gemini-3-flash-preview",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.5-pro",
]

# ================================================================
# 환경변수: Supabase
# ================================================================
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SECRET_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_URL 또는 SUPABASE_SECRET_KEY 환경변수가 없습니다.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ================================================================
# ✨ [추가됨] Supabase 1,000건 제한 우회(Pagination) 헬퍼 함수 ✨
# ================================================================
def fetch_all_supabase_query(base_query):
    """
    Supabase는 한 번의 쿼리로 최대 1,000건의 데이터만 반환합니다.
    이 함수는 .range()를 사용하여 모든 데이터를 반복해서 가져와 하나로 합쳐 반환합니다.
    """
    all_data = []
    limit = 1000
    offset = 0
    while True:
        try:
            res = base_query.range(offset, offset + limit - 1).execute()
            data = res.data or []
            all_data.extend(data)
            if len(data) < limit:
                break
            offset += limit
        except Exception as e:
            print(f"⚠️ Supabase 데이터 조회 오류 (Pagination): {e}")
            break
    return all_data

# ================================================================
# Gemini AI
# ================================================================
API_KEYS = os.environ.get("GEMINI_API_KEYS", "").split(",")
if not API_KEYS or API_KEYS == [""]:
    API_KEYS = [os.environ.get("GEMINI_API_KEY", "")]

API_KEY_INDEX = 0
MODEL_INDEX = 0


def is_gemini_auth_error(status, response):
    """API 키/권한/결제 문제인지 판단 (모델을 바꿔도 해결되지 않는 오류)"""
    if status in (401, 402, 403):
        return True
    if status == 400 and response is not None:
        try:
            return "API key" in response.text
        except Exception:
            return False
    return False

# 캐시 TTL (초) — profile cache 만료 시간
PROFILE_CACHE_TTL = 604800  # 7일

# ================================================================
# 상수
# ================================================================
COOLDOWN_SECONDS = 1.0
MAX_HISTORY_LEN = 20  # 최근 10턴(사용자+모델) 유지 — 퀴즈처럼 여러 턴 이어지는 대화 지원

# DB(chat_logs)에 대화 로그를 보관하는 최대 기간 (주 단위)
# 이 기간보다 오래된 chat_logs 행은 자정 백업 루프에서 자동 삭제됩니다.
CHAT_LOG_DB_RETENTION_WEEKS = 6

# 공지 다중 키워드 추출 시 폭주 방지용 안전상한 (개수를 강제하는 목표치가 아님)
NOTICE_TOPIC_SAFETY_CAP = 6

# 공지 답변이 애매해서 사용자에게 되물었을 때, 그 "보류 상태"를 얼마나 유지할지 (초)
# 이 시간이 지나면 사용자의 다음 메시지는 답변으로 이어붙이지 않고 새 질문으로 취급한다.
PENDING_NOTICE_TTL_SECONDS = 900  # 15분

# ================================================================
# Google Sheets 인증
# ================================================================
json_key_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
if not json_key_str:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON 환경변수가 없습니다.")

json_key_dict = json.loads(json_key_str)

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key_dict, scope)
client = gspread.authorize(creds)

# ================================================================
# LINE 환경변수
# ================================================================
LINE_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET")

# ================================================================
# ✨ [추가됨] 특정 유저가 올린 사진/동영상만 저장 대상 아이디 목록
#
# "아이디": "폴더별칭(영문)" 형태로 넣으면 그 사람이 올린 사진/동영상만 저장된다.
# 비어있거나 보낸 사람이 이 목록에 없으면 아무것도 저장하지 않는다.
# 별칭 자리를 ""로 비워두면 폴더명은 그냥 아이디로 저장됨.
# (Supabase Storage가 폴더/파일명에 한글·공백·이모지를 허용하지 않아서(Invalid key 오류)
#  별칭은 영문/숫자/-, _ 만 사용 가능 — 한글은 여전히 안 됨)
# 수시로 이 딕셔너리만 고쳐서 배포하면 됨.
# (실제 저장 로직은 save_target_user_media 함수 참고)
# ================================================================
MEDIA_SAVE_TARGET_USER_IDS = {
    # "U0177ea0a8668d6885eeb12713d3adc76": "chul-i",  # 예시: 이런 식으로 넣으면 그 사람 것만 저장됨
     "U16fe24fee6f2bf99681f97b6e1ec1e6f": "nyang-2"
}
# ================================================================
# 채팅 로그 저장
# ================================================================
# ✨ [변경됨] 대화 로그를 기록할 그룹방 ID 목록 (여러 개 지정 가능)
CHAT_LOG_TARGET_GROUP_IDS = {
    "C5522f4a633d573bb2e2236074502a7f1",  #채팅로그기록방
}
# ================================================================
# Google Sheets 객체 전역 변수
# ================================================================
line_user_doc = None
user_id_sheet = None
admin_sheet = None

# 목록/멘션에는 표시되지 않고, 코드상으로만 관리자 명령어 권한을 주는 유저 ID
HIDDEN_ADMIN_IDS = [
    "U6362873054f9e1c52c48e7d725ad56ec",
]

eden_doc = None
sheet = None
notice_sheet = None
bung_sheet = None
image_sheet = None

# ================================================================
# Gemini AI Function Calling 도구(Tools) 정의
# ================================================================
# ================================================================
# Gemini AI Function Calling 도구
# ================================================================

GEMINI_TOOLS = [{
    "function_declarations": [

        # ========================================================
        # 내 벙 기록 조회
        # ========================================================
        {
            "name": "get_my_meets",
            "description": """
현재 질문한 사용자의 벙 참여/작성 기록을 DB에서 조회합니다.

이 함수는 반드시 현재 사용자의 개인 기록을 물어보는 질문에 사용합니다.

예:
- 나 벙 몇 번 했어?
- 내 벙 몇 번이야?
- 내가 벙 몇 개 했어?
- 내가 참여한 벙 몇 개야?
- 내가 지금까지 몇 번 벙했어?
- 내 벙 참여내역 보여줘
- 내가 갔던 벙 알려줘
- 내가 참여했던 모임 알려줘
- 내가 만든 벙 몇 개야?
- 내 벙 기록 보여줘

중요:
이 함수에는 닉네임을 전달하지 않습니다.
실제 조회 대상은 서버가 현재 LINE 사용자의 닉네임으로 강제로 결정합니다.

AI가 다른 사람의 닉네임을 추측하거나 지정해서는 안 됩니다.
""",
            "parameters": {
                "type": "OBJECT",
                "properties": {}
            }
        },
                     
        # ========================================================
        # 내 마디 수 조회
        # ========================================================
        {
            "name": "get_my_chat_count",
            "description": """
현재 질문한 사용자의 특정 날짜, 특정 주(이번주/저번주), 또는 최근 N주/N일 동안의
유효 채팅 마디 수를 DB에서 조회합니다.

사용자가 날짜를 말하지 않으면 오늘을 조회합니다.

날짜 표현 예:
- 오늘
- 어제
- 그제
- 모레
- 8월 22일
- 8/22
- 2026년 8월 22일
- 2026-08-22
- 이번주
- 저번주
- 2주동안 (최근 2주 / 지난 2주 등도 동일하게 처리)
- 3일동안 (최근 3일 / 지난 3일 등도 동일하게 처리)

예:
- 오늘 나 몇 마디 했어?
- 어제 나 몇 마디 했어?
- 어제 마디수 알려줘
- 어제 내가 몇 번 말했어?
- 그제 마디 몇 개 했어?
- 8월 22일 내 마디수 알려줘
- 8/22 마디수
- 내 마딧수
- 이번주 마디수는 얼마나 했어?
- 저번주 마디수 알려줘
- 2주동안 마디수 알려줘
- 최근 2주 마디수 얼마나 돼?
- 지난 3일 동안 마디수 알려줘

중요:
기본적으로 이 함수에는 닉네임을 전달하지 않습니다(본인 조회).
실제 조회 대상은 서버가 현재 LINE 사용자의 닉네임으로 강제로 결정합니다.

단, 사용자가 질문 문장에서 본인이 아닌 다른 사람의 닉네임을
명시적으로 언급하며 그 사람의 마디수를 물어본 경우
(예: "안나의 마디수", "철수 마디수 알려줘", "이번주 안나 마디수는?")에는
언급된 그 닉네임을 target_nickname 인자로 그대로 전달합니다.

✨ [추가됨] "/마디수 <닉네임>"처럼 슬래시 명령어 뒤에 닉네임만 덧붙여 오는
경우도 있습니다(예: "마디수 아아", "마디수 철수", "마디 안나").
이때는 "아아"/"철수"/"안나"처럼 명령어 바로 뒤에 붙은 단어를
날짜 표현("오늘","어제","이번주","3일동안" 등)이 아닌 이상
target_nickname으로 그대로 전달합니다.
예: "마디수 아아" → target_nickname="아아" (date는 "오늘")
예: "마디 철수" → target_nickname="철수" (date는 "오늘")
예: "마디수 아아 이번주" → target_nickname="아아", date="이번주"

target_nickname으로 다른 사람의 마디수를 조회할 권한이 있는지는
서버(Python)가 검증합니다(등록된 '마니또'인 경우에만 허용).
AI는 그 사람이 실제로 조회 권한이 있는지 판단하지 않고,
질문에서 언급된 닉네임만 정확히 추출해서 전달하면 됩니다.

"나", "내", "내가" 등 본인을 지칭하는 표현만 있다면
target_nickname을 전달하지 않습니다.

date에는 사용자가 요청한 날짜 표현을 그대로 전달합니다.
"N주"/"N일" 형태의 기간 표현은 "지난"/"최근" 등의 수식어는 유지한 채
"N주동안"/"N일동안"처럼 주/일 단위와 숫자, 그리고 기간을 나타내는
접미사(동안/간/일)를 포함해서 전달합니다.
날짜를 특정하지 않은 경우 date는 "오늘"로 전달합니다.
"""
            ,
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "date": {
                        "type": "STRING",
                        "description": """
조회할 날짜 또는 기간입니다.

예:
오늘 → "오늘"
어제 → "어제"
그제 → "그제"
8월 22일 → "8월 22일"
8/22 → "8/22"
2026년 8월 22일 → "2026년 8월 22일"
2주동안 → "2주동안"
지난 2주 → "지난2주" 또는 "2주동안"
3일 동안 → "3일동안"

날짜가 명시되지 않았다면 "오늘"을 사용합니다.
"""
                    },
                    "target_nickname": {
                        "type": "STRING",
                        "description": """
질문에서 본인이 아닌 다른 사람의 닉네임이 명시적으로 언급된 경우에만
그 닉네임을 전달합니다.

예:
"안나의 마디수" → target_nickname="안나"
"철수 마디수 알려줘" → target_nickname="철수"
"이번주 안나 마디수는?" → target_nickname="안나"

✨ [추가됨] "마디수 <닉네임>"처럼 명령어 뒤에 닉네임이 오는 경우도 있습니다.
예:
"마디수 아아" → target_nickname="아아"
"마디 철수" → target_nickname="철수"
"마디수 아아 이번주" → target_nickname="아아"

본인을 지칭하는 질문("나", "내 마디수" 등)이면 이 인자를
전달하지 않습니다(빈 문자열도 전달하지 않음).
"""
                    }
                },
                "required": ["date"]
            }
        },
                # ========================================================
        # 공지사항 조회 (질문 의도 파악 및 키워드 추출)
        # ========================================================
        {
            "name": "get_notice_info",
            "description": """
사용자가 커뮤니티의 규칙, 가이드, 공지 등을 물어볼 때 사용합니다.

"규칙", "공지"라는 단어가 문장에 없어도,
짧은 명사 하나만 묻는 질문(예: "원픽은?", "선갠라 뭐야?",
"동반 가능해?", "강퇴 기준")도 커뮤니티 용어일 가능성이
있으면 반드시 먼저 이 함수를 호출해서 DB부터 확인하세요.
애매하다고 판단해서 사용자에게 먼저 되묻지 마세요.

질문의 의도를 파악하여, 검색에 필요한 '단일 핵심 명사(예: 인증, 규칙, 정모, 원픽)'만
조사를 제거하고 추출하세요.
특정 키워드가 없는 단순 공지 확인 요청이라면 keyword를 빈 문자열('')로 전달하세요.

이 함수는 공지의 제목(키워드)뿐 아니라 공지 본문 내용까지 함께 검색합니다.
따라서 제목에 직접적으로 등장하지 않는 단어라도, 본문에 관련 내용이 있으면
검색 결과로 함께 찾아줄 수 있습니다.
""",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "keyword": {
                        "type": "STRING",
                        "description": "질문의 의도를 담은 단일 명사 키워드"
                    }
                },
                "required": ["keyword"]
            }
        }


    ]
}]

# ================================================================
# 마디수 조회용 날짜 파싱
# ================================================================
def parse_chat_count_date(raw_date):
    """
    마디수 조회에서 사용하는 날짜 파서.

    지원:
    - 오늘
    - 어제
    - 그제
    - 그저께
    - 모레
    - 8월 22일
    - 8/22
    - 2026년 8월 22일
    - 2026-08-22
    """

    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)

    if not raw_date:
        return now.strftime("%Y-%m-%d")

    text = str(raw_date).strip().replace(" ", "")

    # ------------------------------------------------------------
    # 상대 날짜
    # ------------------------------------------------------------
    if text in ["오늘", "금일"]:
        target_date = now

    elif text in ["어제", "전날"]:
        target_date = now - timedelta(days=1)

    elif text in ["그제", "그저께"]:
        target_date = now - timedelta(days=2)

    elif text in ["모레"]:
        target_date = now + timedelta(days=2)

    # ------------------------------------------------------------
    # YYYY-MM-DD
    # ------------------------------------------------------------
    elif re.match(r"^\d{4}-\d{1,2}-\d{1,2}$", text):
        try:
            y, m, d = map(int, text.split("-"))
            target_date = datetime(y, m, d)
        except ValueError:
            return None

    # ------------------------------------------------------------
    # YYYY년 MM월 DD일
    # ------------------------------------------------------------
    else:
        m1 = re.match(
            r"^(\d{4})년(\d{1,2})월(\d{1,2})일?$",
            text
        )

        if m1:
            try:
                y = int(m1.group(1))
                m = int(m1.group(2))
                d = int(m1.group(3))

                target_date = datetime(y, m, d)

            except ValueError:
                return None

        else:
            # ----------------------------------------------------
            # MM월 DD일
            # ----------------------------------------------------
            m2 = re.match(
                r"^(\d{1,2})월(\d{1,2})일?$",
                text
            )

            if m2:
                try:
                    m = int(m2.group(1))
                    d = int(m2.group(2))

                    target_date = datetime(
                        now.year,
                        m,
                        d
                    )

                except ValueError:
                    return None

            else:
                # ------------------------------------------------
                # MM/DD 또는 MM-DD
                # ------------------------------------------------
                m3 = re.match(
                    r"^(\d{1,2})[/-](\d{1,2})$",
                    text
                )

                if m3:
                    try:
                        m = int(m3.group(1))
                        d = int(m3.group(2))

                        target_date = datetime(
                            now.year,
                            m,
                            d
                        )

                    except ValueError:
                        return None

                else:
                    return None

    return target_date.strftime("%Y-%m-%d")

# ================================================================
# 마디수 조회용 "이번주/저번주" 범위 계산
# ================================================================
def parse_week_range(raw_date):
    """
    "이번주" → (이번주 월요일 ~ 오늘)
    "저번주" → (저번주 월요일 ~ 저번주 일요일)
    "N주" / "N주동안" / "N주간" / "N주일" / "지난 N주" / "최근 N주"
        → (오늘로부터 N*7일 전 ~ 오늘)
    "N일동안" / "N일간" / "지난 N일" / "최근 N일"
        → (오늘로부터 N일 전 ~ 오늘)
    범위가 아니면 None을 반환한다.
    """
    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    text = re.sub(r"\s+", "", str(raw_date or ""))

    if text in ("이번주", "금주", "이번주간"):
        monday = now - timedelta(days=now.weekday())
        return (
            monday.strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
            "이번주"
        )

    if text in ("저번주", "지난주", "전주"):
        this_monday = now - timedelta(days=now.weekday())
        last_monday = this_monday - timedelta(days=7)
        last_sunday = this_monday - timedelta(days=1)
        return (
            last_monday.strftime("%Y-%m-%d"),
            last_sunday.strftime("%Y-%m-%d"),
            "저번주"
        )

    if text in ("이번달", "금월", "이번달간"):
        start = now.replace(day=1)
        return (
            start.strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
            "이번달"
        )

    if text in ("저번달", "지난달", "전달"):
        first_of_this_month = now.replace(day=1)
        last_day_prev_month = first_of_this_month - timedelta(days=1)
        first_of_prev_month = last_day_prev_month.replace(day=1)
        return (
            first_of_prev_month.strftime("%Y-%m-%d"),
            last_day_prev_month.strftime("%Y-%m-%d"),
            "저번달"
        )

    # ------------------------------------------------------------
    # "N주" / "N주동안" / "N주간" / "N주일" / "지난 N주" / "최근 N주"
    # → 오늘을 포함한 최근 N*7일
    # ------------------------------------------------------------
    m_week = re.match(r"^(?:지난|최근)?(\d+)주(?:동안|간|일)?$", text)
    if m_week:
        n = int(m_week.group(1))
        if n > 0:
            start = now - timedelta(days=n * 7 - 1)
            return (
                start.strftime("%Y-%m-%d"),
                now.strftime("%Y-%m-%d"),
                f"최근 {n}주"
            )

    # ------------------------------------------------------------
    # "N일동안" / "N일간" / "지난 N일" / "최근 N일"
    # (접미사 없는 "N일"은 특정 날짜(예: "22일")와 겹치므로 반드시 동안/간이 붙어야 인식)
    # → 오늘을 포함한 최근 N일
    # ------------------------------------------------------------
    m_day = re.match(r"^(?:지난|최근)?(\d+)일(?:동안|간)$", text)
    if m_day:
        n = int(m_day.group(1))
        if n > 0:
            start = now - timedelta(days=n - 1)
            return (
                start.strftime("%Y-%m-%d"),
                now.strftime("%Y-%m-%d"),
                f"최근 {n}일"
            )

    return None

# ================================================================
# 벙지리스트 조회용 "기간" 파싱
# ================================================================
def _parse_flexible_range_text(text):
    """
    '08.29~08.30', '08.29~30', '2026-08-29~2026-09-02' 처럼
    물결(~)로 표시된 날짜 범위를 (start_YYYY-MM-DD, end_YYYY-MM-DD)로 변환한다.
    ('/벙지 작성'에서 쓰는 범위 표기법과 동일하게 맞췄다)
    범위 표현이 아니면 None.
    """
    t = re.sub(r"\s+", "", str(text or ""))
    if "~" not in t:
        return None

    kst = timezone(timedelta(hours=9))
    this_year = datetime.now(kst).year

    left, _, right = t.partition("~")
    if not left or not right:
        return None

    m_start_full = re.match(r'^(\d{4})[-./년](\d{1,2})[-./월](\d{1,2})일?$', left)
    m_start_short = re.match(r'^(\d{1,2})[-./월](\d{1,2})일?$', left)

    if m_start_full:
        y, mo, d = int(m_start_full.group(1)), int(m_start_full.group(2)), int(m_start_full.group(3))
    elif m_start_short:
        y, mo, d = this_year, int(m_start_short.group(1)), int(m_start_short.group(2))
    else:
        return None

    try:
        start_dt = datetime(y, mo, d)
    except ValueError:
        return None

    m_end_full = re.match(r'^(\d{4})[-./년](\d{1,2})[-./월](\d{1,2})일?$', right)
    m_end_day_only = re.match(r'^(\d{1,2})일?$', right)
    m_end_month_day = re.match(r'^(\d{1,2})[-./월](\d{1,2})일?$', right)

    if m_end_full:
        end_y, end_mo, end_d = int(m_end_full.group(1)), int(m_end_full.group(2)), int(m_end_full.group(3))
    elif m_end_day_only:
        end_y, end_mo, end_d = y, mo, int(m_end_day_only.group(1))
    elif m_end_month_day:
        end_y, end_mo, end_d = y, int(m_end_month_day.group(1)), int(m_end_month_day.group(2))
    else:
        return None

    try:
        end_dt = datetime(end_y, end_mo, end_d)
    except ValueError:
        return None

    if end_dt < start_dt:
        end_dt = end_dt.replace(year=end_dt.year + 1)

    return (start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))


def parse_meet_list_period(text):
    """
    '/벙 리스트 이번달', '/벙 리스트 08.01~08.15', '/벙 리스트 8/23' 처럼
    벙지리스트 조회에 곁들여진 기간 표현을 (start, end, label)로 변환한다.
    (마디수 조회에 쓰는 파서들을 그대로 재사용 — 이번주/저번주/이번달/저번달/
     N주/N일 + 특정 날짜 + '~' 날짜범위 모두 지원)

    리스트를 요청할 때 같이 쓰는 단어("기록","내역","목록","상세","리스트")는
    먼저 지우고 남은 부분을 기간 표현으로 판단한다.
    인식되는 기간 표현이 없으면 None.
    """
    cleaned = str(text or "")
    for w in ["기록", "내역", "목록", "상세", "리스트"]:
        cleaned = cleaned.replace(w, " ")
    cleaned = cleaned.strip()
    if not cleaned:
        return None

    week_range = parse_week_range(cleaned)
    if week_range:
        return week_range

    tilde_range = _parse_flexible_range_text(cleaned)
    if tilde_range:
        return (tilde_range[0], tilde_range[1], cleaned)

    single = parse_chat_count_date(cleaned)
    if single:
        return (single, single, cleaned)

    return None


def extract_date_phrase(text):
    """
    "어제 마디수 알려줘", "8/22 마디 몇개?", "이번주 마디수는 얼마나 했어?" 같은
    자유 문장에서 parse_chat_count_date / parse_week_range가 이해할 수 있는
    날짜(또는 주간) 표현만 뽑아낸다.
    못 찾으면 "오늘"을 반환한다.
    """
    text = str(text or "")

    # 주간(이번주/저번주) 표현을 가장 먼저 확인
    for word in ["이번주", "이번 주", "금주"]:
        if word in text:
            return "이번주"

    for word in ["저번주", "지난주", "지난 주", "저번 주"]:
        if word in text:
            return "저번주"

    patterns = [
        r"\d{4}년\s*\d{1,2}월\s*\d{1,2}일?",
        r"\d{4}-\d{1,2}-\d{1,2}",
        r"\d{1,2}월\s*\d{1,2}일?",
        r"\d{1,2}[/-]\d{1,2}",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(0).replace(" ", "")

    for word in ["그저께", "그제", "어제", "전날", "모레", "오늘", "금일"]:
        if word in text:
            return word

    return "오늘"

# 환경변수로 미리 설정된 시크릿 토큰
EXPORT_SECRET = os.environ.get("EXPORT_SECRET")


# ================================================================
# Gemini Function Calling 실제 실행
# ================================================================

def execute_gemini_tool(function_name, args, current_user_nickname, current_user_id=None):
    """
    Gemini가 어떤 DB 조회를 할지 결정하면
    실제 닉네임/아이디는 Gemini가 아닌 Python에서 강제로 지정한다.

    즉,
        Gemini → "get_my_meets가 필요하다"
        Python  → 현재 LINE 사용자의 닉네임/아이디로 DB 조회

    구조로 동작한다.

    ⚠️ 닉네임은 사용자가 중간에 바꿀 수 있기 때문에, 조회(WHERE 조건)는
    가능하면 변하지 않는 user_id(LINE 고유 아이디) 기준으로 하고,
    화면에 보여주는 텍스트에는 항상 "현재" 닉네임(current_user_nickname)을
    사용한다. current_user_id가 없는 예전 호출부와의 호환을 위해,
    id가 없을 때만 닉네임 기준 조회로 폴백한다.
    """

    # ✨ [추가됨] Gemini가 이름을 살짝 다르게 불러오는 경우(예: "get_chat_count")를
    # 실제 등록된 함수명으로 매핑해준다. 여기 없는 이름은 그대로 통과되어
    # 기존처럼 "알 수 없는 함수입니다" 처리된다.
    FUNCTION_NAME_ALIASES = {
        "get_chat_count": "get_my_chat_count",
        "get_chat_counts": "get_my_chat_count",
        "get_meets": "get_my_meets",
        "get_my_meet": "get_my_meets",
        "get_notice": "get_notice_info",
        "get_notices": "get_notice_info",
    }
    function_name = FUNCTION_NAME_ALIASES.get(function_name, function_name)

    kst = timezone(timedelta(hours=9))
    today_str = datetime.now(kst).strftime("%Y-%m-%d")

    # ------------------------------------------------------------
    # 현재 사용자 닉네임 검증
    # ------------------------------------------------------------
    target_nickname = str(current_user_nickname or "").strip()
    target_user_id = str(current_user_id or "").strip()

    if not target_nickname:
        return {
            "success": False,
            "error": "현재 사용자의 닉네임을 확인할 수 없습니다.",
            "message": "현재 사용자의 닉네임을 확인할 수 없어 DB 조회를 진행할 수 없습니다."
        }

    try:

        # ========================================================
        # 1. 내 벙 기록 조회
        # ========================================================
        if function_name == "get_my_meets":

            print(
                f"🔎 DB 벙 조회 시작 | "
                f"현재 사용자={target_nickname}"
            )

            # ----------------------------------------------------
            # 모든 meet_log 가져오기
            # pagination은 기존 함수 사용
            # ----------------------------------------------------
            q = (
                supabase
                .table("meet_log")
                .select(
                    "날짜, 장소, 벙제, 멤버, 작성자, 진행상황"
                )
            )

            all_meets = fetch_all_supabase_query(q)

            # ----------------------------------------------------
            # 닉네임 비교용 정규화
            #
            # 예:
            # ⛑️04 망두📌 -> 망두
            # 망두 -> 망두
            # 망두123 -> 망두
            #
            # 기존 DB 구조가
            # "토리 망두"
            # "모모 망두 포뇨"
            # 처럼 공백으로 멤버를 구분하기 때문에
            # 각각 분리해서 비교한다.
            # ----------------------------------------------------
            def normalize_nickname(name):
                return "".join(
                    re.findall(r"[가-힣]", str(name or ""))
                )

            target_clean = normalize_nickname(target_nickname)

            my_meets = []

            for meet in all_meets:

                members = str(
                    meet.get("멤버") or ""
                ).strip()

                # 멤버는 공백으로 구분되어 있음 (쉼표 ',' / 마침표 '.'도 구분자로 허용)
                member_list = [m for m in re.split(r"[\s,.]+", members) if m]

                member_clean_list = [
                    normalize_nickname(member)
                    for member in member_list
                ]

                # ------------------------------------------------
                # 현재 사용자가 멤버에 포함되어 있는 경우
                # ------------------------------------------------
                if (
                    target_clean
                    and target_clean in member_clean_list
                ):

                    my_meets.append({
                        "날짜": meet.get("날짜"),
                        "장소": meet.get("장소"),
                        "벙제": meet.get("벙제"),
                        "멤버": members,
                        "작성자": meet.get("작성자"),
                        "진행상황": meet.get("진행상황")
                    })

            total_count = len(my_meets)

            print(
                f"📊 DB 벙 조회 완료 | "
                f"{target_nickname} = {total_count}회"
            )

            return {
                "success": True,

                # 서버에서 확정한 현재 사용자
                "nickname": target_nickname,

                "total_count": total_count,

                "meets": my_meets,

                "message": (
                    f"{target_nickname}님의 "
                    f"DB상 벙 참여/작성 기록은 "
                    f"총 {total_count}회입니다."
                )
            }


                # ========================================================
        # 2. 내 마디 수 조회
        # ========================================================
        elif function_name == "get_my_chat_count":

            # ----------------------------------------------------
            # ✨ [추가됨] 마니또 마디수 조회
            #
            # Gemini가 target_nickname(질문에 언급된 다른 사람 닉네임)을
            # 넘긴 경우에만 확인한다. 그 사람이 "라인유저아이디" 시트 D열에
            # 등록된 '나(현재 사용자)의 마니또'일 때만 조회를 허용하고,
            # 아니면 조회 자체를 거부한다. (권한 판단은 전부 서버에서 수행)
            # ----------------------------------------------------
            manito_prefix_note = None

            target_nickname_arg = (
                args.get("target_nickname") if isinstance(args, dict) else None
            )
            requested_hangul = extract_hangul(target_nickname_arg) if target_nickname_arg else ""
            my_hangul = extract_hangul(target_nickname)

            if requested_hangul and requested_hangul != my_hangul:

                my_manito_raw = ""
                try:
                    if target_user_id:
                        my_row_res = (
                            supabase.table("line_users")
                            .select("manito")
                            .eq("line_user_id", target_user_id)
                            .execute()
                        )
                    else:
                        my_row_res = (
                            supabase.table("line_users")
                            .select("manito")
                            .eq("nickname", target_nickname)
                            .execute()
                        )
                    if my_row_res.data:
                        my_manito_raw = str(my_row_res.data[0].get("manito") or "")
                except Exception as e:
                    print(f"⚠️ 마니또 목록 조회 실패: {e}")

                my_manito_hanguls = [
                    extract_hangul(n)
                    for n in re.split(r"[,\s]+", my_manito_raw)
                    if n.strip()
                ]

                if requested_hangul not in my_manito_hanguls:
                    manito_denied_messages = [
                        f"어허, '{target_nickname_arg}'님은 너의 마니또가 아니야! 🕵️ 남의 사생활은 소중히~",
                        f"띠용, '{target_nickname_arg}'님 마디수가 왜 궁금해?! 마니또 등록 안 된 사람은 조회 불가야 🙅",
                        f"'{target_nickname_arg}'님... 너랑 무슨 사이야? 마니또도 아니면서 마디수는 왜 궁금해해 ㅋㅋ 안 알려줌!",
                        f"삐빅- 접근 권한 없음 🚫 '{target_nickname_arg}'님은 네 마니또 명단에 없는데?",
                        f"오호라, 몰래 스토킹? 😏 '{target_nickname_arg}'님 마디수는 등록된 마니또만 볼 수 있어!",
                        f"그건 좀 곤란한데... '{target_nickname_arg}'님이 널 마니또로 등록해준 적이 없나봐 🥲",
                        f"뿅! 여긴 아무나 못 들어와~ '{target_nickname_arg}'님 정보는 마니또 인증된 사람만 OK 🔒",
                    ]
                    return {
                        "success": False,
                        "error": "마니또 권한 없음",
                        "nickname": target_nickname,
                        "message": random.choice(manito_denied_messages)
                    }

                manito_user_id, _ambiguous = find_user_id_by_nickname_partial(target_nickname_arg)

                if not manito_user_id:
                    return {
                        "success": False,
                        "error": "대상 사용자를 찾을 수 없음",
                        "nickname": target_nickname,
                        "message": f"'{target_nickname_arg}'님을 찾을 수 없어요. 닉네임을 다시 확인해줘!"
                    }

                manito_real_nickname = target_nickname_arg
                try:
                    manito_row_res = (
                        supabase.table("line_users")
                        .select("nickname")
                        .eq("line_user_id", manito_user_id)
                        .execute()
                    )
                    if manito_row_res.data:
                        manito_real_nickname = manito_row_res.data[0].get("nickname") or target_nickname_arg
                except Exception as e:
                    print(f"⚠️ 마니또 닉네임 조회 실패: {e}")

                manito_prefix_note = (
                    f"{manito_real_nickname}님의 마니또 {target_nickname}님, "
                    f"마니또의 마디수가 궁금하시군요?"
                )

                # 이후 로직은 마니또(조회 대상)를 기준으로 진행되도록 교체
                target_nickname = manito_real_nickname
                target_user_id = manito_user_id

            # ----------------------------------------------------
            # Gemini가 전달한 날짜
            # ----------------------------------------------------
            requested_date = (
                args.get("date")
                if isinstance(args, dict)
                else None
            )

            if not requested_date:
                requested_date = "오늘"

            # ----------------------------------------------------
            # "이번주" / "저번주" 범위 여부 먼저 확인
            # ----------------------------------------------------
            week_range = parse_week_range(requested_date)

            if week_range:
                range_start, range_end, week_label = week_range
                target_date_str = None
            else:
                target_date_str = parse_chat_count_date(
                    requested_date
                )

                # ----------------------------------------------------
                # 날짜 파싱 실패
                # ----------------------------------------------------
                if not target_date_str:
                    return {
                        "success": False,
                        "nickname": target_nickname,
                        "requested_date": requested_date,
                        "error": "날짜를 이해하지 못했습니다.",
                        "message": (
                            f"'{requested_date}' 날짜를 이해하지 못했어. "
                            "오늘, 어제, 그제, 이번주, 2주동안 또는 8월 22일 같은 형식으로 말해줘."
                        )
                    }

            print(
                f"🔎 마디수 조회 | "
                f"사용자={target_nickname} | "
                f"요청날짜={requested_date} | "
                f"조회범위="
                f"{(range_start + '~' + range_end) if week_range else target_date_str}"
            )

            # ----------------------------------------------------
            # 마디수 집계 제외 대상인지 확인
            # ----------------------------------------------------
            try:

                # 닉네임은 바뀔 수 있으므로, user_id가 있으면 반드시
                # line_user_id(고유 아이디) 기준으로 조회한다.
                # (user_id가 없는 예전 호출부만 닉네임으로 폴백)
                if target_user_id:
                    user_info_res = (
                        supabase
                        .table("line_users")
                        .select("is_excluded")
                        .eq("line_user_id", target_user_id)
                        .execute()
                    )
                else:
                    user_info_res = (
                        supabase
                        .table("line_users")
                        .select("is_excluded")
                        .eq("nickname", target_nickname)
                        .execute()
                    )

                is_excluded_val = False

                if user_info_res.data:

                    val = user_info_res.data[0].get(
                        "is_excluded"
                    )

                    if (
                        str(val) == "1"
                        or str(val).lower() == "true"
                        or val is True
                    ):
                        is_excluded_val = True

            except Exception as e:

                print(
                    f"⚠️ 제외 유저 확인 실패: {e}"
                )

                is_excluded_val = False

            # ----------------------------------------------------
            # 제외 대상
            # ----------------------------------------------------
            if is_excluded_val:

                excluded_message = (
                    f"{target_nickname}님은 "
                    f"마디수 집계 제외 대상으로 "
                    f"설정되어 있습니다."
                )
                if manito_prefix_note:
                    excluded_message = f"{manito_prefix_note}\n{excluded_message}"

                return {
                    "success": True,
                    "nickname": target_nickname,
                    "date": target_date_str,
                    "requested_date": requested_date,
                    "excluded": True,
                    "valid_chat_count": 0,
                    "message": excluded_message
                }

            # ----------------------------------------------------
            # 채팅 로그 조회 (단일 날짜 또는 이번주/저번주 범위)
            #
            # ⚠️ 닉네임은 도중에 바뀔 수 있어서 닉네임으로만 조회하면
            # 닉네임을 바꾸기 전에 쌓인 기록이 누락된다.
            # user_id는 바뀌지 않으므로, 있으면 반드시 user_id 기준으로
            # 조회한다. (user_id가 없는 예전 호출부만 닉네임으로 폴백)
            # ----------------------------------------------------
            if target_user_id:
                q = (
                    supabase
                    .table("chat_logs")
                    .select("id, message, line_message_id, log_time")
                    .eq("user_id", target_user_id)
                )
            else:
                q = (
                    supabase
                    .table("chat_logs")
                    .select("id, message, line_message_id, log_time")
                    .eq("nickname", target_nickname)
                )

            if week_range:
                q = q.gte("log_date", range_start).lte("log_date", range_end)
            else:
                q = q.eq("log_date", target_date_str)

            # id 기준 정렬을 명시해서, 채팅이 계속 쌓이는 도중에
            # range() 페이지네이션이 여러 번 도는 경우에도
            # 조회 결과가 흔들리지 않도록 한다.
            q = q.order("id")

            raw_target_messages = fetch_all_supabase_query(q)

            # ----------------------------------------------------
            # 안전장치: 중복 저장된 행 제거
            #
            # LINE 웹훅 재전송/서버 재시도 등으로 같은 메시지가
            # DB에 두 번 이상 쌓였더라도, 마디수 계산에서는 한 번만
            # 세도록 여기서 한 번 더 걸러낸다.
            # (line_message_id가 있으면 그걸로, 없는 예전 행은
            #  같은 메시지 내용 + 같은 로그시각 조합으로 판단)
            # ----------------------------------------------------
            all_target_messages = []
            seen_message_ids = set()
            seen_legacy_keys = set()

            for chat in raw_target_messages:
                mid = chat.get("line_message_id")

                if mid:
                    if mid in seen_message_ids:
                        continue
                    seen_message_ids.add(mid)
                else:
                    legacy_key = (
                        chat.get("message"),
                        chat.get("log_time")
                    )
                    if legacy_key in seen_legacy_keys:
                        continue
                    seen_legacy_keys.add(legacy_key)

                all_target_messages.append(chat)

            valid_chat_count = 0

            min_length = 2

            forbidden_words = [
                "이모티콘",
                "사진",
                "ㅋㅋㅋ",
                "ㅎㅎㅎ",
                "동영상",
                "????"
            ]

            # ----------------------------------------------------
            # 유효 마디 계산
            # ----------------------------------------------------
            for chat in all_target_messages:

                msg = str(
                    chat.get("message") or ""
                ).strip()

                if not msg:
                    continue

                # 공백 제거 후 2글자 미만 제외
                if len(
                    msg.replace(" ", "")
                ) < min_length:
                    continue

                # 제외 단어
                if any(
                    word in msg
                    for word in forbidden_words
                ):
                    continue

                valid_chat_count += 1

            print(
                f"📊 마디수 조회 완료 | "
                f"{target_nickname} | "
                f"{(week_label if week_range else target_date_str)} = "
                f"{valid_chat_count}개"
            )

            # ----------------------------------------------------
            # 날짜/범위를 자연스러운 한국어 라벨로 변환
            # ----------------------------------------------------
            if week_range:
                period_label = week_label
            else:
                kst_today = datetime.now(
                    timezone(timedelta(hours=9))
                ).strftime("%Y-%m-%d")
                kst_yesterday = (
                    datetime.now(timezone(timedelta(hours=9)))
                    - timedelta(days=1)
                ).strftime("%Y-%m-%d")
                kst_dbd = (
                    datetime.now(timezone(timedelta(hours=9)))
                    - timedelta(days=2)
                ).strftime("%Y-%m-%d")

                if target_date_str == kst_today:
                    period_label = "오늘"
                elif target_date_str == kst_yesterday:
                    period_label = "어제"
                elif target_date_str == kst_dbd:
                    period_label = "그제"
                else:
                    period_label = target_date_str

            # ----------------------------------------------------
            # 답변 메시지 생성 (30마디 미만이면 숫자 숨기고 놀리듯 답변)
            # ----------------------------------------------------
            if valid_chat_count < 30:
                teasing_messages = [
                    f"{target_nickname}님, 음... {period_label} 마디수는... 자세히 말하기가 좀 민망한데 🙈 그냥 30마디도 안 된다고만 해둘게 ㅋㅋㅋ",
                    f"{target_nickname}님, 어우 이건 좀... {period_label} 정확한 숫자는 차마 못 말하겠고, 30마디도 안 된다는 것만 알려줄게 😂 좀 더 채팅해봐~",
                    f"{target_nickname}님, {period_label} 숫자는 비밀로 할래... 너무 조용했던 거 아니야? 😏 딱 봐도 30마디는 확실히 안 넘겨",
                    f"{target_nickname}님, 에고... {period_label}는 숫자 밝히기 부끄러운 수준이야 🙊 30마디 미만이라는 것만 살짝 귀띔해줄게",
                ]
                reply_message = random.choice(teasing_messages)
            else:
                excl_note = "짧은 단답이나 특정 단어들은 마디 계산에서 제외되니까, 체감하는 횟수와는 조금 차이가 있을 수 있어~"

                if valid_chat_count >= 500:
                    normal_messages = [
                        f"{target_nickname}님, {period_label} {valid_chat_count}마디 🔥 이 정도면 키보드에서 연기 나는 거 아니야?? {excl_note}",
                        f"{target_nickname}님, 헉... {period_label} 무려 {valid_chat_count}마디! 혹시 여기가 본캐야? ㅋㅋㅋ {excl_note}",
                        f"{target_nickname}님, {period_label} {valid_chat_count}마디라니, 채팅방 지분 거의 다 가져갔네 👑 {excl_note}",
                    ]
                elif valid_chat_count >= 150:
                    normal_messages = [
                        f"{target_nickname}님, {period_label} {valid_chat_count}마디 했어~ 오늘따라 할 말이 많았나봐?! {excl_note}",
                        f"{target_nickname}님, {period_label}는 {valid_chat_count}마디! 완전 수다쟁이 모드였구나 ㅋㅋ {excl_note}",
                        f"{target_nickname}님, {period_label} 마디수는 {valid_chat_count}개! 분량 확실히 채웠네 💪 {excl_note}",
                    ]
                else:
                    normal_messages = [
                        f"{target_nickname}님, {period_label} {valid_chat_count}마디 했어~ {excl_note}",
                        f"{target_nickname}님, {period_label} 마디수 확인해봤더니 {valid_chat_count}개네! {excl_note}",
                        f"{target_nickname}님, {period_label}엔 {valid_chat_count}마디 남겼더라구~ {excl_note}",
                        f"{target_nickname}님, 짜잔🎉 {period_label} {valid_chat_count}마디! {excl_note}",
                    ]

                reply_message = random.choice(normal_messages)

            if manito_prefix_note:
                reply_message = f"{manito_prefix_note}\n{reply_message}"

            result = {
                "success": True,
                "nickname": target_nickname,
                "requested_date": requested_date,
                "excluded": False,

                # 기존 필드와 호환
                "today_valid_chat_count": valid_chat_count,

                # 새 날짜 조회용 명확한 필드
                "valid_chat_count": valid_chat_count,

                "message": reply_message
            }

            if week_range:
                result["is_range"] = True
                result["range_start"] = range_start
                result["range_end"] = range_end
            else:
                result["date"] = target_date_str

            return result

        # ========================================================
        # 공지사항 DB 조회 로직
        # ========================================================
        elif function_name == "get_notice_info":
            keyword = args.get("keyword", "").strip()
            
            try:
                if keyword:
                    notices = query_notice_rows(keyword)

                    if notices:
                        return {
                            "success": True,
                            "results": notices,
                            "message": f"'{keyword}' 관련 공지가 제목(키워드)과 본문 내용에서 검색되었습니다. 사용자의 질문 의도에 가장 잘 맞는 내용만 골라서 부드럽게 답변해주세요."
                        }
                    else:
                        return {
                            "success": False,
                            "keyword": keyword,
                            "message": "관련 공지를 찾을 수 없습니다."
                        }
                else:
                    # 키워드가 없을 때 전체 목록 반환
                    q = supabase.table("공지").select("키워드").order("키워드", desc=False)
                    res_data = fetch_all_supabase_query(q)
                    keywords = [r.get("키워드", "").strip() for r in res_data if r.get("키워드")]
                    
                    return {
                        "success": True,
                        "keywords": keywords,
                        "message": "전체 공지 키워드 목록입니다. 부드럽게 안내해주세요."
                    }

            except Exception as e:
                print(f"⚠️ 공지 조회 중 오류: {e}")
                return {"success": False, "error": str(e)}


        # ========================================================
        # 알 수 없는 함수
        # ========================================================
        return {
            "success": False,
            "error": f"알 수 없는 함수입니다: {function_name}"
        }


    except Exception as e:

        print(
            f"🚨 Gemini Tool 실행 오류 "
            f"[{function_name}]: {e}"
        )

        return {
            "success": False,
            "error": str(e),
            "function": function_name,
            "nickname": target_nickname
        }

# ================================================================
# Gemini AI 스마트 봇 답변
# ================================================================

def get_smart_reply(user_nickname, user_message, user_id=None):

    global API_KEY_INDEX, MODEL_INDEX

    kst = timezone(timedelta(hours=9))
    today_str = datetime.now(kst).strftime("%Y-%m-%d")

    # ============================================================
    # 현재 사용자의 닉네임은 Python이 확정
    # (조회는 아래 execute_gemini_tool 호출부에서 user_id로 하고,
    #  화면 표시용으로는 이 "현재" 닉네임을 그대로 사용한다)
    # ============================================================

    current_user_nickname = str(
        user_nickname or ""
    ).strip()

    current_user_id = str(
        user_id or ""
    ).strip()

    if not current_user_nickname:

        return (
            "현재 사용자의 닉네임을 확인하지 못했어 🥲"
        )


    # ============================================================
    # Gemini 시스템 지시문
    # ============================================================

    system_instruction = f"""

너는 FLIRTY 커뮤니티의 친절하고 센스 있는 안내 봇이야.

오늘 날짜:
{today_str}

현재 질문한 사용자의 닉네임:
{current_user_nickname}


================================================
[가장 중요한 DB 조회 규칙]
================================================

사용자가 자신의 DB 기록을 물어보면
절대로 AI가 숫자를 추측하거나 직접 계산하지 않는다.

반드시 Function Calling을 사용해서
실제 DB 조회 결과를 받은 후 답변한다.


================================================
[개인정보 조회 규칙]
================================================

사용자가

나
내
내가
내 벙
내 마디
내 기록
내가 참여한 것

등 자신의 기록을 질문하면

반드시 현재 질문한 사용자의 기록만 조회한다.

현재 사용자의 실제 닉네임은:

{current_user_nickname}

이다.

Gemini가 임의로 닉네임을 추측해서 조회 대상으로 결정해서는 안 된다.

기본적으로 Function Calling에서는 닉네임을 전달하지 않으며,
서버 Python 코드가 현재 사용자의 닉네임을 강제로 적용한다.

단, get_my_chat_count에 한해서, 사용자가 질문 문장에 본인이 아닌
다른 사람의 닉네임을 명시적으로 언급한 경우
(예: "안나의 마디수", "철수 마디수 알려줘")에는
그 닉네임을 target_nickname 인자로 그대로 전달할 수 있다.
이때도 실제 조회 권한(마니또 여부)은 서버가 검증하므로,
Gemini는 언급된 닉네임을 정확히 옮겨 전달하기만 하면 된다.


================================================
[벙 관련 질문]
================================================

다음과 같은 질문은
반드시 get_my_meets를 호출한다.

- 나 벙 몇 번 했어?
- 내 벙 몇 번이야?
- 내가 벙 몇 개 했어?
- 내가 참여한 벙 몇 개야?
- 내가 지금까지 몇 번 벙했어?
- 내 벙 횟수
- 내 벙 기록
- 내 벙 참여내역
- 내가 참여했던 벙 알려줘
- 내가 갔던 벙 알려줘
- 내가 만든 벙 몇 개야?
- 내가 참여하거나 작성한 벙이 몇 개야?

위와 비슷한 의미의 자연어 질문도
get_my_meets를 사용한다.


    ================================================
    [마디 수 질문]
    ================================================

    사용자가 자신의 마디 수를 질문하면
    반드시 get_my_chat_count를 호출한다.

    날짜가 질문에 포함되어 있으면
    반드시 그 날짜를 함수의 date 인자로 전달한다.

    날짜가 없으면 "오늘"을 사용한다.

    다음과 같은 질문은
    반드시 get_my_chat_count를 호출한다.

    - 오늘 나 몇 마디 했어?
    - 오늘 몇 마디 했어?
    - 내 마딧수
    - 오늘 마딧수
    - 오늘 채팅 몇 번 했어?
    - 오늘 내가 몇 번 말했어?
    - 오늘 내 마디수 알려줘
    - 오늘 내가 몇 마디 쳤어?

    어제 관련 질문도 반드시
    get_my_chat_count를 호출한다.

    - 어제 나 몇 마디 했어?
    - 어제 몇 마디 했어?
    - 어제 마디수 알려줘
    - 어제 내 마디수
    - 어제 내가 몇 번 말했어?
    - 어제 채팅 몇 번 했어?

    그제 관련 질문도 지원한다.

    - 그제 마디수
    - 그저께 마디수
    - 그제 내가 몇 마디 했어?

    특정 날짜도 지원한다.

    - 8월 22일 마디수
    - 8/22 마디수
    - 2026년 8월 22일 마디수
    - 2026-08-22 마디수

    "이번주"/"저번주" 뿐 아니라
    "N주"/"N일" 단위의 기간 질문도 지원한다.

    - 2주동안 마디수 알려줘 → date="2주동안"
    - 지난 2주 마디수 얼마나 돼? → date="지난2주" (또는 "2주동안")
    - 최근 3일 마디수 알려줘 → date="3일동안" (또는 "최근3일")
    - 5일간 마디수 → date="5일간"

    날짜를 함수에 전달할 때는
    사용자가 말한 날짜 표현을 그대로 date에 넣는다.

    예:

    "어제 마디수" → date="어제"

    "그제 마디수" → date="그제"

    "8월 22일 마디수" → date="8월 22일"

    "오늘 마디수" → date="오늘"

    "2주동안 마디수" → date="2주동안"


    ================================================
    [DB 숫자 규칙]
    ================================================

    DB 조회 결과의 숫자만 사용한다.

    AI가 임의로 숫자를 만들거나 계산하지 않는다.

    특히 벙 횟수는
    get_my_meets의 total_count를 사용한다.

    마디 수는
    get_my_chat_count의
    valid_chat_count를 사용한다.

    반드시 사용자가 요청한 날짜의
    date와 valid_chat_count를 기준으로 답변한다.

    예:

    사용자가 "어제 마디수"라고 질문했고
    Tool 결과가

    date = "2026-08-23"
    valid_chat_count = 37

    이라면

    "어제 37마디 했어~"

    처럼 답한다.

    절대로 오늘 숫자를
    어제 숫자로 바꾸어 말하지 않는다.

================================================
[벙 조회 결과 답변]
================================================

total_count가 0이면

"{current_user_nickname}님... 아직 벙 기록이 하나도 없네 🥲
이러다 유령회원 되는 거 아니야? 얼른 하나 나가보자~"

처럼 살짝 놀리듯 자연스럽게 답한다.

total_count가 있으면

"{current_user_nickname}님은
지금까지 총 N번 벙했어~"

처럼 간단하게 답하되, 숫자가 아주 많거나 적으면
"완전 인싸네 ㅋㅋ" "슬슬 좀 더 나가볼까?" 처럼
가볍게 반응을 곁들여도 좋다.

사용자가 상세 내역을 요구한 경우에만
날짜 / 장소 / 벙제 등을 정리해서 보여준다.


    ================================================
    [마디 조회 결과]
    ================================================

    get_my_chat_count 결과가
    마디수 집계 제외 대상이면

    "해당 아이디는 마디수 집계가 제외되도록
    설정되어 있어서 체크할 수 없어~"

    라고 안내한다.

    ------------------------------------------------
    ⚠️ 30마디 미만인 경우 (매우 중요, 우선 적용)
    ------------------------------------------------

    valid_chat_count가 30 미만이면
    절대로 정확한 숫자를 말하지 않는다.

    대신 너무 적어서 차마 정확히 말하기 민망하다는 듯
    장난스럽게 얼버무리며 놀리는 톤으로 답한다.

    숫자는 절대 노출하지 않고,
    "30마디도 안 돼" 라는 뉘앙스만 전달한다.

    예시 (이런 느낌으로 자유롭게 변형):

    "음... 그게... 자세히 말하기가 좀 민망한데 🙈
    그냥 30마디도 안 된다고만 해둘게 ㅋㅋㅋ"

    "어우 이건 좀... 정확한 숫자는 차마 못 말하겠고
    30마디도 안 된다는 것만 알려줄게 😂 좀 더 채팅해봐~"

    "이 숫자는 비밀로 할래... 너무 조용했던 거 아니야? 😏
    딱 봐도 30마디는 확실히 안 넘겨"

    이런 식으로 매번 표현을 살짝씩 바꿔가며
    귀엽게 놀리듯 답한다. 단, 실제 숫자(정확한 값)는
    어떤 경우에도 문장에 노출하지 않는다.

    ------------------------------------------------
    30마디 이상인 경우
    ------------------------------------------------

    정상적으로 조회되면
    DB의 valid_chat_count를 그대로 사용한다.

    Tool 결과의 date를 기준으로
    사용자가 요청한 날짜를 자연스럽게 표현한다.

    예:

    date가 오늘이면
    "오늘 N마디 했어~"

    date가 어제이면
    "어제 N마디 했어~"

    date가 그제이면
    "그제 N마디 했어~"

    특정 날짜이면
    "2026년 8월 22일에는 N마디 했어~"

    실제 숫자는 반드시
    Tool의 valid_chat_count와
    동일해야 한다.

    그리고 다음 안내를 자연스럽게 덧붙인다.

    "짧은 단답이나 특정 단어들은
    마디 계산에서 제외되기 때문에,
    실제 본인이 체감하는 횟수와는
    조금 차이가 있을 수 있어~"

================================================
[일반 대화]
================================================

인사, 잡담처럼 누가 봐도 명백히
커뮤니티 규칙/공지와 무관한 질문만
Function Calling 없이 자연스럽게 답한다.

================================================
[공지사항 답변 규칙 - 매우 중요]
================================================

사용자가 "규칙", "공지", "안내"라는 단어를
직접 말하지 않아도,
커뮤니티 용어로 보이는 짧은 단어나 문장으로
질문하면 반드시 먼저 `get_notice_info`를
호출해서 DB부터 확인한다.

절대로 먼저 "무슨 뜻이야?", "더 자세히 알려줄래?"
같은 되묻는 질문을 하지 않는다.

일단 DB를 조회해보고, 결과가 없을 때만
사용자에게 다시 물어본다.

예 (전부 반드시 get_notice_info를 먼저 호출):
- 원픽은?
- 원픽이 뭐야?
- 동반 가능해?
- 선갠라 뭐야?
- 동반 원픽 규칙 알려줘
- 초대인증 어떻게해?
- 심야시간 언제야?
- 인증 방법
- 멘션 규칙
- 강퇴 기준이 뭐야?
- 입장규칙 알려줘
- ㄱㅈ (한 단어라도 커뮤니티/모임 관련 용어로
  보이면 일단 시도한다)

keyword를 추출할 때는
질문 문장에서 조사(은,는,이,가,을,를,뭐야,알려줘 등)를
제거한 핵심 명사만 전달한다.

예:
"원픽은?" → keyword="원픽"
"선갠라 뭐야?" → keyword="선갠라"
"동반 가능해?" → keyword="동반"

get_notice_info 결과 success가 false이거나
관련 공지를 찾지 못했다면,
그때 비로소
"음... 그 부분에 대한 내용은 아직 안내된 게 없는 것 같아 🥲
혹시 다른 표현으로 다시 물어봐줄래?"
처럼 자연스럽게 답한다.

이때도 먼저 되묻지 말고,
반드시 함수 호출 → 결과 확인 후에만
이렇게 답한다.

1. 다정하지만 장난기 있는 말투: 딱딱한 기계처럼 답하지 말고, 친하고 짓궂은 친구처럼 답해라. 기본은 다정한 반말(~해요, ~해줄게 등)이지만, 가끔 살짝 놀리거나 능청스럽게 드립을 치는 것도 좋다. 다만 사용자를 무안하게 만들 정도로 심하게 놀리지는 말고, 사용자가 웃으면서 받아칠 수 있을 정도의 선을 지켜라. 이모지도 상황에 맞게 적절히 사용해라.
2. 질문 의도에 맞는 핵심만 쏙: DB에서 여러 개의 공지가 검색되더라도 전부 읽어주지 마라. 사용자가 '진짜로 궁금해하는 부분'만 골라내어 1~3줄로 짧고 깔끔하게 요약해라.
3. TMI 금지: 묻지 않은 내용은 굳이 덧붙이지 마라.
4. 데이터 기반: 검색 결과에 없는 내용은 지어내지 말고, "그 부분은 아직 안내된 내용이 없네 🥲"라고 솔직하게 말해라.
5. 목록 안내: 전체 공지 목록을 안내할 때는 "이런 내용들이 준비되어 있어! 궁금한 걸 말해주면 알려줄게~"처럼 자연스럽게 키워드들을 나열해라.
6. ⚠️ 인정/불인정 왜곡 금지 (매우 중요): 공지 원문이 "인정되지 않는다", "불인정", "안 된다"처럼 명확히 부정하고 있다면, 절대로 "조건부로 가능해", "제한적으로는 돼" 처럼 완화하거나 중간 단계가 있는 것처럼 바꿔 말하지 마라. 특히 여러 조건이 얽힌 규칙(예: 기본 조건만으로는 인정 안 되고, 추가 조건까지 채워야 비로소 인정되는 경우)은 결론만 뭉뚱그리지 말고 "지금 상태로는 어떤지 + 추가로 뭘 더 해야 하는지"를 함께 짧게라도 알려줘라.
7. ⚠️ 경로 누락 금지 (강퇴/미클처럼 중요한 주제): 미클, 강퇴처럼 결과가 갈리는 주제를 답할 때, 원문에 그 결론에 도달하는 방법이 여러 개(예: 방법A / 방법B) 있다면 사용자가 그중 하나만 물어봤어도 나머지 방법까지 놓치지 말고 함께 알려줘라. 이런 경우엔 "1~3줄로 짧게" 원칙보다 빠짐없이 알려주는 게 우선이다.


================================================
[절대 하지 말아야 할 것]
================================================

1. DB 숫자를 추측하지 않는다.

2. DB 숫자를 임의로 계산하지 않는다.

3. DB에 없는 데이터를 만들어내지 않는다.

4. 현재 사용자 이외의 사람의 개인정보를
   조회하지 않는다.

5. 사용자가 다른 사람의 닉네임을 말하더라도
   자신의 개인 기록을 묻는 상황에서는
   현재 사용자 데이터만 조회한다.

6. Tool 결과를 받은 뒤
   Tool 결과와 다른 숫자를 말하지 않는다.
"""


    # ============================================================
    # Gemini 최초 요청
    # ============================================================

    payload = {

        "systemInstruction": {
            "parts": [
                {
                    "text": system_instruction
                }
            ]
        },

        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": user_message
                    }
                ]
            }
        ],

        "tools": GEMINI_TOOLS,

        "generationConfig": {
            "temperature": 0.2
        }
    }


    # ============================================================
    # API KEY / MODEL 순환
    # ============================================================

    max_attempts = max(
        1,
        len(API_KEYS) * len(MODELS) * 2
    )


    for _ in range(max_attempts):

        current_key = API_KEYS[API_KEY_INDEX]
        current_model = MODELS[MODEL_INDEX]

        url = (
            "https://generativelanguage.googleapis.com/"
            f"v1beta/models/{current_model}:generateContent"
            f"?key={current_key}"
        )


        try:

            # ====================================================
            # 1차 Gemini 요청
            # ====================================================

            res = session.post(
                url,
                json=payload,
                timeout=(3, 15)
            )

            res.raise_for_status()

            res_data = res.json()

            candidates = res_data.get(
                "candidates",
                []
            )

            if not candidates:

                return (
                    "응답을 생성하지 못했어 🥲"
                )


            parts = (
                candidates[0]
                .get("content", {})
                .get("parts", [])
            )

            if not parts:

                return (
                    "응답을 생성하지 못했어 🥲"
                )


            # ====================================================
            # Function Call 찾기
            # ====================================================

            function_part = None

            for part in parts:

                if "functionCall" in part:

                    function_part = part
                    break


            # ====================================================
            # 일반 텍스트 답변
            # ====================================================

            if function_part is None:

                for part in parts:

                    if "text" in part and not part.get("thought"):

                        answer = (
                            part["text"]
                            .strip()
                        )

                        if answer:

                            return answer


                return (
                    "응답을 이해하지 못했어 🥲"
                )


            # ====================================================
            # Function Call
            # ====================================================

            func_call = function_part[
                "functionCall"
            ]

            func_name = func_call.get(
                "name"
            )

            # ✨ [추가됨] Gemini가 실제 등록된 이름과 살짝 다르게 부르는 경우
            # (예: "get_chat_count" → "get_my_chat_count") 여기서 먼저 정규화해서,
            # 아래 "2차 Gemini 호출 없이 바로 답하기" 분기(get_my_chat_count 등)와
            # execute_gemini_tool 내부 처리가 서로 어긋나지 않게 한다.
            FUNCTION_NAME_ALIASES = {
                "get_chat_count": "get_my_chat_count",
                "get_chat_counts": "get_my_chat_count",
                "get_meets": "get_my_meets",
                "get_my_meet": "get_my_meets",
                "get_notice": "get_notice_info",
                "get_notices": "get_notice_info",
            }
            func_name = FUNCTION_NAME_ALIASES.get(func_name, func_name)

            # ----------------------------------------------------
            # 중요:
            # Gemini가 args에 무엇을 넣더라도
            # 닉네임은 사용하지 않는다.
            # ----------------------------------------------------

            func_args = (
                func_call.get("args")
                or {}
            )


            print(
                "🤖 Gemini Function Call:",
                func_name
            )

            print(
                "👤 실제 조회 사용자:",
                current_user_nickname
            )


            # ====================================================
            # Python에서 현재 사용자 강제 적용
            # ====================================================

            tool_result = execute_gemini_tool(
                func_name,
                func_args,
                current_user_nickname,
                current_user_id
            )


            print(
                "📊 Tool Result:",
                tool_result
            )

            # ====================================================
            # DB 숫자를 직접 답하는 함수는 2차 Gemini 호출 없이
            # 우리가 이미 만들어둔 정확한 메시지를 그대로 사용한다.
            # (숫자 왜곡/누락 위험 방지 — 특히 30마디 미만 놀리기 로직)
            # ====================================================
            if func_name in ("get_my_chat_count", "get_my_meets"):
                return tool_result.get(
                    "message",
                    "조회 결과를 불러오지 못했어 🥲"
                )

            # ====================================================
            # 2차 Gemini 요청
            # ====================================================

            second_payload = {

                "systemInstruction": {
                    "parts": [
                        {
                            "text": system_instruction
                        }
                    ]
                },

                "contents": [

                    {
                        "role": "user",
                        "parts": [
                            {
                                "text": user_message
                            }
                        ]
                    },

                    {
                        "role": "model",
                        "parts": parts
                    },

                    {
                        "role": "user",
                        "parts": [
                            {
                                "functionResponse": {
                                    "name": func_name,
                                    "response": tool_result
                                }
                            }
                        ]
                    }
                ],

                "tools": GEMINI_TOOLS,

                "generationConfig": {
                    "temperature": 0.2
                }
            }


            res2 = session.post(
                url,
                json=second_payload,
                timeout=(3, 15)
            )

            res2.raise_for_status()

            res2_data = res2.json()

            candidates2 = (
                res2_data.get(
                    "candidates",
                    []
                )
            )


            if not candidates2:

                return (
                    "DB 조회 결과를 받아왔지만 "
                    "답변을 만들지 못했어 🥲"
                )


            parts2 = (
                candidates2[0]
                .get("content", {})
                .get("parts", [])
            )


            if not parts2:

                return (
                    "DB 조회 결과를 받아왔지만 "
                    "답변을 만들지 못했어 🥲"
                )


            # ====================================================
            # 최종 답변
            # ====================================================

            for part in parts2:

                if "text" in part and not part.get("thought"):

                    answer = (
                        part["text"]
                        .strip()
                    )

                    if answer:

                        return answer


            return (
                "DB 조회 결과를 받아왔지만 "
                "답변을 만들지 못했어 🥲"
            )


        # ========================================================
        # HTTP 오류
        # ========================================================

        except requests.exceptions.HTTPError as e:

            status = (
                e.response.status_code
                if e.response is not None
                else 500
            )

            print(
                f"🚨 Gemini HTTP 오류 "
                f"{status}: {e}"
            )


            if status in [503, 404]:

                if status == 503:
                    time.sleep(1)  # 일시적 과부하니 살짝 쉬었다가 재시도

                MODEL_INDEX = (
                    MODEL_INDEX + 1
                ) % len(MODELS)


            elif status == 429:

                API_KEY_INDEX = (
                    API_KEY_INDEX + 1
                ) % len(API_KEYS)

                time.sleep(1)


            elif is_gemini_auth_error(status, e.response):

                # 키/권한/결제 문제는 모델이 아니라 키를 바꿔야 함
                API_KEY_INDEX = (
                    API_KEY_INDEX + 1
                ) % len(API_KEYS)

                time.sleep(1)


            else:

                MODEL_INDEX = (
                    MODEL_INDEX + 1
                ) % len(MODELS)


            continue


        # ========================================================
        # 기타 오류
        # ========================================================

        except Exception as e:

            print(
                f"🚨 Gemini API 오류: {e}"
            )

            MODEL_INDEX = (
                MODEL_INDEX + 1
            ) % len(MODELS)

            continue


    return (
        "앗, 지금 데이터를 조회해서 "
        "답변하는 데 잠깐 문제가 생겼어! 🥲"
    )

# ================================================================
# /공지, /ㄱㅈ 전용: 자연어 질문 → 검색 키워드만 빠르게 추출 (1회 LLM 호출)
# 답변 생성 단계(2차 호출) 없이 바로 DB 원문을 보여주기 때문에
# /봇 대비 응답 속도가 크게 단축된다.
# ================================================================
def gemini_extract_notice_keyword(user_message):
    global API_KEY_INDEX, MODEL_INDEX

    payload = {
        "systemInstruction": {
            "parts": [{
                "text": (
                    "사용자의 질문에서 공지/규칙 검색에 쓸 핵심 명사 키워드 "
                    "하나만 추출해서 반드시 get_notice_info 함수를 호출해. "
                    "조사(은,는,이,가,을,를 등)와 어미(뭐야,알려줘,가능해 등)는 "
                    "제거하고 명사만 남긴다. 키워드를 특정할 수 없으면 "
                    "빈 문자열로 호출해."
                )
            }]
        },
        "contents": [{"role": "user", "parts": [{"text": user_message}]}],
        "tools": GEMINI_TOOLS,
        "toolConfig": {
            "functionCallingConfig": {
                "mode": "ANY",
                "allowedFunctionNames": ["get_notice_info"]
            }
        },
        "generationConfig": {"temperature": 0}
    }

    max_attempts = max(1, len(API_KEYS) * len(MODELS) * 2)

    for _ in range(max_attempts):
        current_key = API_KEYS[API_KEY_INDEX]
        current_model = MODELS[MODEL_INDEX]
        url = (
            "https://generativelanguage.googleapis.com/"
            f"v1beta/models/{current_model}:generateContent"
            f"?key={current_key}"
        )
        try:
            res = session.post(url, json=payload, timeout=(3, 10))
            res.raise_for_status()
            candidates = res.json().get("candidates", [])
            if not candidates:
                return ""
            for part in candidates[0].get("content", {}).get("parts", []):
                if "functionCall" in part:
                    fargs = part["functionCall"].get("args") or {}
                    return str(fargs.get("keyword", "")).strip()
            return ""
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 500
            print(f"🚨 공지 키워드 추출 HTTP 오류 {status}: {e}")
            if status == 429:
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            elif status == 503:
                time.sleep(1)  # 일시적 과부하니 살짝 쉬었다가 재시도
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            elif is_gemini_auth_error(status, e.response):
                # 키/권한/결제 문제는 모델이 아니라 키를 바꿔야 함
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            else:
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue
        except Exception as e:
            print(f"🚨 공지 키워드 추출 오류: {e}")
            MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue

    return ""

# ================================================================
# 검색된 공지 원문을 사용자 질문에 맞춰 쉽게 한 줄로 풀어주는 설명 (1회 LLM 호출)
# 원문은 그대로 보여주고, 이해를 돕는 "설명"만 추가로 덧붙이는 용도.
# ================================================================
def gemini_explain_notice(user_message, rows):
    global API_KEY_INDEX, MODEL_INDEX

    if not rows:
        return ""

    combined_body = "\n\n".join(
        str(r.get("공지", "")).strip() for r in rows[:2]
    )

    payload = {
        "systemInstruction": {
            "parts": [{
                "text": (
                    "너는 커뮤니티 안내봇이야. 아래 공지 원문을 참고해서 "
                    "사용자의 질문에 맞는 핵심만 다정한 말투로 설명해줘. "
                    "보통 2~3줄이면 충분하지만, 강퇴/미클처럼 결과가 갈리는 "
                    "주제이고 원문에 그 결론에 도달하는 방법이 여러 개 있다면, "
                    "질문에서 직접 언급 안 된 방법이라도 놓치지 말고 함께 알려줘 "
                    "(이때는 줄 수가 좀 늘어나도 괜찮아). "
                    "원문에 없는 내용은 절대 지어내지 마. "
                    "⚠️ 특히 원문이 '인정되지 않는다/불인정/안 된다'처럼 명확히 "
                    "부정하고 있다면 '조건부로 가능하다'처럼 완화해서 표현하지 말고 "
                    "그 부정 표현을 그대로 살려서 전달해. "
                    "'~해요, ~해줄게' 처럼 부드럽게 답해."
                )
            }]
        },
        "contents": [{
            "role": "user",
            "parts": [{
                "text": f"[공지 원문]\n{combined_body}\n\n[사용자 질문]\n{user_message}"
            }]
        }],
        "generationConfig": {"temperature": 0.3}
    }

    max_attempts = max(1, len(API_KEYS) * len(MODELS) * 2)

    for _ in range(max_attempts):
        current_key = API_KEYS[API_KEY_INDEX]
        current_model = MODELS[MODEL_INDEX]
        url = (
            "https://generativelanguage.googleapis.com/"
            f"v1beta/models/{current_model}:generateContent"
            f"?key={current_key}"
        )
        try:
            res = session.post(url, json=payload, timeout=(3, 10))
            res.raise_for_status()
            candidates = res.json().get("candidates", [])
            if not candidates:
                return ""
            for part in candidates[0].get("content", {}).get("parts", []):
                if "text" in part and part["text"].strip():
                    return part["text"].strip()
            return ""
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 500
            print(f"🚨 공지 설명 생성 HTTP 오류 {status}: {e}")
            if status == 429:
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            elif status == 503:
                time.sleep(1)  # 일시적 과부하니 살짝 쉬었다가 재시도
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            elif is_gemini_auth_error(status, e.response):
                # 키/권한/결제 문제는 모델이 아니라 키를 바꿔야 함
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            else:
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue
        except Exception as e:
            print(f"🚨 공지 설명 생성 오류: {e}")
            MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue

    return ""

# ================================================================
# /공지, /ㄱㅈ, /벙, /ㅂ, /마디, /ㅁㄷ 자연어 질문 공통 응답 포맷터
# ================================================================
def format_notice_rows_reply(keyword, rows):
    if not rows:
        return f"🥲 '{keyword}' 관련 공지를 찾지 못했어. '/공지 키워드'로 목록을 확인해봐!"

    blocks = []
    for r in rows[:3]:
        title = str(r.get("키워드", "")).strip()
        body = str(r.get("공지", "")).strip()
        blocks.append(f"📌 {title}\n\n{body}")

    return "\n\n─────────\n\n".join(blocks)

# ================================================================
# ✨ [추가됨] 여러 공지 항목을 조합해서 추론하는 답변 파이프라인 ✨
#
# "노미클도 단벙 참석 가능하지?" 처럼 하나의 질문이 서로 다른
# 공지 항목(예: '단벙', '노미클 갠벙권') 여러 개를 함께 참고해야만
# 답할 수 있는 경우를 위한 경로.
#
# 1) gemini_extract_notice_topics   : 질문에서 관련 키워드를 여러 개 추출
# 2) query_notice_rows_multi        : 키워드별로 공지를 찾아 중복 없이 합침
# 3) gemini_answer_notice_multi     : 모아진 공지 원문을 근거로 결론을 추론하고
#                                      필요한 부분만 짧게 발췌
# 4) format_notice_answer_reply     : 위 결과를 카톡/라인 메시지 형태로 포맷
# 5) handle_notice_question         : 위 단계를 순서대로 실행하는 진입점
#                                      (실패 시 None → 호출부가 기존 방식으로 폴백)
# ================================================================

def gemini_extract_notice_topics(user_message):
    """
    사용자 질문에서 공지 검색에 필요한 핵심 키워드를 추출한다.
    개수를 고정하지 않고, 질문에 필요한 만큼(보통 1~3개, 복잡하면 그 이상)
    모델이 스스로 판단하게 한다. 단, 폭주 방지를 위한 안전상한(NOTICE_TOPIC_SAFETY_CAP)만 둔다.
    단일 키워드만 뽑는 gemini_extract_notice_keyword와 달리,
    질문이 여러 공지 항목을 조합해야 답할 수 있는 경우를 위해
    관련 키워드를 리스트로 반환한다.
    """
    global API_KEY_INDEX, MODEL_INDEX

    topic_tool = [{
        "function_declarations": [{
            "name": "get_notice_topics",
            "description": (
                "사용자의 질문에 답하기 위해 공지 DB에서 찾아봐야 할 "
                "핵심 명사 키워드를 추출한다. 개수는 정해져 있지 않으며, "
                "질문에 답하는 데 실제로 필요한 만큼만 추출한다 "
                "(단순 질문이면 1개, 여러 공지 항목을 조합해야 답할 수 있는 "
                "질문이면 필요한 만큼 여러 개). "
                "예: '노미클도 단벙 참석 가능하지?' → ['단벙', '노미클']"
            ),
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "keywords": {
                        "type": "ARRAY",
                        "items": {"type": "STRING"},
                        "description": "검색할 핵심 명사 키워드 목록 (질문에 필요한 만큼만, 조사/어미 제거)"
                    }
                },
                "required": ["keywords"]
            }
        }]
    }]

    payload = {
        "systemInstruction": {
            "parts": [{
                "text": (
                    "사용자의 질문에서 공지/규칙 검색에 필요한 핵심 명사 키워드를 "
                    "추출해서 반드시 get_notice_topics 함수를 호출해. "
                    "키워드 개수를 억지로 맞추지 말고, 질문에 답하는 데 "
                    "실제로 필요한 만큼만 뽑아. 단순한 질문이면 1개만 뽑아도 되고, "
                    "서로 다른 공지 항목을 조합해야 답할 수 있는 질문이면 "
                    "관련 키워드를 필요한 만큼 모두 포함해. "
                    "조사(은,는,이,가,을,를 등)와 어미(뭐야,알려줘,가능해,가능하지 등)는 "
                    "제거하고 명사만 남긴다. 키워드를 특정할 수 없으면 빈 배열로 호출해."
                )
            }]
        },
        "contents": [{"role": "user", "parts": [{"text": user_message}]}],
        "tools": topic_tool,
        "toolConfig": {
            "functionCallingConfig": {
                "mode": "ANY",
                "allowedFunctionNames": ["get_notice_topics"]
            }
        },
        "generationConfig": {"temperature": 0}
    }

    max_attempts = max(1, len(API_KEYS) * len(MODELS) * 2)

    for _ in range(max_attempts):
        current_key = API_KEYS[API_KEY_INDEX]
        current_model = MODELS[MODEL_INDEX]
        url = (
            "https://generativelanguage.googleapis.com/"
            f"v1beta/models/{current_model}:generateContent"
            f"?key={current_key}"
        )
        try:
            res = session.post(url, json=payload, timeout=(3, 10))
            res.raise_for_status()
            candidates = res.json().get("candidates", [])
            if not candidates:
                return []
            for part in candidates[0].get("content", {}).get("parts", []):
                if "functionCall" in part:
                    fargs = part["functionCall"].get("args") or {}
                    raw_keywords = fargs.get("keywords") or []
                    cleaned, seen = [], set()
                    for k in raw_keywords:
                        k = str(k).strip()
                        if k and k not in seen:
                            seen.add(k)
                            cleaned.append(k)
                    # 개수는 모델이 자유롭게 판단하되, 폭주만 안전상한으로 막는다.
                    return cleaned[:NOTICE_TOPIC_SAFETY_CAP]
            return []
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 500
            print(f"🚨 공지 키워드(다중) 추출 HTTP 오류 {status}: {e}")
            if status == 429:
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            elif status == 503:
                time.sleep(1)  # 일시적 과부하니 살짝 쉬었다가 재시도
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            elif is_gemini_auth_error(status, e.response):
                # 키/권한/결제 문제는 모델이 아니라 키를 바꿔야 함
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            else:
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue
        except Exception as e:
            print(f"🚨 공지 키워드(다중) 추출 오류: {e}")
            MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue

    return []


def query_notice_rows_multi(keywords, limit_per_keyword=3, max_total=None):
    """
    여러 키워드로 공지 DB를 검색해서 결과를 제목(키워드) 기준으로
    중복 없이 하나로 합친다.
    max_total을 지정하지 않으면 키워드 개수에 맞춰 자동으로 늘어난다
    (키워드가 많은 질문일수록 더 많은 근거를 모을 수 있게).
    """
    if max_total is None:
        max_total = min(NOTICE_TOPIC_SAFETY_CAP * limit_per_keyword, max(6, len(keywords) * limit_per_keyword))

    def _rank_rows(rows, kw):
        """
        DB가 돌려준 순서(시트 순서)대로 앞 3개만 자르면, 본문에 키워드가 스치기만 한
        공지가 먼저 잡혀 정작 제목이 일치하는 공지가 빠진다. 그래서 관련도순으로 정렬한다.
          0: 제목 키워드가 정확히 일치 (예: '미클, ㅁㅋ')
          1: 제목 키워드의 단어 중 하나가 일치 (예: '남자 미클', '여자 미클')
          2: 제목 어딘가에 글자가 포함 (예: '미클자')
          3: 본문에만 포함
        같은 점수끼리는 원래 순서를 유지한다(stable sort).
        """
        def score(r):
            title = str(r.get("키워드", "")).strip()
            tokens = [t.strip() for t in re.split(r"[,，]", title) if t.strip()]
            if kw in tokens:
                return 0
            if any(kw in t.split() for t in tokens):
                return 1
            if kw in title:
                return 2
            return 3
        return sorted(rows, key=score), score

    merged, seen_titles = [], set()
    for kw in keywords:
        if not kw:
            continue
        ranked, _score = _rank_rows(query_notice_rows(kw), kw)
        # 제목이 일치하는 공지(점수 0~1)는 개수가 많아도 최대 6개까지 모두 포함
        strong = sum(1 for r in ranked if _score(r) <= 1)
        take = max(limit_per_keyword, min(strong, 6))
        for r in ranked[:take]:
            title = str(r.get("키워드", "")).strip()
            if title and title not in seen_titles:
                seen_titles.add(title)
                merged.append(r)
            if len(merged) >= max_total:
                return merged
    return merged



def gemini_answer_notice_multi(user_message, rows):
    """
    여러 공지 항목(rows)을 근거로 질문에 대한 답을 조합/추론하고,
    원문 전체가 아니라 판단 근거가 된 부분만 짧게 발췌해서 함께 돌려준다.
    반환값: {"answer": str, "excerpts": [{"title": str, "excerpt": str}, ...]} 또는 None
    """
    global API_KEY_INDEX, MODEL_INDEX

    if not rows:
        return None

    combined_source = "\n\n".join(
        f"[{str(r.get('키워드', '')).strip()}]\n{str(r.get('공지', '')).strip()}"
        for r in rows
    )

    payload = {
        "systemInstruction": {
            "parts": [{
                "text": (
                    "너는 커뮤니티 안내봇이야. 아래에는 여러 개의 공지 원문이 "
                    "[제목] 형태로 구분되어 주어져. 사용자의 질문에 답하기 위해 "
                    "필요한 항목들의 내용을 서로 조합/추론해서 결론을 내려줘.\n\n"
                    "먼저 스스로 판단해: 주어진 공지 원문만으로 이 질문에 정확한 결론을 "
                    "낼 수 있는지, 아니면 질문자의 구체적인 상황(예: 어느 쪽 조건에 "
                    "해당하는지, 이미 충족한 조건이 있는지, 누구를 기준으로 묻는 건지 등)을 "
                    "몰라서 결론이 '가능'과 '불가능' 사이에서 갈릴 수 있는지.\n\n"
                    "⚠️ 결론이 갈릴 수 있는데 그 정보가 질문에 없다면, 절대 임의로 하나를 "
                    "골라서 추측 답변하지 말고 아래 '되묻기' 형식으로만 응답해 "
                    "(설명이나 코드블록 없이 JSON 객체만):\n"
                    "{\n"
                    '  "needs_clarification": true,\n'
                    '  "clarification_question": "사용자에게 되물을 구체적인 질문. 무엇을 몰라서 '
                    '판단이 안 되는지와, 어떤 정보를 알려주면 되는지를 명확하게 쓰고, '
                    '가능하면 선택지도 함께 제시해 (예: \'동반으로 오셨나요, 아니면 개별로 '
                    '초대하신 건가요?\')"\n'
                    "}\n\n"
                    "반대로 원문만으로 충분히 답할 수 있다면 아래 형식으로만 응답해 "
                    "(설명이나 코드블록 없이 JSON 객체만):\n"
                    "{\n"
                    '  "needs_clarification": false,\n'
                    '  "answer": "질문에 대한 결론을 다정한 말투로 (~해요, ~해줄게 체). 보통 2~4줄이면 충분하지만, 아래 규칙에 해당하면 더 길어져도 된다",\n'
                    '  "excerpts": [\n'
                    '    {"title": "근거가 된 공지 제목", "excerpt": "판단 근거가 된 원문 중 필요한 부분만 발췌 (1~3문장)"}\n'
                    "  ]\n"
                    "}\n\n"
                    "되묻기 판단 기준 (남발하지 말 것):\n"
                    "- 같은 키워드라도 원문에 서로 다른 결과로 이어지는 여러 경로/조건이 있고, "
                    "질문자가 그중 어느 경로에 해당하는지 알 수 없을 때만 되물어.\n"
                    "- 단순히 정보가 조금 더 있으면 좋은 정도라면 되묻지 말고, 원문에 있는 "
                    "모든 경로를 다 안내하는 방식(아래 강퇴/미클 규칙 참고)으로 답해.\n"
                    "- 질문이 명확하고 원문 그대로 답할 수 있으면 절대 되묻지 말고 바로 답해.\n\n"
                    "답변 규칙 (needs_clarification이 false일 때):\n"
                    "- 원문 전체를 넣지 말고, 결론의 근거가 되는 문장만 짧게 발췌해.\n"
                    "- 질문과 직접 관련 없는 항목은 excerpts에서 제외해.\n"
                    "- 원문에 없는 내용은 절대 추측하거나 지어내지 마. "
                    "근거가 부족하면 answer에 그 사실을 솔직히 밝혀.\n"
                    "- ⚠️ 매우 중요: 원문이 '인정되지 않는다/불인정/안 된다'처럼 "
                    "명확히 부정하고 있다면, 그걸 '조건부로 가능하다', '제한적으로 된다'처럼 "
                    "완화하거나 중간 단계가 있는 것처럼 표현하지 마. 원문의 인정/불인정, "
                    "가능/불가능 여부를 절대 바꾸지 말고 그대로 전달해.\n"
                    "- 조건이 여러 단계로 얽혀 있는 경우(예: 특정 조건을 추가로 채워야만 "
                    "인정되는 경우), 결론만 말하지 말고 '지금 상태로는 어떻고, 어떤 조건을 "
                    "추가로 채워야 하는지'까지 구체적으로 answer에 포함해.\n"
                    "- ⚠️ 강퇴/미클처럼 결과가 갈리는 주제는, 원문에 그 결론에 "
                    "도달하는 방법(경로)이 여러 개 있다면 질문에서 직접 언급되지 않은 "
                    "경로라도 절대 누락하지 말고 전부 요약해서 알려줘. "
                    "(예: '방법A로도 되고, 방법B로도 된다'처럼 원문에 있는 경로를 다 훑어보고 "
                    "빠짐없이 반영해. 이때는 answer가 2~4줄을 넘어가도 괜찮아.)"
                )
            }]
        },
        "contents": [{
            "role": "user",
            "parts": [{
                "text": f"[공지 원문 목록]\n{combined_source}\n\n[사용자 질문]\n{user_message}"
            }]
        }],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json"
        }
    }

    max_attempts = max(1, len(API_KEYS) * len(MODELS) * 2)

    for _ in range(max_attempts):
        current_key = API_KEYS[API_KEY_INDEX]
        current_model = MODELS[MODEL_INDEX]
        url = (
            "https://generativelanguage.googleapis.com/"
            f"v1beta/models/{current_model}:generateContent"
            f"?key={current_key}"
        )
        try:
            res = session.post(url, json=payload, timeout=(3, 12))
            res.raise_for_status()
            candidates = res.json().get("candidates", [])
            if not candidates:
                return None
            for part in candidates[0].get("content", {}).get("parts", []):
                if "text" in part and part["text"].strip():
                    raw = part["text"].strip()
                    raw = re.sub(r"^```(json)?|```$", "", raw, flags=re.MULTILINE).strip()
                    try:
                        return json.loads(raw)
                    except Exception:
                        return {"answer": raw, "excerpts": []}
            return None
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 500
            print(f"🚨 공지 종합 답변 HTTP 오류 {status}: {e}")
            if status == 429:
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            elif status == 503:
                time.sleep(1)  # 일시적 과부하니 살짝 쉬었다가 재시도
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            elif is_gemini_auth_error(status, e.response):
                # 키/권한/결제 문제는 모델이 아니라 키를 바꿔야 함
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            else:
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue
        except Exception as e:
            print(f"🚨 공지 종합 답변 오류: {e}")
            MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue

    return None


def format_notice_answer_reply(data):
    """
    gemini_answer_notice_multi()의 결과를 LINE 메시지 텍스트로 포맷한다.
    원문 전체가 아니라 결론 + 필요한 부분만 발췌해서 보여준다.
    """
    if not data:
        return None

    answer = str(data.get("answer", "")).strip()
    excerpts = data.get("excerpts") or []

    parts = []
    if answer:
        parts.append(f"💬 {answer}")

    ex_lines = []
    for ex in excerpts[:3]:
        title = str(ex.get("title", "")).strip()
        body = str(ex.get("excerpt", "")).strip()
        if title and body:
            ex_lines.append(f"📌 {title}\n{body}")
    if ex_lines:
        parts.append("─────────\n" + "\n\n".join(ex_lines))

    return "\n\n".join(parts) if parts else None


def format_notice_clarification_reply(clarification_question):
    """
    정보가 부족해서 되물어야 할 때 사용자에게 보여줄 메시지를 만든다.
    """
    question = str(clarification_question or "").strip()
    if not question:
        return None
    return (
        f"❓ {question}\n\n"
        "(정확하게 안내해드리고 싶어서 여쭤봐요! 답을 알려주시면 이어서 확인해드릴게요 🙏)"
    )


# ================================================================
# 공지 되묻기(clarification) 보류 상태 저장/조회/삭제
#
# 되물은 직후 사용자가 보내는 "다음 메시지"를 그 되묻기에 대한 답으로
# 이어붙이기 위해, 원래 질문 + 판단에 썼던 공지 원문(rows) + 되물은
# 질문을 user_histories 테이블에 잠깐(PENDING_NOTICE_TTL_SECONDS) 저장한다.
# 기존 history/quiz_mode 저장 구조를 그대로 재사용하고, 그 안에
# "pending_notice" 키만 추가로 얹는 방식이라 기존 대화 흐름에는
# 영향을 주지 않는다.
# ================================================================
def get_pending_notice(user_id):
    """
    이 사용자가 방금 전 되묻기 질문을 받은 상태인지 확인한다.
    보류 상태가 없거나, 있어도 너무 오래돼서(TTL 초과) 더 이상
    이어붙이면 안 되는 경우 None을 반환한다.
    """
    if not user_id:
        return None
    try:
        res = supabase.table("user_histories").select("history").eq("user_id", user_id).limit(1).execute()
        if not res.data:
            return None
        raw = res.data[0].get("history") or {}
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = {}
        if not isinstance(raw, dict):
            return None

        pending = raw.get("pending_notice")
        if not isinstance(pending, dict) or not pending.get("question") or not pending.get("rows"):
            return None

        ts_str = pending.get("ts")
        if ts_str:
            try:
                ts = datetime.fromisoformat(ts_str)
                if (now_utc() - ts).total_seconds() > PENDING_NOTICE_TTL_SECONDS:
                    return None
            except Exception:
                pass

        return pending
    except Exception as e:
        print(f"⚠️ Supabase get_pending_notice 오류: {e}")
        return None


def save_pending_notice(user_id, question, rows, clarification_question):
    """
    되묻기 질문에 대한 답을 다음 메시지에서 이어받을 수 있도록
    원래 질문 + 근거 공지(rows) + 되물은 질문을 임시로 저장한다.
    """
    if not user_id:
        return
    try:
        history, quiz_mode, quiz_deadline, _ = get_user_state(user_id)
        row = {
            "user_id": user_id,
            "history": {
                "messages": history,
                "quiz_mode": bool(quiz_mode),
                "quiz_deadline": quiz_deadline,
                "pending_notice": {
                    "question": question,
                    "rows": rows,
                    "clarification_question": clarification_question,
                    "ts": now_utc().isoformat()
                }
            },
            "updated_at": now_utc().isoformat()
        }
        supabase.table("user_histories").upsert(row, on_conflict="user_id").execute()
    except Exception as e:
        print(f"⚠️ Supabase save_pending_notice 오류: {e}")


def clear_pending_notice(user_id):
    """
    보류 중이던 공지 되묻기 상태를 지운다 (답을 받았거나, 더 이상
    이어붙이면 안 될 때 호출). 기존 history/quiz_mode는 그대로 둔다.
    """
    if not user_id:
        return
    try:
        history, quiz_mode, quiz_deadline, _ = get_user_state(user_id)
        row = {
            "user_id": user_id,
            "history": {
                "messages": history,
                "quiz_mode": bool(quiz_mode),
                "quiz_deadline": quiz_deadline
            },
            "updated_at": now_utc().isoformat()
        }
        supabase.table("user_histories").upsert(row, on_conflict="user_id").execute()
    except Exception as e:
        print(f"⚠️ Supabase clear_pending_notice 오류: {e}")


# ================================================================
# 그룹(방)별 "현재 진행 중인 퀴즈 낸 사람" 추적
#
# 전용 테이블(group_quiz_state)을 따로 둔다. user_histories에 얹어 쓰던
# 이전 방식은 FK/JSON 경로 필터링 이슈로 안정적이지 않아서,
# 외래키 없는 단순한 key-value 테이블로 바꿨다.
#
#   create table if not exists group_quiz_state (
#     group_id text primary key,
#     owner_user_id text not null,
#     updated_at timestamptz not null default now()
#   );
# ================================================================
def set_quiz_owner(group_id, user_id):
    """이 방에서 지금 진행 중인 퀴즈를 낸 사람을 기록한다."""
    if not group_id or not user_id:
        return
    try:
        supabase.table("group_quiz_state").upsert({
            "group_id": group_id,
            "owner_user_id": user_id,
            "updated_at": now_utc().isoformat()
        }, on_conflict="group_id").execute()
    except Exception as e:
        print(f"⚠️ Supabase set_quiz_owner 오류: {e}")


def get_quiz_owner(group_id):
    """이 방에서 지금 진행 중인 퀴즈를 낸 사람의 user_id를 반환한다. 없으면 None."""
    if not group_id:
        return None
    try:
        res = supabase.table("group_quiz_state").select("owner_user_id").eq(
            "group_id", group_id
        ).limit(1).execute()
        if not res.data:
            return None
        return res.data[0].get("owner_user_id") or None
    except Exception as e:
        print(f"⚠️ Supabase get_quiz_owner 오류: {e}")
        return None


def clear_quiz_owner(group_id, user_id=None):
    """이 방의 퀴즈 진행자 기록을 지운다 (퀴즈가 끝났을 때 호출).
    user_id가 주어지면, 그 사이 다른 사람이 새로 퀴즈를 시작했을 때
    실수로 그 기록까지 지우지 않도록 owner가 일치할 때만 지운다."""
    if not group_id:
        return
    try:
        q = supabase.table("group_quiz_state").delete().eq("group_id", group_id)
        if user_id:
            q = q.eq("owner_user_id", user_id)
        q.execute()
    except Exception as e:
        print(f"⚠️ Supabase clear_quiz_owner 오류: {e}")


def gemini_answer_notice_followup(original_question, rows, followup_message):
    """
    한 번 되물어본 뒤, 사용자가 알려준 추가 정보(followup_message)를
    반영해서 최종 결론을 다시 만든다. 이번에는 정말 판단이 불가능한
    경우가 아니라면 되묻지 말고 결론을 내리도록 유도한다.
    """
    global API_KEY_INDEX, MODEL_INDEX

    if not rows:
        return None

    combined_source = "\n\n".join(
        f"[{str(r.get('키워드', '')).strip()}]\n{str(r.get('공지', '')).strip()}"
        for r in rows
    )

    payload = {
        "systemInstruction": {
            "parts": [{
                "text": (
                    "너는 커뮤니티 안내봇이야. 방금 전 사용자의 질문에 답하기 위해 "
                    "정보가 부족해서 한 번 되물었고, 지금은 사용자가 그 추가 정보를 "
                    "알려준 상황이야. 아래 공지 원문, 사용자의 원래 질문, 사용자가 "
                    "추가로 알려준 답변을 모두 종합해서 이번에는 가능한 한 명확한 "
                    "결론을 내려줘.\n\n"
                    "반드시 아래 JSON 형식으로만 응답해 (설명이나 코드블록 없이 JSON 객체만):\n"
                    "{\n"
                    '  "answer": "질문에 대한 결론을 다정한 말투로 (~해요, ~해줄게 체)",\n'
                    '  "excerpts": [\n'
                    '    {"title": "근거가 된 공지 제목", "excerpt": "판단 근거가 된 원문 중 필요한 부분만 발췌"}\n'
                    "  ]\n"
                    "}\n\n"
                    "규칙:\n"
                    "- 원문에 없는 내용은 절대 추측하거나 지어내지 마.\n"
                    "- 원문의 인정/불인정, 가능/불가능 여부를 절대 바꾸지 마.\n"
                    "- 사용자가 준 추가 답변으로도 여전히 판단이 정말 불가능하다면, "
                    "다시 되묻지 말고 answer에 어떤 정보가 아직 부족한지, 그래도 "
                    "지금 알 수 있는 선에서는 어떤지를 솔직하게 설명해줘."
                )
            }]
        },
        "contents": [{
            "role": "user",
            "parts": [{
                "text": (
                    f"[공지 원문 목록]\n{combined_source}\n\n"
                    f"[사용자의 원래 질문]\n{original_question}\n\n"
                    f"[봇이 되물은 뒤 사용자가 알려준 추가 답변]\n{followup_message}"
                )
            }]
        }],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json"
        }
    }

    max_attempts = max(1, len(API_KEYS) * len(MODELS) * 2)

    for _ in range(max_attempts):
        current_key = API_KEYS[API_KEY_INDEX]
        current_model = MODELS[MODEL_INDEX]
        url = (
            "https://generativelanguage.googleapis.com/"
            f"v1beta/models/{current_model}:generateContent"
            f"?key={current_key}"
        )
        try:
            res = session.post(url, json=payload, timeout=(3, 12))
            res.raise_for_status()
            candidates = res.json().get("candidates", [])
            if not candidates:
                return None
            for part in candidates[0].get("content", {}).get("parts", []):
                if "text" in part and part["text"].strip():
                    raw = part["text"].strip()
                    raw = re.sub(r"^```(json)?|```$", "", raw, flags=re.MULTILINE).strip()
                    try:
                        return json.loads(raw)
                    except Exception:
                        return {"answer": raw, "excerpts": []}
            return None
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 500
            print(f"🚨 공지 되묻기 후속 답변 HTTP 오류 {status}: {e}")
            if status == 429:
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            elif status == 503:
                time.sleep(1)  # 일시적 과부하니 살짝 쉬었다가 재시도
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            elif is_gemini_auth_error(status, e.response):
                # 키/권한/결제 문제는 모델이 아니라 키를 바꿔야 함
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            else:
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue
        except Exception as e:
            print(f"🚨 공지 되묻기 후속 답변 오류: {e}")
            MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue

    return None


def resolve_notice_clarification(pending, followup_message):
    """
    보류 중이던 되묻기(pending)에 사용자의 답변(followup_message)을 이어붙여
    최종 답변 텍스트를 만든다. 실패하면 None (호출부가 기존 방식으로 폴백).
    """
    if not pending:
        return None
    data = gemini_answer_notice_followup(
        pending.get("question", ""),
        pending.get("rows", []),
        followup_message
    )
    return format_notice_answer_reply(data)


def handle_notice_question(user_message, user_id=None):
    """
    공지 관련 질문 처리 진입점.
    여러 공지 항목을 조합해서 추론한 답변 + 필요한 부분만 발췌한 결과를 만든다.
    (LLM 호출 실패 등으로 만들 수 없으면 None을 반환해서
     호출부가 기존 단일 키워드 방식으로 폴백할 수 있게 한다.)

    질문이 애매해서 정확한 결론을 낼 수 없는 경우, 추측 답변 대신
    사용자에게 되묻는다. user_id가 주어지면 그 되묻기 상태를 잠깐
    저장해서, 사용자가 다음 메시지로 알려주는 추가 정보를 이어붙여
    다시 답할 수 있게 한다.
    """
    keywords = gemini_extract_notice_topics(user_message)

    if not keywords:
        fallback_kw = simple_extract_notice_keyword(user_message)
        keywords = [fallback_kw] if fallback_kw else []

    if not keywords:
        return None

    rows = query_notice_rows_multi(keywords)
    if not rows:
        return None

    data = gemini_answer_notice_multi(user_message, rows)
    if not data:
        return None

    if data.get("needs_clarification"):
        clarify_q = str(data.get("clarification_question", "")).strip()
        reply = format_notice_clarification_reply(clarify_q)
        if not reply:
            # 되물을 질문조차 못 만들었으면 애매하게 두지 말고 폴백시킨다.
            return None
        if user_id:
            save_pending_notice(user_id, user_message, rows, clarify_q)
        return reply

    return format_notice_answer_reply(data)

@app.route("/internal/export-logs", methods=["POST"])
def internal_export_logs():
    token = request.headers.get("X-Internal-Token") or request.args.get("token")
    if not token or token != EXPORT_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    target_date = data.get("date")
    temp = bool(data.get("temp", False))

    if not target_date:
        kst = timezone(timedelta(hours=9))
        target_date = datetime.now(kst).strftime("%Y-%m-%d")

    result = export_logs_to_sheet(target_date, temp=temp)
    return jsonify({"result": result})

# ================================================================
# Vercel Cron 전용: 자정 글마디 자동 백업
#
# 베르셀은 서버리스 환경이라 threading + time.sleep()으로 만든
# "자정까지 대기하는 백그라운드 루프"가 동작하지 않습니다.
# (모듈이 import만 되고 __main__으로 실행되지 않으며,
#  설사 실행되어도 요청이 없으면 프로세스가 유지되지 않습니다.)
#
# 대신 vercel.json의 cron 설정이 KST 자정에 이 엔드포인트를
# 직접 호출해서 "전날" 데이터를 백업하도록 합니다.
# ================================================================
@app.route("/cron/daily-save", methods=["GET", "POST"])
def cron_archive_chatlog():
    # Vercel Cron은 기본적으로 Authorization: Bearer <CRON_SECRET> 헤더를 보냅니다.
    auth_header = request.headers.get("Authorization", "")
    token = auth_header.replace("Bearer ", "").strip() or request.args.get("token")

    cron_secret = os.environ.get("CRON_SECRET") or EXPORT_SECRET
    if not cron_secret or token != cron_secret:
        return jsonify({"error": "Unauthorized"}), 401

    kst = timezone(timedelta(hours=9))
    archive_date = (datetime.now(kst) - timedelta(days=1)).strftime("%Y-%m-%d")

    ok = archive_chatlog_date(archive_date)

    try:
        doc = get_daily_archive_doc()
        if doc:
            cleanup_old_sheets(doc)
    except Exception as e:
        print(f"⚠️ 크론 오래된 시트 정리 실패: {e}")

    # ✨ [추가됨] 퇴장 유예기간(6주) 경과자 실제 삭제도 매일 자정 크론에서 같이 처리
    try:
        purge_ok, purge_msg = purge_expired_left_members()
        print(f"🧹 퇴장자 정리: {purge_msg}" if purge_ok else f"⚠️ 퇴장자 정리 실패: {purge_msg}")
    except Exception as e:
        print(f"⚠️ 크론 퇴장자 정리 오류: {e}")

    return jsonify({"date": archive_date, "success": ok})


def now_utc():
    return datetime.now(timezone.utc)

def supabase_upsert_line_users(rows, batch_size=200):
    """
    ✨ [변경됨] 한 번에 다 upsert하면(수백~수천 건) Supabase가 504 Gateway Timeout을
    내는 경우가 있어서, batch_size 단위로 쪼개서 여러 번 upsert 하도록 변경.
    일부 배치가 실패해도 나머지 배치는 계속 시도하고, 실패한 배치가 있으면
    (False, 실패메시지)를 반환한다.
    """
    if not rows:
        return (True, "빈 목록")

    total_done = 0
    failed_msgs = []
    for i in range(0, len(rows), batch_size):
        chunk = rows[i:i + batch_size]
        try:
            res = supabase.table("line_users").upsert(chunk, on_conflict="line_user_id").execute()
            total_done += len(res.data) if res.data else len(chunk)
        except Exception as e:
            print(f"🚨 Supabase upsert 실패 (batch {i // batch_size + 1}, raw error): {repr(e)}")
            failed_msgs.append(str(e))

    if failed_msgs:
        return (False, f"{total_done}건 반영 후 실패 발생: {failed_msgs[0]}")
    return (True, f"{total_done}건 반영")
        
def supabase_get_all_line_users():
    try:
        q = supabase.table("line_users").select("line_user_id,nickname,sheet_row,updated_at,is_excluded,join_date,leave_date,gender")
        return fetch_all_supabase_query(q)
    except Exception as e:
        print("⚠️ Supabase line_users 조회 오류:", e)
        return []

def supabase_set_profile_cache(line_user_id, nickname, ttl_seconds=PROFILE_CACHE_TTL):
    try:
        expires_at = (now_utc() + timedelta(seconds=ttl_seconds)).isoformat()
        row = {"line_user_id": line_user_id, "nickname": nickname, "expires_at": expires_at}
        supabase.table("profile_cache").upsert(row, on_conflict="line_user_id").execute()
        return True
    except Exception as e:
        print("⚠️ Supabase profile_cache upsert 오류:", e)
        return False

def supabase_get_profile_cache_valid(line_user_id):
    try:
        now_iso = now_utc().isoformat()
        res = supabase.table("profile_cache").select("nickname,expires_at").eq("line_user_id", line_user_id).gte("expires_at", now_iso).execute()
        if res.data:
            return res.data[0].get("nickname")
        return None
    except Exception as e:
        print("⚠️ Supabase profile_cache 조회 오류:", e)
        return None

def supabase_delete_profile_cache_all():
    try:
        supabase.table("profile_cache").delete().neq("line_user_id", "__keep__").execute()
        return True
    except Exception as e:
        print("⚠️ Supabase profile_cache 전체 삭제 오류:", e)
        return False

def supabase_delete_profile_cache_stale(keep_ids):
    """
    ✨ [변경됨] 전체 삭제(supabase_delete_profile_cache_all) 대신,
    구글 시트(keep_ids)에 더 이상 존재하지 않는 line_user_id의
    profile_cache 행만 골라서 삭제한다. (업서트 + 잔여분만 삭제)
    """
    try:
        keep_ids = {str(uid).strip() for uid in (keep_ids or []) if str(uid).strip()}
        res = supabase.table("profile_cache").select("line_user_id").execute()
        existing_ids = {str(r.get("line_user_id") or "").strip() for r in (res.data or [])}
        stale_ids = [uid for uid in existing_ids if uid and uid not in keep_ids]
        if stale_ids:
            supabase.table("profile_cache").delete().in_("line_user_id", stale_ids).execute()
        return True, len(stale_ids)
    except Exception as e:
        print("⚠️ Supabase profile_cache 잔여분 삭제 오류:", e)
        return False, 0

def supabase_insert_meet_logs(rows):
    if not rows:
        return True
    try:
        supabase.table("meet_log").insert(rows).execute()
        return True
    except Exception as e:
        print("⚠️ Supabase meet_log insert 오류:", e)
        return False

def supabase_upsert_individual_greetings(rows):
    if not rows:
        return True
    try:
        supabase.table("개별인사말").upsert(rows, on_conflict="닉네임").execute()
        return True
    except Exception as e:
        print("⚠️ Supabase 개별인사말 upsert 오류:", e)
        return False

def supabase_upsert_notices(rows):
    if not rows:
        return True
    try:
        supabase.table("공지").upsert(rows, on_conflict="키워드").execute()
        return True
    except Exception as e:
        print("⚠️ Supabase 공지 upsert 오류:", e)
        return False

def supabase_get_chatlog_user_ids_for_dates(dates):
    try:
        q = supabase.table("chat_logs").select("user_id").in_("log_date", dates)
        res_data = fetch_all_supabase_query(q)
        ids = set()
        for r in res_data:
            uid = (r.get("user_id") or "").strip()
            if uid and uid.startswith("U"):
                ids.add(uid)
        return ids
    except Exception as e:
        print("⚠️ Supabase chat_logs 조회 오류:", e)
        return set()

def supabase_insert_chat_log(entry):
    """
    chat_logs에 메시지를 저장합니다.

    ⚠️ LINE 웹훅은 응답이 늦으면 같은 이벤트를 재전송할 수 있고,
    Vercel 콜드스타트/Gemini 재시도 등으로 처리 시간이 길어지면
    이 재전송이 실제로 발생해서 같은 메시지가 두 번 저장될 수 있습니다.

    entry에 line_message_id가 있으면, DB의 UNIQUE 제약(line_message_id)을
    이용한 upsert로 "이미 저장된 메시지면 조용히 무시"하도록 처리해서
    중복 삽입을 원천 차단합니다. (line_message_id가 없는 예전 호출부는
    기존처럼 단순 insert로 폴백)
    """
    try:
        if entry.get("line_message_id"):
            supabase.table("chat_logs").upsert(
                entry,
                on_conflict="line_message_id",
                ignore_duplicates=True
            ).execute()
        else:
            supabase.table("chat_logs").insert(entry).execute()
        return True
    except Exception as e:
        print("⚠️ Supabase chat_logs insert 오류:", e)
        return False

def cleanup_old_chat_logs_db(retention_weeks=CHAT_LOG_DB_RETENTION_WEEKS, batch_sleep_seconds=0.3):
    """
    Supabase DB의 chat_logs 테이블에서
    retention_weeks(기본 6주)보다 오래된 행을 삭제합니다.

    구글시트 백업(cleanup_old_sheets)과는 별개로,
    실제 DB 원본 데이터의 보관 기간을 제한하기 위한 함수입니다.
    log_date(YYYY-MM-DD 문자열) 기준으로 cutoff 이전 데이터를 삭제합니다.

    ✨ 대량 삭제로 인한 DB 부하를 피하기 위해,
    한 번에 전체를 지우지 않고 '삭제 대상 날짜'를 먼저 조회한 뒤
    날짜 단위(하루치)로 나눠서 순차적으로 삭제합니다.
    (특히 이 기능을 처음 배포할 때, 6주보다 훨씬 오래된 데이터가
    쌓여 있어도 한 번에 몰아서 지우지 않도록 하기 위함)
    """
    if not supabase:
        return "⚠️ Supabase 연결이 필요합니다."

    try:
        kst = timezone(timedelta(hours=9))
        cutoff_date = datetime.now(kst).date() - timedelta(weeks=retention_weeks)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        # ----------------------------------------------------
        # 삭제 대상이 되는 날짜 목록만 먼저 조회
        # (1,000건 제한 우회를 위해 fetch_all_supabase_query 사용)
        # ----------------------------------------------------
        date_query = supabase.table("chat_logs").select("log_date").lt("log_date", cutoff_str)
        rows = fetch_all_supabase_query(date_query)
        old_dates = sorted({r.get("log_date") for r in rows if r.get("log_date")})

        if not old_dates:
            msg = f"✅ DB chat_logs 정리: 삭제 대상 없음 (기준일 {cutoff_str} 이전)"
            print(msg)
            return msg

        # ----------------------------------------------------
        # 날짜 하루씩 나눠서 삭제 (대량 삭제 부하 방지)
        # ----------------------------------------------------
        deleted_dates = []
        failed_dates = []

        for target_date in old_dates:
            try:
                supabase.table("chat_logs").delete().eq("log_date", target_date).execute()
                deleted_dates.append(target_date)
            except Exception as e:
                print(f"⚠️ chat_logs 삭제 실패 ({target_date}): {e}")
                failed_dates.append(target_date)

            # 하루치씩 지울 때마다 짧게 텀을 둬서 DB 부하 분산
            time.sleep(batch_sleep_seconds)

        msg = (
            f"✅ DB chat_logs 정리 완료: {len(deleted_dates)}개 날짜 삭제"
            f" (기준일 {cutoff_str} 이전, 보관기간 {retention_weeks}주)"
        )
        if failed_dates:
            msg += f" | ⚠️ 삭제 실패 날짜: {', '.join(failed_dates)}"

        print(msg)
        return msg

    except Exception as e:
        msg = f"🚨 DB chat_logs 정리 오류: {e}"
        print(msg)
        return msg

# ================================================================
# 라인유저아이디 시트 연결
# ================================================================
# ✨ [추가됨] 라인유저 시트("아이디" 탭) 공용 헤더 — 입장일/퇴장일 컬럼 추가
LINE_USER_SHEET_HEADER = ["LINE User ID", "닉네임", "제외", "마니또", "입장일", "퇴장일", "성별"]

# ✨ [변경됨] 닉네임에 포함된 이모지로 성별을 판별한다.
# 기준 목록은 Supabase "gender_emojis" 테이블(emoji, gender['남'/'여'])에서 읽는다.
# 테이블이 비어있거나 조회에 실패하면 아래 기본 목록(폴백)을 쓴다.
FEMALE_GENDER_EMOJIS = ["🪷", "⚖️"]
MALE_GENDER_EMOJIS = ["🐛", "🪻", "🏹", "🧨"]

GENDER_EMOJI_TABLE = "gender_emojis"
GENDER_EMOJI_CACHE_TTL = 300  # 초 — DB 수정 후 최대 5분 뒤 반영
_gender_emoji_cache = {"map": None, "ts": 0.0}

def _strip_variation_selector(text):
    # ⚖️ 처럼 이모지 뒤에 붙는 보이지 않는 변형선택자(U+FE0F)를 제거해
    # DB에 변형선택자 없이 입력해도 매칭되게 한다.
    return str(text or "").replace("\ufe0f", "").strip()

def load_gender_emoji_map(force=False):
    """{이모지: '남'|'여'} 반환. 5분 캐시, DB 실패/빈 테이블이면 기본 목록 폴백."""
    now = time.time()
    cached = _gender_emoji_cache["map"]
    if not force and cached is not None and now - _gender_emoji_cache["ts"] < GENDER_EMOJI_CACHE_TTL:
        return cached

    emoji_map = {}
    try:
        rows = supabase.table(GENDER_EMOJI_TABLE).select("emoji,gender").execute().data or []
        for r in rows:
            e = _strip_variation_selector(r.get("emoji"))
            g = str(r.get("gender") or "").strip()
            if e and g in ("남", "여"):
                emoji_map[e] = g
    except Exception as e:
        print(f"⚠️ gender_emojis 조회 실패 (기존 캐시/기본 목록 사용): {e}")
        if cached is not None:
            _gender_emoji_cache["ts"] = now
            return cached

    if not emoji_map:
        emoji_map = {_strip_variation_selector(e): "여" for e in FEMALE_GENDER_EMOJIS}
        emoji_map.update({_strip_variation_selector(e): "남" for e in MALE_GENDER_EMOJIS})

    _gender_emoji_cache["map"] = emoji_map
    _gender_emoji_cache["ts"] = now
    return emoji_map

def derive_gender_from_nickname(nickname):
    """
    닉네임에 포함된 이모지로 성별을 판별한다 (기준: gender_emojis 테이블).
    판별 불가면 "" 를 반환한다 — 호출부는 "" 일 때 기존 성별을 덮어쓰면 안 된다.
    """
    nick = _strip_variation_selector(nickname)
    if not nick:
        return ""
    found = {g for e, g in load_gender_emoji_map().items() if e in nick}
    if "여" in found:
        return "여"
    if "남" in found:
        return "남"
    return ""

def get_line_user_sheet():
    global line_user_doc, user_id_sheet, admin_sheet

    if user_id_sheet is not None:
        return user_id_sheet

    try:
        if line_user_doc is None:
            line_user_doc = client.open("라인유저아이디")

        try:
            user_id_sheet = line_user_doc.worksheet("아이디")
            # ✨ [추가됨] 예전 시트(4열)에 입장일/퇴장일(5,6열)이 없으면
            # 헤더만 자동으로 확장한다 (기존 데이터 행은 건드리지 않음 —
            # 빈 칸은 각 함수가 알아서 ""로 처리한다).
            try:
                existing_header = user_id_sheet.row_values(1)
                if existing_header[:7] != LINE_USER_SHEET_HEADER:
                    user_id_sheet.update(values=[LINE_USER_SHEET_HEADER], range_name="A1")
            except Exception as header_e:
                print(f"⚠️ 라인유저 시트 헤더 확장 실패: {header_e}")
        except gspread.exceptions.WorksheetNotFound:
            user_id_sheet = line_user_doc.add_worksheet(title="아이디", rows=1000, cols=7)
            # 제외/마니또/입장일/퇴장일 열 추가 반영
            user_id_sheet.update(values=[LINE_USER_SHEET_HEADER], range_name="A1")

        try:
            admin_sheet = line_user_doc.worksheet("관리자")
        except gspread.exceptions.WorksheetNotFound:
            admin_sheet = line_user_doc.add_worksheet(title="관리자", rows=100, cols=4)
            admin_sheet.update(values=[["트로피(부방장) 닉네임", "핀(관리자) 닉네임", "북(인증자) 닉네임", "홍보대장 닉네임"]], range_name="A1")

        return user_id_sheet
    except Exception as e:
        print(f"⚠️ '라인유저아이디' 시트 연결 실패: {e}")
        user_id_sheet = None
        admin_sheet = None
        return None

def get_admin_sheet():
    global line_user_doc, admin_sheet
    
    if admin_sheet is not None:
        return admin_sheet
        
    try:
        if line_user_doc is None:
            get_line_user_sheet()
            
        if line_user_doc is None:
            return None

        try:
            admin_sheet = line_user_doc.worksheet("관리자")
        except gspread.exceptions.WorksheetNotFound:
            admin_sheet = line_user_doc.add_worksheet(title="관리자", rows=100, cols=4)
            admin_sheet.update(values=[["트로피(부방장) 닉네임", "핀(관리자) 닉네임", "북(인증자) 닉네임", "홍보대장 닉네임"]], range_name="A1")
            
        return admin_sheet
    except Exception as e:
        print(f"⚠️ 관리자 시트 연결 실패: {e}")
        admin_sheet = None
        return None

def sync_admins_to_db_from_sheet():
    if not supabase:
        return (False, "⚠️ Supabase 연결이 필요합니다.")

    try:
        admin_ws = get_admin_sheet()
        if not admin_ws:
            return (False, "⚠️ 관리자 시트를 찾을 수 없습니다.")

        records = admin_ws.get_all_values()
        if not records or len(records) < 2:
            return (True, "관리자 시트에 등록된 운영진이 없습니다.")

        headers = records[0] 
        user_map = get_line_user_map()  # uid -> {nickname, ...} (표시용 최신 닉네임 확인에 사용)
        rows = []
        not_found = []

        for row in records[1:]:
            for col_idx, cell in enumerate(row):
                # ✨ [변경됨] 시트 칸에는 이제 닉네임을 넣는다. resolve_admin_cell_to_user_id가
                # line_users에서 닉네임(한글 기준)으로 실제 line_user_id를 찾아준다.
                # (예전처럼 U로 시작하는 ID를 그대로 넣어놔도 하위 호환됨)
                raw_value = str(cell).strip()
                if not raw_value:
                    continue
                uid = resolve_admin_cell_to_user_id(raw_value)
                if not uid:
                    not_found.append(raw_value)
                    continue

                role_name = headers[col_idx] if col_idx < len(headers) else "기타"
                nickname = user_map.get(uid, {}).get("nickname", "") or raw_value

                rows.append({
                    "line_user_id": uid,
                    "nickname": nickname,
                    "role": role_name
                })

        if not rows:
            msg = "관리자 시트에 유효한 이름이 없습니다."
            if not_found:
                msg += f" (매칭 실패: {', '.join(not_found)})"
            return (True, msg)

        supabase.table("admin_users").upsert(rows, on_conflict="line_user_id").execute()

        # ✨ [추가됨] 시트에서 빠진 사람은 admin_users에서도 삭제 (동기화 = upsert + 삭제)
        new_ids = {r["line_user_id"] for r in rows}
        removed_count = 0
        removed_nicknames = []
        try:
            existing_res = supabase.table("admin_users").select("line_user_id, nickname").execute()
            existing_rows = existing_res.data or []
            stale_ids = [r["line_user_id"] for r in existing_rows if r["line_user_id"] not in new_ids]
            if stale_ids:
                supabase.table("admin_users").delete().in_("line_user_id", stale_ids).execute()
                removed_count = len(stale_ids)
                stale_id_set = set(stale_ids)
                removed_nicknames = [r.get("nickname", r["line_user_id"]) for r in existing_rows if r["line_user_id"] in stale_id_set]
        except Exception as del_e:
            print(f"⚠️ 운영진 삭제(정리) 중 오류: {del_e}")

        unique_count = len(new_ids)
        msg = f"운영진 {unique_count}명 Supabase 동기화 완료 (역할/닉네임 포함)"
        if removed_count:
            msg += f"\n🗑️ 시트에서 빠져서 삭제됨: {', '.join(removed_nicknames)} ({removed_count}명)"
        if not_found:
            msg += f"\n⚠️ 매칭 실패(닉네임 확인 필요): {', '.join(not_found)}"
        return (True, msg)
        
    except Exception as e:
        return (False, f"운영진 동기화 중 오류: {e}")

# ================================================================
# 봇 사용폴더 연결
# ================================================================
def get_eden_sheets():
    global eden_doc, sheet, notice_sheet, bung_sheet, image_sheet

    if (
        sheet is not None
        and notice_sheet is not None
        and bung_sheet is not None
        and image_sheet is not None
    ):
        return (sheet, notice_sheet, bung_sheet, image_sheet)

    try:
        if eden_doc is None:
            eden_doc = client.open("봇 사용폴더")

        try:
            sheet = eden_doc.worksheet("개별 인사말")
        except gspread.exceptions.WorksheetNotFound:
            sheet = eden_doc.add_worksheet(
                title="개별 인사말",
                rows=100,
                cols=2
            )
            sheet.update(
                values=[["닉네임", "인사말"]],
                range_name="A1"
            )

        try:
            notice_sheet = eden_doc.worksheet("공지")
        except gspread.exceptions.WorksheetNotFound:
            notice_sheet = eden_doc.add_worksheet(
                title="공지",
                rows=100,
                cols=2
            )
            notice_sheet.update(
                values=[["키워드", "내용"]],
                range_name="A1"
            )

        try:
            bung_sheet = eden_doc.worksheet("벙지")
        except gspread.exceptions.WorksheetNotFound:
            bung_sheet = eden_doc.add_worksheet(
                title="벙지",
                rows=100,
                cols=6
            )
            bung_sheet.update(
                values=[[
                    "작성자",
                    "날짜",
                    "벙멤버",
                    "장소",
                    "벙제",
                    "진행상황"
                ]],
                range_name="A1"
            )

        # 이미지파일 시트
        try:
            image_sheet = eden_doc.worksheet("이미지파일")
        except gspread.exceptions.WorksheetNotFound:
            image_sheet = eden_doc.add_worksheet(
                title="이미지파일",
                rows=100,
                cols=2
            )
            image_sheet.update(
                values=[["키워드", "파일명"]],
                range_name="A1"
            )

        return (sheet, notice_sheet, bung_sheet, image_sheet)

    except Exception as e:
        print(f"⚠️ '봇 사용폴더' 파일 연결 실패: {e}")
        return (None, None, None, None)
# ================================================================
# 구글 시트 -> Supabase 동기화 함수들
# ================================================================
def normalize_sheet_date(date_raw):
    """
    ✨ [추가됨] 구글 시트 날짜 셀은 표시 형식에 따라 "2026. 9. 18"처럼
    로케일(점+공백) 포맷 문자열로 읽혀올 수 있다.
    Supabase의 date 컬럼은 "YYYY-MM-DD" 형식만 허용하므로(22007 오류),
    DB에 넣기 전에 여기서 항상 ISO 형식으로 정규화한다.
    인식 실패 시 원본 문자열을 그대로 반환한다(기존 동작 유지).
    """
    s = (date_raw or "").strip()
    if not s:
        return s

    # 이미 YYYY-MM-DD 형식이면 그대로 반환
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s

    # "2026. 9. 18" / "2026.9.18" / "2026/9/18" / "2026년 9월 18일" 등
    m = re.match(r"^(\d{4})[.\-/년]\s*(\d{1,2})[.\-/월]\s*(\d{1,2})일?\.?$", s)
    if m:
        y, mo, d = m.groups()
        try:
            return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
        except ValueError:
            return s

    return s


def sync_line_users_sheet_to_db():
    ws = get_line_user_sheet()
    if ws is None:
        return (False, "라인유저아이디 시트가 없습니다.")
    try:
        records = ws.get_all_values()
        unique_rows = {}
        excluded_in_sheet = 0

        for row_index, row in enumerate(records, start=1):
            if not row:
                continue
            uid = str(row[0]).strip()
            if not uid or uid == "LINE User ID":
                continue
            nickname = str(row[1]).strip() if len(row) >= 2 else ""
            is_excluded = 1 if len(row) >= 3 and str(row[2]).strip() == "1" else 0
            if is_excluded:
                excluded_in_sheet += 1

            # ✨ [추가됨] D열: 마니또 (콤마로 구분된 2글자 닉네임들, 원문 그대로 저장)
            manito_raw = str(row[3]).strip() if len(row) >= 4 else ""

            row_data = {
                "line_user_id": uid,
                "nickname": nickname,
                "is_excluded": is_excluded,
                "manito": manito_raw,
                "sheet_row": row_index,
                "updated_at": now_utc().isoformat()
            }

            # ✨ [추가됨] E/F열: 입장일/퇴장일 — 시트에 값이 있을 때만 DB에 반영
            # (관리자가 시트에서 수동으로 고친 경우를 위함. 비어있으면 DB에
            # 자동 기록돼 있던 값을 그대로 두고 건드리지 않는다)
            join_date_raw = normalize_sheet_date(str(row[4]).strip() if len(row) >= 5 else "")
            leave_date_raw = normalize_sheet_date(str(row[5]).strip() if len(row) >= 6 else "")
            if join_date_raw:
                row_data["join_date"] = join_date_raw
            if leave_date_raw:
                row_data["leave_date"] = leave_date_raw

            # ✨ [추가됨] G열: 성별 — 시트 값 기준으로 DB에 반영 ("아이디 갱신"시에도 동일)
            gender_raw = str(row[6]).strip() if len(row) >= 7 else ""
            row_data["gender"] = gender_raw

            unique_rows[uid] = row_data

        rows = list(unique_rows.values())

        if rows:
            ok, msg = supabase_upsert_line_users(rows)
            if not ok:
                return (False, f"Supabase 동기화 실패: {msg}")

        # ✨ [변경됨] 전체 삭제 후 재업로드 대신, 시트에 더 이상 없는
        # line_user_id만 골라서 line_users에서 삭제 (업서트 + 잔여분만 삭제)
        removed_count = 0
        try:
            sheet_ids = set(unique_rows.keys())
            existing_res = supabase.table("line_users").select("line_user_id").execute()
            existing_ids = {str(r.get("line_user_id") or "").strip() for r in (existing_res.data or [])}
            stale_ids = [uid for uid in existing_ids if uid and uid not in sheet_ids]
            if stale_ids:
                supabase.table("line_users").delete().in_("line_user_id", stale_ids).execute()
                removed_count = len(stale_ids)
        except Exception as del_e:
            print(f"⚠️ line_users 잔여분 삭제 오류: {del_e}")

        return (True, f"라인유저아이디 {len(rows)}건 동기화 (시트상 제외대상 {excluded_in_sheet}명 포함, 삭제 {removed_count}명)")
    except Exception as e:
        return (False, f"라인유저아이디 동기화 오류: {e}")
        
def sync_individual_greetings_to_db():
    ws, _, _, _  = get_eden_sheets()
    if not ws:
        return (False, "개별 인사말 시트가 없습니다.")
    try:
        records = ws.get_all_values()
        rows = []
        for i, row in enumerate(records):
            if i == 0:
                continue
            row.extend([""] * 2)
            nickname = row[0].strip()
            greeting = row[1].strip()
            if not nickname:
                continue
            rows.append({"닉네임": nickname, "인사말": greeting})

        removed_count = 0
        if rows:
            supabase_upsert_individual_greetings(rows)
            # ✨ [추가됨] 시트에 없는 닉네임만 골라서 삭제 (업서트 + 잔여분만 삭제)
            try:
                sheet_names = {r["닉네임"] for r in rows}
                existing_res = supabase.table("개별인사말").select("닉네임").execute()
                existing_names = {str(r.get("닉네임") or "").strip() for r in (existing_res.data or [])}
                stale_names = [n for n in existing_names if n and n not in sheet_names]
                if stale_names:
                    supabase.table("개별인사말").delete().in_("닉네임", stale_names).execute()
                    removed_count = len(stale_names)
            except Exception as del_e:
                print(f"⚠️ 개별인사말 잔여분 삭제 오류: {del_e}")

        return (True, f"개별 인사말 {len(rows)}건 동기화 (삭제 {removed_count}건)")
    except Exception as e:
        return (False, f"개별 인사말 동기화 오류: {e}")

def sync_notice_to_db():
    _, notice_ws, _, _ = get_eden_sheets()
    if not notice_ws:
        return (False, "공지 시트가 없습니다.")
    try:
        records = notice_ws.get_all_values()
        rows = []
        for i, row in enumerate(records):
            if i == 0:
                continue
            row.extend([""] * 2)
            keyword = row[0].strip()
            content = row[1].strip()
            if not keyword:
                continue
            rows.append({"키워드": keyword, "공지": content})

        removed_count = 0
        if rows:
            supabase_upsert_notices(rows)
            # ✨ [추가됨] 시트에 없는 키워드만 골라서 삭제 (업서트 + 잔여분만 삭제)
            try:
                sheet_keywords = {r["키워드"] for r in rows}
                existing_res = supabase.table("공지").select("키워드").execute()
                existing_keywords = {str(r.get("키워드") or "").strip() for r in (existing_res.data or [])}
                stale_keywords = [k for k in existing_keywords if k and k not in sheet_keywords]
                if stale_keywords:
                    supabase.table("공지").delete().in_("키워드", stale_keywords).execute()
                    removed_count = len(stale_keywords)
            except Exception as del_e:
                print(f"⚠️ 공지 잔여분 삭제 오류: {del_e}")

        return (True, f"공지 {len(rows)}건 동기화 (삭제 {removed_count}건)")
    except Exception as e:
        return (False, f"공지 동기화 오류: {e}")

def compact_line_user_sheet():
    ws = get_line_user_sheet()
    if ws is None:
        return (False, "시트가 연결되지 않았습니다.")

    try:
        records = ws.get_all_values()
        if not records:
            return (False, "시트에 데이터가 없습니다.")

        header = LINE_USER_SHEET_HEADER

        seen = {}
        duplicate_count = 0
        for row in records[1:]:
            if not row:
                continue
            uid = str(row[0]).strip() if len(row) >= 1 else ""
            if not uid:
                continue
            nickname = str(row[1]).strip() if len(row) >= 2 else ""
            excluded = str(row[2]).strip() if len(row) >= 3 else "0"
            # ✨ [수정됨] D~G열(마니또/입장일/퇴장일/성별)을 빼먹고 재작성하면
            # 데이터가 통째로 사라지므로, 반드시 같이 들고 다녀야 한다.
            manito = str(row[3]).strip() if len(row) >= 4 else ""
            join_date = normalize_sheet_date(str(row[4]).strip() if len(row) >= 5 else "")
            leave_date = normalize_sheet_date(str(row[5]).strip() if len(row) >= 6 else "")
            gender = str(row[6]).strip() if len(row) >= 7 else ""

            if uid in seen:
                duplicate_count += 1
                # 나중에 나온 행이 더 최신 정보일 가능성이 높으므로 덮어씀
            seen[uid] = [uid, nickname, excluded, manito, join_date, leave_date, gender]

        data_rows = list(seen.values())
        if not data_rows:
            return (True, "정리할 데이터가 없습니다.")

        removed_blank = (len(records) - 1) - (len(data_rows) + duplicate_count)

        ws.clear()
        ws.update(values=[header] + data_rows, range_name="A1")

        rows_to_upsert = []
        for idx, row in enumerate(data_rows, start=2):
            uid, nickname, excluded, manito, join_date, leave_date, gender = row
            row_data = {
                "line_user_id": uid,
                "nickname": nickname,
                "is_excluded": 1 if excluded == "1" else 0,
                "manito": manito,
                "sheet_row": idx,
                "updated_at": now_utc().isoformat(),
                "gender": gender,
            }
            if join_date:
                row_data["join_date"] = join_date
            if leave_date:
                row_data["leave_date"] = leave_date
            rows_to_upsert.append(row_data)

        ok, msg = supabase_upsert_line_users(rows_to_upsert)
        if not ok:
            return (False, f"시트 정리는 완료됐지만 DB 재동기화 실패: {msg}")

        return (True, f"🧹 빈줄 {max(removed_blank,0)}개 제거, 중복 {duplicate_count}건 제거, 총 {len(data_rows)}명으로 압축 완료")
    except Exception as e:
        return (False, f"시트 정리 중 오류: {e}")

# ================================================================
# ✨ [추가됨] 라인유저 아이디 삭제 공용 헬퍼
#
# - "/아이디정리"(최근 N일 비활동자 일괄 삭제)와
#   MemberLeftEvent(방 퇴장 시 개별 삭제) 양쪽에서 공용으로 사용한다.
# - compact_line_user_sheet와 동일하게 시트를 통째로 지우고 다시 쓰는
#   방식이라, 특정 행만 delete_rows로 지울 때 생기는 인덱스 밀림 문제가 없다.
# ================================================================
def delete_line_users_by_ids(uid_list):
    """
    주어진 line_user_id 목록을 라인유저 시트("아이디" 탭) +
    Supabase(line_users, profile_cache)에서 삭제한다.

    반환: (성공 여부, 실제로 삭제된 닉네임 리스트)
    """
    uid_set = {str(uid).strip() for uid in (uid_list or []) if str(uid).strip()}
    if not uid_set:
        return (True, [])

    ws = get_line_user_sheet()
    if ws is None:
        return (False, [])

    try:
        records = ws.get_all_values()
        header = LINE_USER_SHEET_HEADER

        kept_rows = []
        removed_nicknames = []
        for row in records[1:]:
            if not row:
                continue
            uid = str(row[0]).strip() if len(row) >= 1 else ""
            if not uid:
                continue
            nickname = str(row[1]).strip() if len(row) >= 2 else ""
            if uid in uid_set:
                removed_nicknames.append(nickname or uid)
                continue
            excluded = str(row[2]).strip() if len(row) >= 3 else "0"
            # ✨ [수정됨] D~G열(마니또/입장일/퇴장일/성별)을 빼먹고 재작성하면
            # 데이터가 전부 사라지므로, 남기는 행은 반드시 같이 들고 간다.
            manito = str(row[3]).strip() if len(row) >= 4 else ""
            join_date = normalize_sheet_date(str(row[4]).strip() if len(row) >= 5 else "")
            leave_date = normalize_sheet_date(str(row[5]).strip() if len(row) >= 6 else "")
            gender = str(row[6]).strip() if len(row) >= 7 else ""
            kept_rows.append([uid, nickname, excluded, manito, join_date, leave_date, gender])

        if not removed_nicknames:
            # 시트에 애초에 없던 아이디들 → 삭제할 것 없음
            return (True, [])

        ws.clear()
        ws.update(values=[header] + kept_rows, range_name="A1")

        try:
            supabase.table("line_users").delete().in_("line_user_id", list(uid_set)).execute()
        except Exception as e:
            print(f"⚠️ line_users 삭제 오류: {e}")

        try:
            supabase.table("profile_cache").delete().in_("line_user_id", list(uid_set)).execute()
        except Exception as e:
            print(f"⚠️ profile_cache 삭제 오류: {e}")

        # 남은 행 기준으로 sheet_row 재동기화
        rows_to_upsert = []
        for idx, row in enumerate(kept_rows, start=2):
            uid, nickname, excluded, manito, join_date, leave_date, gender = row
            row_data = {
                "line_user_id": uid,
                "nickname": nickname,
                "is_excluded": 1 if excluded == "1" else 0,
                "manito": manito,
                "sheet_row": idx,
                "updated_at": now_utc().isoformat(),
                "gender": gender,
            }
            if join_date:
                row_data["join_date"] = join_date
            if leave_date:
                row_data["leave_date"] = leave_date
            rows_to_upsert.append(row_data)
        if rows_to_upsert:
            supabase_upsert_line_users(rows_to_upsert)

        return (True, removed_nicknames)
    except Exception as e:
        print(f"⚠️ 라인유저 삭제 중 오류: {e}")
        return (False, [])

# ================================================================
# ✨ [변경됨] 방 퇴장 처리: 즉시 삭제하지 않고 leave_date만 기록
#
# 예전엔 MemberLeftEvent가 delete_line_users_by_ids()로 즉시 삭제했지만,
# 그러면 "/신입통계"에서 최근에 나간 사람 정보(언제 들어와서 언제 나갔는지)를
# 더 이상 알 수 없었다. 이제는 나가도 leave_date만 찍어두고 CHAT_LOG_DB_
# RETENTION_WEEKS(기본 6주) 동안은 line_users에 그대로 남겨둔 뒤,
# purge_expired_left_members()가 그 기간이 지난 사람만 실제로 삭제한다.
#
# ⚠️ 이 유예기간 동안 나간 사람이 line_users에 남아있는 동안에도 멘션 등
# "현재 방에 있는 사람"을 기준으로 하는 조회는 get_line_user_map(active_only=True,
# 기본값)이 leave_date가 찍힌 사람을 자동으로 걸러내므로 문제없다.
# ================================================================
def mark_line_users_left(uid_list):
    """
    주어진 line_user_id 목록의 leave_date를 오늘 날짜(KST)로 기록한다.
    (실제 삭제는 하지 않음 — purge_expired_left_members 참고)

    반환: (성공 여부, leave_date가 기록된 닉네임 리스트)
    """
    uid_list = [str(uid).strip() for uid in (uid_list or []) if str(uid).strip()]
    if not uid_list:
        return (True, [])

    kst = timezone(timedelta(hours=9))
    today_str = datetime.now(kst).strftime("%Y-%m-%d")

    try:
        # 원래 등록돼 있던 사람만 기록 대상으로 삼는다 (등록 이력 없는 아이디는 skip)
        user_map = get_line_user_map(active_only=False)
    except Exception:
        user_map = {}

    rows_to_upsert = []
    marked_nicknames = []
    ws = get_line_user_sheet()
    for uid in uid_list:
        info = user_map.get(uid)
        if not info:
            continue
        rows_to_upsert.append({
            "line_user_id": uid,
            "leave_date": today_str,
        })
        marked_nicknames.append(info.get("nickname") or uid)

        # ✨ [추가됨] 구글시트 F열(퇴장일)에도 같이 기록
        row_index = info.get("row")
        if ws is not None and row_index:
            try:
                ws.update_cell(int(row_index), 6, today_str)
            except Exception as sheet_e:
                print(f"⚠️ 퇴장일 시트 반영 실패({uid}): {sheet_e}")

    if not rows_to_upsert:
        return (True, [])

    ok, msg = supabase_upsert_line_users(rows_to_upsert)
    if not ok:
        print(f"⚠️ leave_date 기록 실패: {msg}")
        return (False, [])

    return (True, marked_nicknames)

# ================================================================
# ✨ [추가됨] 퇴장 유예기간(6주) 경과자 실제 삭제
#
# mark_line_users_left()로 leave_date만 찍어둔 사람들 중, 나간 지
# CHAT_LOG_DB_RETENTION_WEEKS(기본 6주)가 지난 사람을 line_users(DB) +
# 라인유저 구글시트 + profile_cache에서 실제로 삭제한다.
# (매일 자정 크론에서 cleanup_old_chat_logs_db()와 함께 호출된다)
# ================================================================
def purge_expired_left_members(retention_weeks=CHAT_LOG_DB_RETENTION_WEEKS):
    try:
        kst = timezone(timedelta(hours=9))
        cutoff = (datetime.now(kst).date() - timedelta(weeks=retention_weeks)).strftime("%Y-%m-%d")
        res = (
            supabase.table("line_users")
            .select("line_user_id")
            .not_.is_("leave_date", "null")
            .lte("leave_date", cutoff)
            .execute()
        )
        expired_ids = [r["line_user_id"] for r in (res.data or []) if r.get("line_user_id")]
    except Exception as e:
        print(f"⚠️ 만료된 퇴장자 조회 오류: {e}")
        return (False, "조회 실패")

    if not expired_ids:
        return (True, "삭제할 만료된 퇴장자가 없습니다.")

    ok, removed_nicknames = delete_line_users_by_ids(expired_ids)
    if not ok:
        return (False, "삭제 처리 중 오류가 발생했습니다.")

    return (True, f"🗑️ 퇴장 {retention_weeks}주 경과자 {len(removed_nicknames)}명 정리 완료 ({', '.join(removed_nicknames)})")

# ================================================================
# ✨ [추가됨] "/아이디정리" : 최근 N일간 대화 없는 사용자 일괄 삭제
# ================================================================
def cleanup_inactive_line_users(inactive_days=14):
    """
    최근 inactive_days일(기본 2주) 동안 chat_logs에 메시지 기록이 없는
    사용자를 라인유저 시트+DB에서 삭제한다.

    - 운영진(부방장/관리자/인증자/홍보대장)과 방장은 대화가 뜸해도
      멘션/권한 관련 기능이 깨지지 않도록 삭제 대상에서 제외한다.
    - chat_logs는 CHAT_LOG_DB_RETENTION_WEEKS(6주)까지는 DB에 남아있으므로
      inactive_days(14일=2주) 조회에는 문제가 없다.
    """
    user_map = get_line_user_map()
    if not user_map:
        return (True, "등록된 사용자가 없습니다.")

    kst = timezone(timedelta(hours=9))
    today = datetime.now(kst).date()
    recent_dates = [
        (today - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(inactive_days)
    ]
    active_ids = supabase_get_chatlog_user_ids_for_dates(recent_dates)

    try:
        admins = get_admin_members()
        protected_ids = set(
            admins.get("trophy", []) + admins.get("pin", []) +
            admins.get("book", []) + admins.get("promoter", [])
        )
    except Exception:
        protected_ids = set()
    protected_ids.add("U6362873054f9e1c52c48e7d725ad56ec")  # 방장

    stale_ids = [
        uid for uid in user_map
        if uid not in active_ids and uid not in protected_ids
    ]

    if not stale_ids:
        return (True, f"최근 {inactive_days}일간 대화 없는 사용자가 없습니다.")

    ok, removed_nicknames = delete_line_users_by_ids(stale_ids)
    if not ok:
        return (False, "삭제 처리 중 오류가 발생했습니다.")

    return (
        True,
        f"🗑️ 최근 {inactive_days}일간 대화 없는 {len(removed_nicknames)}명 삭제 완료\n"
        + (", ".join(removed_nicknames) if removed_nicknames else "")
    )
        
def sync_bung_to_db():
    _, _, bung_ws, _ = get_eden_sheets()
    if not bung_ws:
        return (False, "벙지 시트가 없습니다.")
    try:
        records = bung_ws.get_all_values()
        rows = []
        for i, row in enumerate(records):
            if i == 0:
                continue
            row.extend([""] * 6)
            author = row[0].strip()
            date_val = row[1].strip()
            members = row[2].strip()
            location = row[3].strip()
            title = row[4].strip()
            status = row[5].strip()
            
            if not title:
                continue
                
            rows.append({
                "작성자": author, 
                "날짜": date_val, 
                "멤버": members, 
                "장소": location, 
                "벙제": title, 
                "진행상황": status 
            })
            
        removed_count = 0
        if rows:
            # ✨ [변경됨] 전체 삭제 후 재업로드 대신, "벙제"를 키로 업서트하고
            # 시트에 더 이상 없는 "벙제"만 골라서 삭제한다.
            # ⚠️ 주의: on_conflict="벙제"가 동작하려면 Supabase의 meet_log
            # 테이블에 "벙제" 컬럼 UNIQUE 제약(또는 unique index)이 있어야 한다.
            # 예) ALTER TABLE meet_log ADD CONSTRAINT meet_log_벙제_key UNIQUE (벙제);
            supabase.table("meet_log").upsert(rows, on_conflict="벙제").execute()

            try:
                sheet_titles = {r["벙제"] for r in rows}
                existing_res = supabase.table("meet_log").select("벙제").execute()
                existing_titles = {str(r.get("벙제") or "").strip() for r in (existing_res.data or [])}
                stale_titles = [t for t in existing_titles if t and t not in sheet_titles]
                if stale_titles:
                    supabase.table("meet_log").delete().in_("벙제", stale_titles).execute()
                    removed_count = len(stale_titles)
            except Exception as del_e:
                print(f"⚠️ meet_log 잔여분 삭제 오류: {del_e}")

        return (True, f"벙지 {len(rows)}건 동기화 (업서트, 삭제 {removed_count}건)")
    except Exception as e:
        return (False, f"벙지 동기화 오류: {e}")

def refresh_nicknames_from_line(group_id=None, time_budget=25):
    """
    디비갱신용: 시트에 등록된(퇴장 기록 없는) 유저의 닉네임을 LINE에서 새로 받아
    시트 B열(닉네임)과 G열(성별, 이모지로 판별될 때만)에 반영하고 profile_cache도 갱신한다.
    - group_id가 있으면 그룹 멤버 API를 먼저, 실패하면 일반 프로필 API로 폴백한다.
    - LINE에서 못 가져온 사람은 기존 값을 그대로 둔다 (빈 값으로 덮어쓰지 않음).
    - 응답 시간이 길어지지 않도록 time_budget(초) 안에 끝낸 만큼만 반영한다.
    반환값: 결과 요약 문자열
    """
    if not LINE_ACCESS_TOKEN:
        return "[닉네임] LINE 토큰이 없어 건너뜀"
    ws = get_line_user_sheet()
    if ws is None:
        return "[닉네임] 라인유저아이디 시트가 없어 건너뜀"

    from concurrent.futures import ThreadPoolExecutor, as_completed
    from concurrent.futures import TimeoutError as FutTimeout

    try:
        records = ws.get_all_values()
        targets = []  # (row_index, uid, old_nickname, old_gender)
        for row_index, row in enumerate(records, start=1):
            if not row:
                continue
            uid = str(row[0]).strip()
            if not uid or uid == "LINE User ID":
                continue
            leave_date = str(row[5]).strip() if len(row) >= 6 else ""
            if leave_date:  # 나간 사람은 LINE에서 조회되지 않으므로 제외
                continue
            old_nick = str(row[1]).strip() if len(row) >= 2 else ""
            old_gender = str(row[6]).strip() if len(row) >= 7 else ""
            targets.append((row_index, uid, old_nick, old_gender))

        if not targets:
            return "[닉네임] 대상 유저 없음"

        headers = {"Authorization": f"Bearer {LINE_ACCESS_TOKEN}"}

        # ✨ [변경됨] 1:1 대화창에서 /디비갱신을 실행하면 group_id가 없어서 일반 프로필 API만
        # 쓰게 되고, 봇을 친구로 추가하지 않은 사람은 조회에 실패해 시트 닉이 안 바뀌었다.
        # group_id가 없으면 봇이 들어가 있는 알려진 그룹방들로 대신 조회한다.
        # (그 방에 없는 사람은 404가 나올 뿐이라 다음 후보/프로필 API로 자연스럽게 넘어간다)
        lookup_group_ids = [group_id] if group_id else []
        if not group_id:
            for _gid in list(CHAT_LOG_TARGET_GROUP_IDS) + [INVITE_STAT_ALLOWED_GROUP_ID]:
                if _gid and _gid not in lookup_group_ids:
                    lookup_group_ids.append(_gid)

        def _fetch(uid):
            urls = []
            for _gid in lookup_group_ids:
                urls.append(f"https://api.line.me/v2/bot/group/{_gid}/member/{uid}")
            urls.append(f"https://api.line.me/v2/bot/profile/{uid}")
            for url in urls:
                try:
                    r = requests.get(url, headers=headers, timeout=3)
                    if r.status_code == 200:
                        name = str(r.json().get("displayName") or "").strip()
                        if name:
                            return name
                except Exception:
                    pass
            return None

        fetched = {}
        timed_out = False
        ex = ThreadPoolExecutor(max_workers=8)
        futs = {ex.submit(_fetch, uid): (row_index, uid, old_nick, old_gender)
                for (row_index, uid, old_nick, old_gender) in targets}
        try:
            for f in as_completed(futs, timeout=time_budget):
                name = f.result()
                if name:
                    fetched[futs[f][1]] = name
        except FutTimeout:
            timed_out = True
        finally:
            for f in futs:
                f.cancel()
            ex.shutdown(wait=False)

        updates = []
        changed = []
        for (row_index, uid, old_nick, old_gender) in targets:
            new_nick = fetched.get(uid)
            if not new_nick or new_nick == old_nick:
                continue
            updates.append({"range": f"B{row_index}", "values": [[new_nick]]})
            new_gender = derive_gender_from_nickname(new_nick)
            if new_gender and new_gender != old_gender:
                updates.append({"range": f"G{row_index}", "values": [[new_gender]]})
            changed.append((uid, old_nick, new_nick))

        for i in range(0, len(updates), 200):
            ws.batch_update(updates[i:i + 200], value_input_option="RAW")

        # ✨ [변경됨] 닉네임이 바뀐 사람만이 아니라, LINE에서 확인된 모든 사람의
        # profile_cache를 최신 값으로 갱신한다. (시트 값은 이미 최신인데 캐시만
        # 옛 닉으로 남아 /운영진 등에 옛 닉이 나오는 경우까지 바로잡기 위함)
        try:
            cache_expires = (now_utc() + timedelta(seconds=PROFILE_CACHE_TTL)).isoformat()
            cache_rows = [
                {"line_user_id": uid, "nickname": name, "expires_at": cache_expires}
                for uid, name in fetched.items()
            ]
            for i in range(0, len(cache_rows), 200):
                supabase.table("profile_cache").upsert(
                    cache_rows[i:i + 200], on_conflict="line_user_id"
                ).execute()
        except Exception as e:
            print(f"⚠️ profile_cache 일괄 갱신 오류: {e}")

        failed = len(targets) - len(fetched)
        msg = f"[닉네임] LINE 확인 {len(fetched)}명 / 아이디 시트 닉 변경 {len(changed)}명 / 조회 실패 {failed}명"
        if timed_out:
            msg += " (시간 초과로 일부만 확인됨, 한 번 더 실행해 주세요)"
        if changed:
            sample = ", ".join(f"{o or '?'}→{n}" for _u, o, n in changed[:10])
            msg += f"\n  변경: {sample}" + (" 외" if len(changed) > 10 else "")
        return msg
    except Exception as e:
        return f"[닉네임] LINE 닉네임 갱신 오류: {e}"

def sync_all_sheets_to_db(group_id=None):
    results = []
    # ✨ [추가됨] 시트→DB 동기화 전에 LINE에서 최신 닉네임을 먼저 받아 시트에 반영
    results.append(refresh_nicknames_from_line(group_id))
    _, msg1 = sync_line_users_sheet_to_db()
    results.append(msg1)
    _, msg2 = sync_individual_greetings_to_db()
    results.append(msg2)
    _, msg3 = sync_notice_to_db()
    results.append(msg3)
    _, msg4 = sync_bung_to_db()
    results.append(msg4)
    _, msg5 = sync_image_files_to_db()
    results.append(msg5)
    _, msg6 = sync_admins_to_db_from_sheet()
    results.append(f"[운영진] {msg6}")

    return "\n".join(results)

def get_line_user_map(active_only=True):
    """
    ✨ [변경됨] active_only=True(기본값)면 leave_date가 찍혀있는(=방을 나간)
    사용자는 결과에서 빼고 반환한다. 퇴장한 사람은 purge_expired_left_members()가
    처리(6주 경과 시 실삭제)하기 전까지도 line_users 테이블에는 남아있지만,
    멘션/조회 등 "지금 방에 있는 사람" 기준으로 동작해야 하는 함수들이
    실수로 나간 사람까지 포함시키지 않도록 하기 위함이다.

    나간 사람까지 포함해서(재등록/재입장 판별 등에) 봐야 하는 경우에만
    active_only=False로 호출한다.
    """
    user_map = {}
    # 1. Supabase에서 읽어오기
    try:
        data = supabase_get_all_line_users()
        for r in data:
            uid = r.get("line_user_id")
            if not uid:
                continue
            leave_date = r.get("leave_date")
            if active_only and leave_date:
                continue
            nickname = r.get("nickname") or ""
            sheet_row = r.get("sheet_row") or 0
            is_excluded = 1 if str(r.get("is_excluded")) in ["1", "True", "true"] else 0
            user_map[uid] = {
                "row": sheet_row,
                "nickname": nickname,
                "is_excluded": is_excluded,
                "join_date": r.get("join_date"),
                "leave_date": leave_date,
                "gender": r.get("gender") or "",
            }
        if user_map:
            return user_map
    except Exception as e:
        print(f"⚠️ Supabase line_users 읽기 오류: {e}")

    # 2. Supabase 오류 시 구글 시트에서 fallback
    try:
        ws = get_line_user_sheet()
        if ws:
            records = ws.get_all_values()
            for row_index, row in enumerate(records, start=1):
                if not row:
                    continue
                user_id = str(row[0]).strip()
                if not user_id or user_id == "LINE User ID":
                    continue
                nickname = str(row[1]).strip() if len(row) >= 2 else ""
                is_excluded = 1 if len(row) >= 3 and str(row[2]).strip() == "1" else 0
                join_date = normalize_sheet_date(str(row[4]).strip() if len(row) >= 5 else "")
                leave_date = normalize_sheet_date(str(row[5]).strip() if len(row) >= 6 else "")
                gender = str(row[6]).strip() if len(row) >= 7 else ""
                if active_only and leave_date:
                    continue
                user_map[user_id] = {
                    "row": row_index,
                    "nickname": nickname,
                    "is_excluded": is_excluded,
                    "join_date": join_date or None,
                    "leave_date": leave_date or None,
                    "gender": gender,
                }
    except Exception as e:
        print(f"⚠️ 구글 시트 사용자 맵 로드 실패: {e}")

    return user_map

def sync_image_files_to_db():
    try:
        _, _, _, image_ws = get_eden_sheets()

        if not image_ws:
            return (False, "이미지파일 시트가 없습니다.")

        records = image_ws.get_all_values()
        rows = []

        for i, row in enumerate(records):
            if i == 0:
                continue

            row = row + [""] * (2 - len(row))

            keyword = str(row[0]).strip()
            filename = str(row[1]).strip()

            if not keyword or not filename:
                continue

            rows.append({
                "키워드": keyword,
                "파일명": filename
            })

        # ✨ [변경됨] 전체 삭제 후 재업로드 대신, "키워드"를 키로 업서트하고
        # 시트에 더 이상 없는 "키워드"만 골라서 삭제한다.
        # ⚠️ 주의: on_conflict="키워드"가 동작하려면 Supabase의 이미지파일
        # 테이블에 "키워드" 컬럼 UNIQUE 제약(또는 unique index)이 있어야 한다.
        # 예) ALTER TABLE "이미지파일" ADD CONSTRAINT 이미지파일_키워드_key UNIQUE ("키워드");
        removed_count = 0
        if rows:
            supabase.table("이미지파일").upsert(rows, on_conflict="키워드").execute()

            try:
                sheet_keywords = {r["키워드"] for r in rows}
                existing_res = supabase.table("이미지파일").select("키워드").execute()
                existing_keywords = {str(r.get("키워드") or "").strip() for r in (existing_res.data or [])}
                stale_keywords = [k for k in existing_keywords if k and k not in sheet_keywords]
                if stale_keywords:
                    supabase.table("이미지파일").delete().in_("키워드", stale_keywords).execute()
                    removed_count = len(stale_keywords)
            except Exception as del_e:
                print(f"⚠️ 이미지파일 잔여분 삭제 오류: {del_e}")

        return (
            True,
            f"이미지파일 {len(rows)}건 동기화 완료 (삭제 {removed_count}건)"
        )

    except Exception as e:
        print(f"⚠️ 이미지파일 동기화 오류: {e}")
        return (False, f"이미지파일 동기화 오류: {e}")
        
def save_user_to_line_sheet(user_id, nickname):
    if not user_id or not nickname:
        return False
    nickname = str(nickname).strip()
    if not nickname:
        return False

    ws = get_line_user_sheet()
    if ws is None:
        return False

    kst = timezone(timedelta(hours=9))
    today_str = datetime.now(kst).strftime("%Y-%m-%d")

    # ✨ [변경됨] active_only=False로 조회 — 방을 나갔다가(leave_date 기록됨)
    # 다시 들어온 사람도 "기존 사용자"로 인식해서 중복 행을 만들지 않고
    # leave_date를 지우고 join_date를 오늘로 갱신(재입장 처리)한다.
    user_map = get_line_user_map(active_only=False)
    if user_id in user_map:
        try:
            row_index = int(user_map[user_id]["row"])
            old_nickname = str(user_map[user_id].get("nickname", "")).strip()
            is_excluded = user_map[user_id].get("is_excluded", 0)
            was_left = bool(user_map[user_id].get("leave_date"))
            # ✨ [추가됨] 닉네임의 이모지로 성별 판별 (🪷,⚖️→여 / 🐛,🪻,🏹,🧨→남)
            new_gender = derive_gender_from_nickname(nickname)

            if old_nickname == nickname and not was_left:
                supabase_set_profile_cache(user_id, nickname)
                return True

            if old_nickname != nickname:
                ws.update_cell(row_index, 2, nickname)
                if new_gender:  # 판별된 경우에만 갱신 (이모지 없는 닉네임이 기존 성별을 지우지 않게)
                    ws.update_cell(row_index, 7, new_gender)
                user_map[user_id]["nickname"] = nickname

            upsert_row = {
                "line_user_id": user_id,
                "nickname": nickname,
                "is_excluded": is_excluded,
                "sheet_row": row_index,
                "updated_at": now_utc().isoformat(),
            }
            if new_gender:
                upsert_row["gender"] = new_gender
            if was_left:
                # 재입장: 퇴장 기록을 지우고 입장일을 오늘로 새로 기록
                # (시트 E~G열에도 그대로 반영 — 배치 업데이트로 한 번에)
                upsert_row["join_date"] = today_str
                upsert_row["leave_date"] = None
                try:
                    if new_gender:
                        ws.update(values=[[today_str, "", new_gender]], range_name=f"E{row_index}:G{row_index}")
                    else:
                        ws.update(values=[[today_str, ""]], range_name=f"E{row_index}:F{row_index}")
                except Exception as sheet_e:
                    print(f"⚠️ 재입장 시트 반영 실패({user_id}): {sheet_e}")

            supabase_upsert_line_users([upsert_row])
            supabase_set_profile_cache(user_id, nickname)
            return True
        except Exception as e:
            print(f"⚠️ 기존 사용자 업데이트 오류: {e}")
            return False

    # 신규 등록
    try:
        # ✨ [추가됨] 닉네임의 이모지로 성별 판별 (🪷,⚖️→여 / 🐛,🪻,🏹,🧨→남)
        new_gender = derive_gender_from_nickname(nickname)
        # 제외 기본값 "0", 입장일은 오늘 날짜로 행 추가
        ws.append_row([user_id, nickname, "0", "", today_str, "", new_gender], value_input_option="USER_ENTERED")
        # ✨ [변경됨] 신규 유저 한 명 등록될 때마다 시트 전체(수백~수천 행)를
        # 동기적으로 Supabase에 재업로드하면 응답이 느려지고 504 Gateway Timeout이
        # 나기도 해서, 백그라운드 스레드로 돌려 LINE 응답을 막지 않게 한다.
        # (upsert 자체도 supabase_upsert_line_users에서 배치로 쪼개 전송됨)
        threading.Thread(
            target=load_line_users_to_db_from_sheet,
            kwargs={"force": True},
            daemon=True
        ).start()
        # ✨ [추가됨] 신입통계(join_date 기반 조회)가 곧바로 이 사람을 찾을 수
        # 있도록, 백그라운드 재동기화(닉네임/제외/행번호만 갱신)와는 별개로
        # join_date/gender는 여기서 바로 기록해둔다. (join_date/leave_date/gender
        # 키가 없는 이후의 upsert들은 이 값을 건드리지 않으므로 안전하게 유지된다)
        supabase_upsert_line_users([{
            "line_user_id": user_id,
            "join_date": today_str,
            "leave_date": None,
            "gender": new_gender,
        }])
        supabase_set_profile_cache(user_id, nickname)
        return True
    except Exception as e:
        print(f"⚠️ LINE 사용자 신규 등록 오류: {e}")
        return False

# ================================================================
# 날짜 파싱
# ================================================================
def parse_date_string(raw_date):
    if not raw_date:
        return datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d")

    kst = timezone(timedelta(hours=9))
    now = datetime.now(kst)
    text = raw_date.strip().replace(" ", "")

    if text == "오늘":
        return now.strftime("%Y-%m-%d")
    elif text == "내일":
        return (now + timedelta(days=1)).strftime("%Y-%m-%d")
    elif text in ["모레", "내일모레"]:
        return (now + timedelta(days=2)).strftime("%Y-%m-%d")
    elif text == "글피":
        return (now + timedelta(days=3)).strftime("%Y-%m-%d")

    m1 = re.match(r'^(\d{4})[-./년]?(\d{1,2})[-./월]?(\d{1,2})일?$', text)
    if m1:
        try:
            dt = datetime(int(m1.group(1)), int(m1.group(2)), int(m1.group(3)))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    m2 = re.match(r'^(\d{1,2})[-./월](\d{1,2})일?$', text)
    if m2:
        try:
            dt = datetime(now.year, int(m2.group(1)), int(m2.group(2)))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return raw_date.strip()

# ================================================================
# load_line_users_to_db_from_sheet
# ================================================================
def load_line_users_to_db_from_sheet(force=False):
    try:
        ws = get_line_user_sheet()
        if ws is None:
            return (False, "시트 없음")

        records = ws.get_all_values()
        rows_to_upsert = []
        excluded_in_sheet = 0

        for row_index, row in enumerate(records, start=1):
            if not row:
                continue
            uid = str(row[0]).strip()
            if not uid or uid == "LINE User ID":
                continue
            nickname = str(row[1]).strip() if len(row) >= 2 else ""
            is_excluded = 1 if len(row) >= 3 and str(row[2]).strip() == "1" else 0
            if is_excluded:
                excluded_in_sheet += 1

            # ✨ [추가됨] D열: 마니또 (콤마로 구분된 2글자 닉네임들, 원문 그대로 저장)
            manito_raw = str(row[3]).strip() if len(row) >= 4 else ""

            row_data = {
                "line_user_id": uid,
                "nickname": nickname,
                "is_excluded": is_excluded,
                "manito": manito_raw,
                "sheet_row": row_index,
                "updated_at": now_utc().isoformat()
            }
            # ✨ [추가됨] E/F열: 입장일/퇴장일 — 시트에 값이 있을 때만 반영
            join_date_raw = normalize_sheet_date(str(row[4]).strip() if len(row) >= 5 else "")
            leave_date_raw = normalize_sheet_date(str(row[5]).strip() if len(row) >= 6 else "")
            if join_date_raw:
                row_data["join_date"] = join_date_raw
            if leave_date_raw:
                row_data["leave_date"] = leave_date_raw

            # ✨ [추가됨] G열: 성별 — 시트 값 기준으로 DB에 반영 ("아이디 갱신"시에도 동일)
            gender_raw = str(row[6]).strip() if len(row) >= 7 else ""
            row_data["gender"] = gender_raw

            rows_to_upsert.append(row_data)

        if rows_to_upsert:
            ok, msg = supabase_upsert_line_users(rows_to_upsert)
            if not ok:
                print(f"⚠️ line_users 동기화 실패: {msg}")
                return (False, msg)

        # ✨ [변경됨] 전체 삭제 후 재업로드 대신, 시트에 더 이상 없는
        # line_user_id만 골라서 line_users에서 삭제 (업서트 + 잔여분만 삭제)
        removed_count = 0
        try:
            sheet_ids = {r["line_user_id"] for r in rows_to_upsert}
            existing_res = supabase.table("line_users").select("line_user_id").execute()
            existing_ids = {str(r.get("line_user_id") or "").strip() for r in (existing_res.data or [])}
            stale_ids = [uid for uid in existing_ids if uid and uid not in sheet_ids]
            if stale_ids:
                supabase.table("line_users").delete().in_("line_user_id", stale_ids).execute()
                removed_count = len(stale_ids)
        except Exception as del_e:
            print(f"⚠️ line_users 잔여분 삭제 오류: {del_e}")

        print(f"✅ line_users 테이블 갱신: {len(rows_to_upsert)}명 (제외대상 {excluded_in_sheet}명, 삭제 {removed_count}명)")
        return (True, f"{len(rows_to_upsert)}명 갱신 (제외대상 {excluded_in_sheet}명, 삭제 {removed_count}명)")
    except Exception as e:
        print(f"⚠️ line_users 동기화 실패: {e}")
        return (False, str(e))
        
# ================================================================
# refresh_unregistered_users
# ================================================================
def refresh_unregistered_users(group_id):
    ws = get_line_user_sheet()
    if ws is None or not group_id:
        return "⚠️ 시트가 연결되지 않았거나 그룹방 ID가 없습니다."


    try:
        # ✨ [추가됨] 등록된 유저아이디를 LINE에서 조회해 현재 닉네임으로 시트(B/G열)를 먼저 갱신한다.
        # 아래 시트 읽기·DB 동기화가 이 갱신된 값을 기준으로 진행되므로 시트와 DB가 함께 반영된다.
        nick_msg = refresh_nicknames_from_line(group_id)

        kst = timezone(timedelta(hours=9))
        now = datetime.now(kst)
        today_str = now.strftime("%Y-%m-%d")
        yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        target_dates = [yesterday_str, today_str]

        active_user_ids = supabase_get_chatlog_user_ids_for_dates(target_dates)

        records = ws.get_all_values()
        sheet_users = {}
        for row in records:
            if not row:
                continue
            uid = str(row[0]).strip()
            if not uid or uid == "LINE User ID":
                continue
            nickname = str(row[1]).strip() if len(row) >= 2 else ""
            sheet_users[uid] = nickname

        new_user_count = 0
        nickname_success_count = 0

        for uid in sorted(active_user_ids):
            if uid in sheet_users:
                continue

            real_nickname = None
            try:
                url = f"https://api.line.me/v2/bot/group/{group_id}/member/{uid}"
                response = requests.get(url, headers={"Authorization": f"Bearer {LINE_ACCESS_TOKEN}"}, timeout=3)
                if response.status_code == 200:
                    real_nickname = response.json().get("displayName")
            except Exception:
                pass

            if not real_nickname:
                try:
                    url = f"https://api.line.me/v2/bot/profile/{uid}"
                    response = requests.get(url, headers={"Authorization": f"Bearer {LINE_ACCESS_TOKEN}"}, timeout=3)
                    if response.status_code == 200:
                        real_nickname = response.json().get("displayName")
                except Exception:
                    pass

            nickname = real_nickname.strip() if real_nickname and real_nickname.strip() else "아이디 확인"
            if real_nickname:
                nickname_success_count += 1

            sheet_users[uid] = nickname
            new_user_count += 1

        if new_user_count > 0:
            existing_ids = {str(row[0]).strip() for row in records if row and str(row[0]).strip() != "LINE User ID"}
            # 제외 기본값 "0" 지정
            # ✨ [추가됨] 신규 발견된 유저도 닉네임의 이모지로 성별을 판별해 G열에 기록
            new_rows = [
                [uid, sheet_users[uid], "0", "", "", "", derive_gender_from_nickname(sheet_users[uid])]
                for uid in sorted(sheet_users.keys()) if uid not in existing_ids
            ]
            if new_rows:
                ws.append_rows(new_rows, value_input_option="USER_ENTERED")


        db_ok, db_msg = load_line_users_to_db_from_sheet(force=True)   # ✅ 이제 튜플로 받음

        final_records = ws.get_all_values()
        final_user_map = {}
        for row_index, row in enumerate(final_records, start=1):
            if not row:
                continue
            uid = str(row[0]).strip()
            if not uid or uid == "LINE User ID":
                continue
            nickname = str(row[1]).strip() if len(row) >= 2 else ""
            is_excluded = 1 if len(row) >= 3 and str(row[2]).strip() == "1" else 0
            final_user_map[uid] = {"row": row_index, "nickname": nickname, "is_excluded": is_excluded}

        # ✨ [변경됨] 전체 삭제 후 재업로드 대신, 현재 시트(final_user_map)에
        # 없는 line_user_id의 캐시만 골라서 삭제 (업서트 + 잔여분만 삭제)
        supabase_delete_profile_cache_stale(final_user_map.keys())

        profile_cache_success = 0
        for uid, user_info in final_user_map.items():
            nickname = str(user_info.get("nickname", "")).strip()
            try:
                if nickname:
                    supabase_set_profile_cache(uid, nickname, ttl_seconds=PROFILE_CACHE_TTL)
                profile_cache_success += 1
            except Exception as e:
                print(f"⚠️ 프로필 캐시 갱신 실패 {uid}: {e}")

        total_count = len(final_user_map)

        # ✨ [추가됨] 아이디/닉네임이 바뀌면 관리자 시트의 닉네임 매칭 결과도 달라질 수 있으므로
        # 운영진(admin_users)도 같이 동기화한다 (추가/삭제 모두 반영)
        admin_ok, admin_msg = sync_admins_to_db_from_sheet()
        admin_line = f"\n운영진: {admin_msg}" if admin_ok else f"\n⚠️ 운영진 동기화 실패: {admin_msg}"

        if db_ok:
            return f"✅ 아이디 갱신 완료\n{nick_msg}\n전체 {total_count}명 중 {profile_cache_success}명 닉네임 캐시 갱신 성공\nDB: {db_msg}{admin_line}"
        else:
            return f"🚨 DB 반영 실패!\n{db_msg}\n{nick_msg}\n(닉네임 캐시만 갱신됨, 제외값 등은 반영 안 됐을 수 있음){admin_line}"
    except Exception as e:
        return f"🚨 아이디 갱신 중 오류 발생: {e}"
# ================================================================
# 관리자 목록
# ================================================================
_LINE_USER_ID_RE = re.compile(r"^U[0-9a-fA-F]{32}$")

def resolve_admin_cell_to_user_id(cell_value):
    """
    ✨ [변경됨] 관리자 시트에 이제 LINE ID 대신 닉네임(보통 한글 2글자)을 넣어두면,
    line_users 테이블에서 매칭되는 실제 line_user_id를 찾아 반환한다.
    - 예전처럼 U로 시작하는 LINE ID를 그대로 넣어놔도 하위 호환되게, ID 형식이면
      닉네임 조회 없이 그대로 사용한다.
    - 완전히 같은 닉네임이 없어도, 입력한 글자가 실제 닉네임에 포함되기만 하면
      매칭한다 (find_user_id_by_nickname_partial 참고).
    - 매칭되는 사람이 없거나, 여러 명이라 모호하면 None을 반환하고 로그를 남긴다
      (해당 칸은 멘션 목록에서 빠짐 — 잘못된 사람 멘션보다 안전).
    """
    value = str(cell_value or "").strip()
    if not value:
        return None
    if _LINE_USER_ID_RE.match(value):
        return value
    user_id, ambiguous_candidates = find_user_id_by_nickname_partial(value)
    if not user_id:
        if ambiguous_candidates:
            print(f"⚠️ 관리자 시트: '{value}'가 여러 명과 매칭돼 특정할 수 없습니다 → {ambiguous_candidates}")
        else:
            print(f"⚠️ 관리자 시트: '{value}' 닉네임에 매칭되는 유저를 찾지 못했습니다.")
    return user_id

# admin_users.role 값 -> get_admin_members()가 반환하는 카테고리 키
# (sync_admins_to_db_from_sheet가 시트 헤더를 그대로 role에 저장하므로 헤더 문구와 일치해야 함)
_ADMIN_ROLE_TO_KEY = {
    "부방장": "trophy",
    "관리자": "pin",
    "인증자": "book",
    "홍보대장": "promoter",
}

def get_admin_members():
    # ✨ [변경됨] 매번 구글시트를 읽던 것을, admin_users(Supabase DB)에서 먼저 읽도록 변경 (속도 개선).
    # DB는 운영진갱신/디비갱신/아이디갱신 명령에서 시트와 동기화(추가+삭제)되므로 최신 상태 유지됨.
    # DB 조회가 비거나 실패하면 기존처럼 구글시트를 직접 읽어 폴백한다.
    if supabase:
        try:
            res = supabase.table("admin_users").select("line_user_id, role").execute()
            rows = res.data or []
            if rows:
                trophy, pin, book, promoter = [], [], [], []
                role_map = {"trophy": trophy, "pin": pin, "book": book, "promoter": promoter}
                for r in rows:
                    uid = r.get("line_user_id")
                    key = _ADMIN_ROLE_TO_KEY.get(str(r.get("role", "")).strip())
                    if uid and key:
                        role_map[key].append(uid)
                return {"trophy": trophy, "pin": pin, "book": book, "promoter": promoter}
        except Exception as e:
            print(f"⚠️ Supabase admin_users 조회 오류: {e}")

    trophy, pin, book, promoter = [], [], [], []
    admin_ws = get_admin_sheet()
    if admin_ws:
        try:
            records = admin_ws.get_all_values()
            role_lists = [trophy, pin, book, promoter]
            for row in records[1:]:
                for col_idx, target_list in enumerate(role_lists):
                    if col_idx < len(row):
                        resolved = resolve_admin_cell_to_user_id(row[col_idx])
                        if resolved:
                            target_list.append(resolved)
            return {"trophy": trophy, "pin": pin, "book": book, "promoter": promoter}
        except Exception as e:
            print(f"⚠️ 관리자 시트 읽기 오류: {e}")
    return {"trophy": [], "pin": [], "book": [], "promoter": []}

# ================================================================
# 멀티 멘션, 벙지 목록
# ================================================================
LINE_MAX_MENTIONS_PER_MESSAGE = 20  # ⚠️ LINE 메시징 API의 textV2 멘션 최대 개수 (초과 시 API가 메시지 전체를 거부함)

def _is_late_night_kst():
    """✨ [추가됨] 한국시간 00:00~07:59 (심야시간) 여부"""
    return 0 <= datetime.now(timezone(timedelta(hours=9))).hour < 8

def build_multi_mention_payload_v2(user_ids, suffix_msg="", group_id=None, night_silent=True):
    if not user_ids:
        return {
            "type": "text",
            "text": "지정된 대상자가 없습니다." + ("\n" + suffix_msg if suffix_msg else "")
        }

    # ✨ [추가됨] 심야시간(00~07시)에는 멘션(알림)이 울리지 않도록
    # 멘션 대신 닉네임만 일반 텍스트로 나열한다. (벙시작/벙종/벙취소/벙지*/관리자/
    # 인증자/부방장 등 이 함수를 쓰는 모든 멘션에 공통 적용.
    # 특정 호출만 심야에도 멘션하고 싶으면 night_silent=False 로 호출)
    if night_silent and _is_late_night_kst():
        try:
            nick_map = get_line_user_map(active_only=False)
        except Exception as e:
            print(f"⚠️ 심야 닉네임 조회용 유저맵 읽기 실패: {e}")
            nick_map = {}
        nicknames = []
        for uid in user_ids:
            nick = str(nick_map.get(uid, {}).get("nickname") or "").strip()
            if not nick or nick == "아이디 확인" or nick.startswith("아이디 확인("):
                nick = get_line_user_nickname(uid, group_id)
            nicknames.append(nick)
        text = ", ".join(nicknames)
        if suffix_msg:
            text += "\n" + suffix_msg
        text += "\n(심야시간이라 멘션 없이 닉네임만 표시합니다)"
        return {"type": "text", "text": text}

    # ✨ [추가됨] LINE은 textV2 메시지 하나에 멘션을 최대 20개까지만 허용한다.
    # 이걸 넘기면 LINE API가 400 에러로 메시지 전체를 거부하는데, reply_line()이
    # 예외를 조용히 삼키다보니 사용자 입장에서는 "완전 무응답"으로 보였다.
    # (예: /인증, /남초, /여초, /동반 처럼 부방장+관리자+인증자를 합쳐서
    #  한 번에 멘션하는 명령어가 인원이 늘면서 이 한도를 넘겨버림)
    # 우선은 20명까지만 멘션하고 나머지는 인원수로만 안내해서, 최소한 무응답은 막는다.
    original_count = len(user_ids)
    truncated = original_count > LINE_MAX_MENTIONS_PER_MESSAGE
    if truncated:
        user_ids = user_ids[:LINE_MAX_MENTIONS_PER_MESSAGE]

    substitution = {}
    placeholders = []
    idx = 1
    for uid in user_ids:
        key = f"user{idx}"
        placeholders.append(f"{{{key}}}")
        substitution[key] = {
            "type": "mention",
            "mentionee": {
                "type": "user",
                "userId": uid
            }
        }
        idx += 1

    text = " ".join(placeholders)
    if suffix_msg:
        text += "\n" + suffix_msg
    if truncated:
        text += f"\n(외 {original_count - LINE_MAX_MENTIONS_PER_MESSAGE}명은 인원 제한으로 멘션에서 생략됨)"

    return {
        "type": "textV2",
        "text": text,
        "substitution": substitution
    }

def get_active_bungs(nickname):
    _, _, ws , _ = get_eden_sheets()
    if not ws:
        return []

    try:
        records = ws.get_all_values()
    except Exception as e:
        print(f"벙지 시트 읽기 에러: {e}")
        return []

    active = []
    my_hangul = extract_hangul(nickname)

    for i, row in enumerate(records):
        if i == 0:
            continue

        row.extend([""] * (6 - len(row)))
        author, date_val, members, location, title, status = [col.strip() for col in row[:6]]

        if status not in ["종료", "취소"]:
            is_author = (author == nickname)
            is_member = False
            # 2~3글자 닉네임 모두 대응: 멤버를 구분자로 나눈 뒤 한글만 뽑아 완전 일치 비교
            member_hanguls = [
                extract_hangul(m)
                for m in re.split(r"[\s,.]+", members) if m
            ]
            if my_hangul and my_hangul in member_hanguls:
                is_member = True

            if is_author or is_member:
                active.append({
                    "row": i + 1,
                    "date": date_val,
                    "members": members,
                    "location": location,
                    "title": title,
                    "status": status,
                    "author": author
                })

    return active

# ================================================================
# ✨ [추가됨] '검사지' 스프레드시트 (여자 탭) 연결
#
# 벙지가 '진행중'으로 시작될 때, 벙멤버가 정확히 2명이면
# 그 중 한 명이 이 시트(여자 탭, A열: 닉네임)에 있는지 확인하고,
# 있으면 그 사람의 만료예정일(G열, 'YYYY. MM. DD' 형식)이
# 벙 날짜 기준으로 아직 지나지 않았는지 확인하는 데 쓰인다.
# ================================================================
inspection_doc = None
inspection_female_sheet = None


def get_inspection_female_sheet():
    global inspection_doc, inspection_female_sheet
    if inspection_female_sheet is not None:
        return inspection_female_sheet
    try:
        if inspection_doc is None:
            inspection_doc = client.open("검사지")
        inspection_female_sheet = inspection_doc.worksheet("여자")
        return inspection_female_sheet
    except Exception as e:
        print(f"⚠️ '검사지' 시트(여자 탭) 연결 실패: {e}")
        inspection_female_sheet = None
        return None


def parse_inspection_expiry_date(raw_date):
    """
    '검사지' 시트 여자 탭의 만료예정일(G열) 형식('YYYY. MM. DD')을
    date 객체로 변환한다. 형식이 다르면 None을 반환한다.
    """
    if not raw_date:
        return None
    t = str(raw_date).strip()
    m = re.match(r'^(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?$', t)
    if not m:
        return None
    try:
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
    except ValueError:
        return None


def check_and_mark_no_ttuk_female(row_num, members, date_val):
    """
    벙이 '진행중'으로 시작될 때 호출한다.

    조건:
    1) 벙멤버(members)가 정확히 2명일 것
    2) 그 중 한 명이 '검사지' 여자 탭(A열: 닉네임)에 존재할 것
    3) 그 사람의 만료예정일(G열)이 벙 날짜 기준으로 아직 지나지 않았을 것
       (만료예정일 >= 벙 날짜)

    조건을 모두 만족하면 벙지 시트의 G열(노뚝여성참여여부)에 "1"을 입력한다.
    """
    try:
        member_list = [m for m in re.split(r"[\s,.]+", str(members or "").strip()) if m]
        member_clean_list = [extract_hangul(m) for m in member_list]
        member_clean_list = [m for m in member_clean_list if m]

        # 참가자가 정확히 두 명이 아니면 대상 아님
        if len(member_clean_list) != 2:
            return

        # 벙 날짜 파싱 (범위 표기면 시작일 기준)
        meet_date_raw = str(date_val or "").split("~")[0].strip()
        dm = re.match(r'^(\d{4})-(\d{1,2})-(\d{1,2})$', meet_date_raw)
        if not dm:
            print(f"⚠️ 노뚝여성참여여부 체크: 벙 날짜 형식을 이해하지 못함 ({date_val})")
            return
        meet_date = datetime(int(dm.group(1)), int(dm.group(2)), int(dm.group(3))).date()

        female_ws = get_inspection_female_sheet()
        if not female_ws:
            return

        female_rows = female_ws.get_all_values()

        matched = False
        for i, row in enumerate(female_rows):
            if i == 0:
                continue
            nickname_cell = row[0] if len(row) > 0 else ""
            sheet_nickname = extract_hangul(nickname_cell)
            if not sheet_nickname or sheet_nickname not in member_clean_list:
                continue

            expiry_raw = row[6] if len(row) > 6 else ""
            expiry_date = parse_inspection_expiry_date(expiry_raw)

            if expiry_date and expiry_date >= meet_date:
                matched = True
                break

        if matched:
            _, _, bung_sheet_ws, _ = get_eden_sheets()
            if bung_sheet_ws:
                bung_sheet_ws.update_acell(f"G{row_num}", "1")
                print(f"✅ 노뚝여성참여여부 반영 완료 (row {row_num})")

    except Exception as e:
        print(f"⚠️ 노뚝여성참여여부 체크 오류: {e}")


# ================================================================
# LINE 닉네임 조회
# ================================================================
def get_line_user_nickname(user_id, group_id=None):
    if not user_id:
        return "알수없음"

    # 1. 유효한 캐시가 있으면 그대로 사용
    # (캐시 자체가 예전에 LINE에서 가져와 저장해둔 값이라, 매 메시지마다
    #  LINE API를 부르지 않기 위한 최적화. TTL은 PROFILE_CACHE_TTL 참고)
    try:
        cached_nickname = supabase_get_profile_cache_valid(user_id)
        if cached_nickname:
            return cached_nickname
    except Exception:
        pass

    # ✨ [변경됨] 닉네임은 "LINE에서 가져온 값"을 최우선으로 한다.
    # 구글시트/DB에 저장된 값은 예전에 한 번 캐시됐던 옛날 이름일 수
    # 있으므로, 캐시가 만료됐다면 먼저 LINE에 최신 이름을 물어보고,
    # 그게 실패할 때만 구글시트/DB에 저장된 값으로 폴백한다.
    nickname = None

    if LINE_ACCESS_TOKEN and group_id:
        try:
            url = f"https://api.line.me/v2/bot/group/{group_id}/member/{user_id}"
            response = requests.get(url, headers={"Authorization": f"Bearer {LINE_ACCESS_TOKEN}"}, timeout=3)
            if response.status_code == 200:
                nickname = response.json().get("displayName")
        except Exception:
            pass

    if not nickname and LINE_ACCESS_TOKEN:
        try:
            url = f"https://api.line.me/v2/bot/profile/{user_id}"
            response = requests.get(url, headers={"Authorization": f"Bearer {LINE_ACCESS_TOKEN}"}, timeout=3)
            if response.status_code == 200:
                nickname = response.json().get("displayName")
        except Exception:
            pass

    if nickname:
        nickname = nickname.strip()
        if nickname:
            supabase_set_profile_cache(user_id, nickname)
            save_user_to_line_sheet(user_id, nickname)
            return nickname

    # 2. LINE에서 가져오지 못했을 때만 구글시트/DB에 저장된 값 사용
    user_map = get_line_user_map()
    if user_id in user_map:
        saved_nickname = str(user_map[user_id].get("nickname", "")).strip()
        if saved_nickname and saved_nickname != "아이디 확인" and not saved_nickname.startswith("아이디 확인("):
            supabase_set_profile_cache(user_id, saved_nickname)
            return saved_nickname

    if user_id not in user_map:
        save_user_to_line_sheet(user_id, "아이디 확인")

    return f"아이디 확인({user_id[:8]})"



def log_message_to_db(user_id, message, source_type=None, group_id=None, message_id=None, timestamp=None):
    if source_type != "group" or group_id not in CHAT_LOG_TARGET_GROUP_IDS:
        return

    if not message or not message.strip():
        return

    try:
        kst = timezone(timedelta(hours=9))
        msg_dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).astimezone(kst) if timestamp else datetime.now(kst)

        date_str = msg_dt.strftime("%Y-%m-%d")
        time_str = msg_dt.strftime("%H:%M:%S.%f")[:-3]

        nickname = get_line_user_nickname(user_id, group_id) if user_id else "알수없음"

        log_entry = {
            "log_date": date_str,
            "log_time": time_str,
            "nickname": nickname,
            "message": message,
            "user_id": user_id if user_id else None,
            "created_at": now_utc().isoformat(),
            # LINE이 웹훅을 재전송해도 같은 message_id로 upsert되어
            # chat_logs에 중복 저장되지 않도록 함
            "line_message_id": message_id if message_id else None
        }
        supabase_insert_chat_log(log_entry)
    except Exception as e:
        print(f"🚨 대화 로그 저장 실패: {e}")

# ================================================================
# ✨ [추가됨] 특정 유저가 올린 사진/동영상만 Supabase Storage에 저장
#
# - MEDIA_SAVE_TARGET_USER_IDS 에 LINE user_id를 넣어두면 그 사람이 올린
#   사진/동영상만 저장한다. 목록이 비어있거나 보낸 사람이 목록에 없으면
#   아무것도 저장하지 않는다. (수시로 이 리스트만 고쳐서 배포하면 됨)
# - Supabase Storage의 "user_media" 버킷 안에 "폴더/파일명" 구조로 저장한다.
#   (폴더명은 MEDIA_SAVE_TARGET_USER_IDS에 지정한 영문 별칭을 쓰고, 별칭이 없으면
#    아이디를 그대로 폴더명으로 씀 — Storage가 한글/이모지/공백 키를 허용하지 않음)
# - 파일명은 내용의 MD5 해시값이라서, 같은 파일이 다시 올라와도
#   업로드 전에 같은 이름이 이미 있는지 확인해 중복 저장을 막는다.
# ================================================================

MEDIA_STORAGE_BUCKET = "user_media"

def _sanitize_storage_folder_name(name, fallback=None):
    name = str(name or "").strip()
    # Supabase Storage 키는 영문/숫자/-, _, . 정도만 허용하고
    # 한글·이모지·공백 등은 전부 거부(Invalid key)하므로 안전한 문자만 남긴다
    name = re.sub(r'[^0-9A-Za-z_\-\.]', '', name)
    name = name.strip(" ._")
    if name:
        return name
    fallback = re.sub(r'[^0-9A-Za-z_\-]', '', str(fallback or ""))
    return fallback or "unknown"

def _ensure_media_bucket_exists():
    try:
        buckets = supabase.storage.list_buckets()
        if any(getattr(b, "name", None) == MEDIA_STORAGE_BUCKET or (isinstance(b, dict) and b.get("name") == MEDIA_STORAGE_BUCKET) for b in (buckets or [])):
            return
    except Exception as e:
        print(f"⚠️ Storage 버킷 목록 조회 실패: {e}")
    try:
        supabase.storage.create_bucket(MEDIA_STORAGE_BUCKET, options={"public": False})
        print(f"✅ Storage 버킷 생성됨: {MEDIA_STORAGE_BUCKET}")
    except Exception as e:
        # 이미 존재하는 경우도 여기로 올 수 있어서 에러여도 크게 문제 없음
        print(f"ℹ️ Storage 버킷 생성 스킵(이미 있을 수 있음): {e}")

def _download_line_message_content(message_id):
    if not LINE_ACCESS_TOKEN:
        return None, None
    try:
        url = f"https://api-data.line.me/v2/bot/message/{message_id}/content"
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {LINE_ACCESS_TOKEN}"},
            timeout=20,
        )
        if response.status_code == 200:
            return response.content, response.headers.get("Content-Type", "")
        print(f"⚠️ LINE 미디어 다운로드 실패: status={response.status_code}")
    except Exception as e:
        print(f"⚠️ LINE 미디어 다운로드 오류: {e}")
    return None, None

def _guess_media_ext(content_type, media_kind):
    content_type = (content_type or "").lower()
    mapping = {
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/png": "png",
        "image/gif": "gif",
        "image/webp": "webp",
        "video/mp4": "mp4",
        "video/quicktime": "mov",
    }
    if content_type in mapping:
        return mapping[content_type]
    return "jpg" if media_kind == "image" else "mp4"

def save_target_user_media(event, media_kind):
    """
    MEDIA_SAVE_TARGET_USER_IDS에 있는 유저가 올린 사진/동영상만
    Supabase Storage(user_media 버킷)에 아이디(또는 지정된 영문 별칭) 폴더로 저장한다.
    같은 내용의 파일은 다시 저장하지 않는다.
    """
    try:
        user_id = event.source.user_id

        # 대상 목록에 없으면(목록이 비어있어도 마찬가지) 아무것도 하지 않음
        if not user_id or user_id not in MEDIA_SAVE_TARGET_USER_IDS:
            return

        source_type = event.source.type
        group_id = event.source.group_id if source_type == "group" else None
        message_id = event.message.id

        content, content_type = _download_line_message_content(message_id)
        if not content:
            return

        file_hash = hashlib.md5(content).hexdigest()
        ext = _guess_media_ext(content_type, media_kind)
        file_name = f"{file_hash}.{ext}"

        nickname = get_line_user_nickname(user_id, group_id)
        alias = MEDIA_SAVE_TARGET_USER_IDS.get(user_id)
        folder = _sanitize_storage_folder_name(alias, fallback=user_id) if alias else _sanitize_storage_folder_name(user_id)
        storage_path = f"{folder}/{file_name}"

        _ensure_media_bucket_exists()

        # 같은 파일(=같은 해시)이 이미 저장돼 있으면 스킵
        try:
            existing = supabase.storage.from_(MEDIA_STORAGE_BUCKET).list(folder)
            if any((f.get("name") if isinstance(f, dict) else getattr(f, "name", None)) == file_name for f in (existing or [])):
                print(f"↩️ 이미 저장된 파일이라 스킵: {storage_path}")
                return
        except Exception as e:
            print(f"⚠️ 기존 파일 목록 조회 실패(계속 업로드 시도): {e}")

        try:
            supabase.storage.from_(MEDIA_STORAGE_BUCKET).upload(
                storage_path,
                content,
                {"content-type": content_type or "application/octet-stream"},
            )
            print(f"✅ 미디어 저장 완료: {storage_path} (닉네임: {nickname})")
        except Exception as e:
            print(f"🚨 미디어 업로드 실패: {e}")

    except Exception as e:
        print(f"🚨 save_target_user_media 처리 오류: {e}")

# ================================================================
# ✨ [추가됨] 초대인증요청(/남초, /여초, /동반) 횟수 조회 — 관리자 전용 ✨
#
# chat_logs에는 사용자가 보낸 원문 메시지가 그대로 쌓이기 때문에,
# "/남초", "/여초", "/동반" 요청 자체도 이미 기록되어 있다.
# 이를 이용해 특정 닉네임이 초대인증요청을 몇 번 했는지 집계한다.
#
# 같은 종류의 요청을 5분 이내에 연속으로 보내면(예: 응답이 없어 재요청)
# '중복'으로 분리 집계해서 "3번(중복 2번)"처럼 보여준다.
#
# 실제 닉네임에는 이모티콘/숫자가 붙어있는 경우가 많아서(예: '모모1234🌟'),
# 관리자가 매번 그걸 다 입력하긴 번거롭다. 그래서 조회할 때는 한글만 입력해도
# 실제 닉네임에서 한글 부분만 뽑아 비교해서 대상을 찾아준다.
#
# 1) extract_hangul                  : 문자열에서 한글(가~힣)만 뽑아낸다
# 2) is_invite_stat_question         : 구어체 질문인지 저비용으로 1차 판별
# 3) gemini_extract_invite_stat_target: 질문에서 대상 닉네임(한글)만 추출 (LLM 1회)
# 4) get_invite_verification_stats   : 한글 닉네임으로 남초/여초/동반 요청 집계 + 5분 중복 처리
# 5) find_nicknames_like             : 정확히 일치하는 기록이 없을 때 비슷한 닉네임 후보 검색
# 6) format_invite_verification_stats: 집계 결과를 메시지 텍스트로 포맷
# ================================================================

INVITE_VERIFICATION_COMMANDS = ["남초", "여초", "동반"]

# 초대인증요청 통계 기능은 이 방(그룹)에서만 동작한다.
# (다른 방/1:1 채팅에서는 "초대시도 몇번?" 같은 문구가 와도 이 기능이 반응하지 않고
#  그냥 평소 처리(공지 검색/일반 대화 등)로 흘러간다.)
INVITE_STAT_ALLOWED_GROUP_ID = "C668c127b1dc4d04b42a2f2724c91f113"

# ✨ [추가됨] 관리자가 "개인(1:1) 대화창"에서 /디비갱신, /아이디갱신 등을 입력했을 때
# 대신 갱신할 "연결된 그룹채팅방" ID.
# - 환경변수 LINKED_GROUP_ID 가 있으면 그것을 우선 사용하고,
# - 없으면 INVITE_STAT_ALLOWED_GROUP_ID(본방으로 쓰는 방)를 기본값으로 사용한다.
# 다른 방을 쓰고 싶으면 아래 값만 바꾸거나 환경변수를 설정하면 된다.
ADMIN_LINKED_GROUP_ID = os.environ.get("LINKED_GROUP_ID") or INVITE_STAT_ALLOWED_GROUP_ID

def resolve_sync_group_id(source_type, group_id):
    """그룹방에서 입력했으면 그 방 ID를, 1:1 대화창이면 연결된 그룹방 ID를 반환한다."""
    if source_type == "group" and group_id:
        return group_id
    return ADMIN_LINKED_GROUP_ID

_HANGUL_RE = re.compile(r"[가-힣]+")


def extract_hangul(text):
    """문자열에서 한글(완성형 가~힣)만 뽑아 이어붙여 반환한다.
    예: '모모1234🌟' → '모모', '🌟민수☆' → '민수'"""
    if not text:
        return ""
    return "".join(_HANGUL_RE.findall(text))


def find_user_id_by_nickname_hangul(nickname_query):
    """
    닉네임(한글 부분만 비교)으로 현재 등록된 LINE 유저의 user_id를 찾는다.
    이모티콘/숫자가 붙은 닉네임(예: '⛑️04 망두📌')도 한글만 추출해서 비교하므로
    '망두'처럼 입력해도 매칭된다. ('/ㅇㅅ 대상 멘션할사람' 처럼 멘션 대상을
    이름으로 지정할 때 사용)

    후보가 여러 명이면 가장 최근에 갱신된(updated_at 최신) 사람을 우선한다.

    ✨ [수정됨] 나간 사람(leave_date 기록됨)은 후보에서 제외한다.
    """
    hangul_query = extract_hangul(nickname_query)
    if not hangul_query:
        return None
    try:
        rows = (
            supabase.table("line_users")
            .select("line_user_id,nickname,updated_at,leave_date")
            .execute()
            .data or []
        )
    except Exception as e:
        print(f"⚠️ 닉네임→아이디 조회 오류: {e}")
        return None

    candidates = [
        r for r in rows
        if r.get("line_user_id")
        and not r.get("leave_date")
        and extract_hangul(str(r.get("nickname") or "")) == hangul_query
    ]
    if not candidates:
        return None

    candidates.sort(key=lambda r: r.get("updated_at") or "", reverse=True)
    return candidates[0]["line_user_id"]


def find_user_id_by_nickname_partial(nickname_query):
    """
    ✨ [추가됨] 관리자 시트처럼 닉네임 전체가 아니라 한글 일부(보통 2글자)만
    입력해놔도 매칭시키기 위한 함수. find_user_id_by_nickname_hangul(완전 일치)과
    달리, 입력값이 실제 닉네임의 한글 부분에 '포함'되기만 해도 매칭한다.
    예: 시트에 '철수'만 적어놔도 실제 닉네임이 '김철수'나 '철수짱🔥'이면 찾는다.

    1) 한글 부분이 완전히 같은 사람이 있으면 그 사람으로 확정 (동명이인이면 최신순 우선).
    2) 완전 일치가 없으면, 입력값이 부분 포함되는 사람을 찾는다.
       - 정확히 한 명이면 그 사람으로 확정.
       - 두 명 이상이면 누구를 가리키는지 모호하므로 매칭 실패 처리한다
         (잘못된 사람을 멘션하는 것보다 안전하게 실패시키는 게 낫다).

    반환값: (user_id 또는 None, 모호할 때의 후보 닉네임 리스트)

    ✨ [수정됨] 나간 사람(leave_date 기록됨)은 후보에서 제외한다.
    """
    hangul_query = extract_hangul(nickname_query)
    if not hangul_query:
        return None, []

    try:
        rows = (
            supabase.table("line_users")
            .select("line_user_id,nickname,updated_at,leave_date")
            .execute()
            .data or []
        )
    except Exception as e:
        print(f"⚠️ 닉네임→아이디 조회 오류: {e}")
        return None, []

    exact_matches = []
    partial_matches = []
    for r in rows:
        uid = r.get("line_user_id")
        if not uid or r.get("leave_date"):
            continue
        target_hangul = extract_hangul(str(r.get("nickname") or ""))
        if not target_hangul:
            continue
        if target_hangul == hangul_query:
            exact_matches.append(r)
        elif hangul_query in target_hangul:
            partial_matches.append(r)

    if exact_matches:
        exact_matches.sort(key=lambda r: r.get("updated_at") or "", reverse=True)
        return exact_matches[0]["line_user_id"], []

    if len(partial_matches) == 1:
        return partial_matches[0]["line_user_id"], []

    if len(partial_matches) > 1:
        candidate_names = [str(r.get("nickname") or "") for r in partial_matches]
        return None, candidate_names

    return None, []


def is_invite_stat_question(text):
    """
    "모모 초대시도 몇번?" 같은 초대인증요청 횟수 질문인지 가볍게 판별한다.
    (LLM 호출 전에 걸러내는 저비용 1차 필터. '초대' + 횟수를 묻는 표현이 함께 있어야 통과)

    """
    if not text or "초대" not in text:
        return False
    count_hints = ["몇번", "몇 번", "몇회", "몇 회", "횟수", "얼마나", "몇차례"]
    return any(h in text for h in count_hints)


def gemini_extract_invite_stat_target(user_message):
    """
    초대인증요청 횟수 질문("모모 초대시도 몇번?")에서 조회 대상 닉네임만 추출한다.
    """
    global API_KEY_INDEX, MODEL_INDEX

    tool = [{
        "function_declarations": [{
            "name": "get_invite_stat_target",
            "description": "초대인증요청(남초/여초/동반) 횟수를 조회할 대상자의 닉네임을 추출한다.",
            "parameters": {
                "type": "OBJECT",
                "properties": {
                    "nickname": {
                        "type": "STRING",
                        "description": "조회 대상의 닉네임 (조사/어미와 질문 표현은 제거하고 닉네임만)"
                    }
                },
                "required": ["nickname"]
            }
        }]
    }]

    payload = {
        "systemInstruction": {
            "parts": [{
                "text": (
                    "사용자의 질문에서 초대인증요청(남초/여초/동반) 횟수를 조회할 "
                    "대상자의 닉네임만 추출해서 반드시 get_invite_stat_target 함수를 호출해. "
                    "조사(은,는,이,가,의 등)와 뒤에 붙는 질문 표현(초대시도 몇번?, "
                    "초대 몇번 했어?, 초대인증요청 횟수 등)은 제거하고 닉네임만 남긴다. "
                    "닉네임을 특정할 수 없으면 빈 문자열로 호출해."
                )
            }]
        },
        "contents": [{"role": "user", "parts": [{"text": user_message}]}],
        "tools": tool,
        "toolConfig": {
            "functionCallingConfig": {
                "mode": "ANY",
                "allowedFunctionNames": ["get_invite_stat_target"]
            }
        },
        "generationConfig": {"temperature": 0}
    }

    max_attempts = max(1, len(API_KEYS) * len(MODELS) * 2)

    for _ in range(max_attempts):
        current_key = API_KEYS[API_KEY_INDEX]
        current_model = MODELS[MODEL_INDEX]
        url = (
            "https://generativelanguage.googleapis.com/"
            f"v1beta/models/{current_model}:generateContent"
            f"?key={current_key}"
        )
        try:
            res = session.post(url, json=payload, timeout=(3, 10))
            res.raise_for_status()
            candidates = res.json().get("candidates", [])
            if not candidates:
                return ""
            for part in candidates[0].get("content", {}).get("parts", []):
                if "functionCall" in part:
                    fargs = part["functionCall"].get("args") or {}
                    return str(fargs.get("nickname", "")).strip()
            return ""
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 500
            print(f"🚨 초대통계 대상 추출 HTTP 오류 {status}: {e}")
            if status == 429:
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            elif status == 503:
                time.sleep(1)  # 일시적 과부하니 살짝 쉬었다가 재시도
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            elif is_gemini_auth_error(status, e.response):
                # 키/권한/결제 문제는 모델이 아니라 키를 바꿔야 함
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            else:
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue
        except Exception as e:
            print(f"🚨 초대통계 대상 추출 오류: {e}")
            MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue

    return ""



def _parse_chatlog_dt(row):
    """chat_logs 한 행에서 집계용 datetime(KST)을 뽑아낸다. created_at을 우선 사용하고,
    없으면 log_date + log_time을 조합해서 만든다."""
    kst = timezone(timedelta(hours=9))
    created_at = row.get("created_at")
    if created_at:
        try:
            dt = datetime.fromisoformat(created_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(kst)
        except Exception:
            pass

    log_date, log_time = row.get("log_date"), row.get("log_time")
    if log_date and log_time:
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(f"{log_date} {log_time}", fmt).replace(tzinfo=kst)
            except Exception:
                continue
    return None


def get_invite_verification_stats(nickname_query):
    """
    닉네임(한글만 입력해도 됨)으로 /남초, /여초, /동반 초대인증요청 횟수를 집계한다.
    같은 종류의 요청이 5분 이내에 연속으로 오면 '중복'으로 별도 집계한다.
    (예: 짧은 시간 안에 3번 보냈으면 → 총 3번, 그 중 중복 2번, 순수 시도 1번)

    실제 저장된 닉네임에 이모티콘/숫자가 섞여 있어도(예: '모모1234🌟'),
    입력값과 닉네임 둘 다 한글만 추출해서 비교하기 때문에 '모모'만 입력해도 찾는다.
    한글 부분이 같은 닉네임이 여러 개(예: 닉네임을 바꾼 이력) 있으면 모두 합산한다.

    각 요청의 실제 날짜/시각도 entries에 함께 담아서, 중복이 정확히
    언제(며칠 몇시) 발생했는지 나중에 표시할 수 있게 한다.

    반환값: (stats, matched_nicknames)
      - stats: {
            "남초": {"raw":.., "duplicate":.., "unique":..,
                     "entries": [{"dt": datetime, "duplicate": bool}, ...]},
            "여초": {...}, "동반": {...},
            "_total": {"raw":.., "duplicate":.., "unique":..}
        } 또는 기록이 전혀 없으면 None
      - matched_nicknames: 실제로 매칭된 원본 닉네임 목록 (예: ['모모1234🌟'])
    """
    hangul_query = extract_hangul(nickname_query)
    if not hangul_query:
        return None, []

    try:
        q = (
            supabase.table("chat_logs")
            .select("nickname,log_date,log_time,message,created_at")
            .in_("message", [f"/{c}" for c in INVITE_VERIFICATION_COMMANDS])
        )
        rows = fetch_all_supabase_query(q)
    except Exception as e:
        print(f"⚠️ 초대인증요청 조회 오류: {e}")
        return None, []

    if not rows:
        return None, []

    matched_nicknames = set()
    by_type = {c: [] for c in INVITE_VERIFICATION_COMMANDS}
    for r in rows:
        raw_nickname = str(r.get("nickname", "")).strip()
        if extract_hangul(raw_nickname) != hangul_query:
            continue
        matched_nicknames.add(raw_nickname)

        msg = str(r.get("message", "")).strip()
        cmd = msg[1:].strip() if msg.startswith("/") else msg
        if cmd in by_type:
            dt = _parse_chatlog_dt(r)
            if dt:
                by_type[cmd].append(dt)

    if not matched_nicknames:
        return None, []

    result = {}
    total_raw, total_dup = 0, 0
    for cmd, timestamps in by_type.items():
        timestamps.sort()
        entries, dup_count, last_dt = [], 0, None
        for dt in timestamps:
            # 날짜가 바뀌었더라도 datetime 자체를 비교하므로 자정을 넘나드는
            # 요청도 정확하게(예: 23:58 → 익일 00:02 = 4분 차이) 판별된다.
            is_dup = last_dt is not None and (dt - last_dt) <= timedelta(minutes=5)
            if is_dup:
                dup_count += 1
            entries.append({"dt": dt, "duplicate": is_dup})
            last_dt = dt
        result[cmd] = {
            "raw": len(timestamps),
            "duplicate": dup_count,
            "unique": len(timestamps) - dup_count,
            "entries": entries,
        }
        total_raw += len(timestamps)
        total_dup += dup_count

    if total_raw == 0:
        return None, []

    result["_total"] = {"raw": total_raw, "duplicate": total_dup, "unique": total_raw - total_dup}
    return result, sorted(matched_nicknames)


def find_nicknames_like(nickname_query, limit=5):
    """
    정확히 일치하는 닉네임 기록이 없을 때, chat_logs에 남아있는 닉네임 중
    한글 부분에 검색어가 포함되는 후보를 찾아준다 (오타/줄임말 대응용).
    """
    hangul_query = extract_hangul(nickname_query)
    if not hangul_query:
        return []
    try:
        rows = (
            supabase.table("chat_logs")
            .select("nickname")
            .ilike("nickname", f"%{hangul_query}%")
            .limit(500)
            .execute()
            .data or []
        )
        seen = []
        for r in rows:
            nk = str(r.get("nickname", "")).strip()
            if nk and nk not in seen:
                seen.append(nk)
        return seen[:limit]
    except Exception as e:
        print(f"⚠️ 닉네임 후보 검색 오류: {e}")
        return []


def format_invite_verification_stats(nickname_query, stats, matched_nicknames=None, max_dates_per_type=10):
    """
    get_invite_verification_stats() 결과를 메시지 텍스트로 포맷한다.
    날짜별로 묶어서 중복이 정확히 어느 날짜(몇 시)에 발생했는지도 보여준다.
    """
    if not stats:
        return f"😢 '{nickname_query}'님의 초대인증요청(남초/여초/동반) 기록을 찾지 못했어요."

    matched_nicknames = matched_nicknames or []
    header = f"🔍 {nickname_query}님의 초대인증요청 현황"
    # 실제 닉네임이 이모티콘/숫자가 붙은 형태라 입력값과 다르면 참고용으로 함께 보여준다.
    if matched_nicknames and matched_nicknames != [nickname_query]:
        header += f"\n(대상 닉네임: {', '.join(matched_nicknames)})"
    lines = [header]

    for cmd in INVITE_VERIFICATION_COMMANDS:
        s = stats.get(cmd, {"raw": 0, "duplicate": 0, "unique": 0, "entries": []})
        if s["raw"] == 0:
            continue

        line_header = f"- {cmd}: {s['raw']}번"
        if s["duplicate"] > 0:
            line_header += f"(중복 {s['duplicate']}번)"
        lines.append(line_header)

        # 날짜(월/일) 단위로 묶기
        by_date = {}
        for e in s["entries"]:
            d = e["dt"].strftime("%m/%d")
            by_date.setdefault(d, []).append(e)

        dates_sorted = sorted(by_date.keys())
        shown_dates = dates_sorted[-max_dates_per_type:]  # 최근 날짜 우선 표시
        omitted = len(dates_sorted) - len(shown_dates)
        if omitted > 0:
            lines.append(f"   … 이전 {omitted}개 날짜 생략")

        for d in shown_dates:
            ents = by_date[d]
            times = [
                f"{e['dt'].strftime('%H:%M')}(중복)" if e["duplicate"] else e["dt"].strftime("%H:%M")
                for e in ents
            ]
            dup_in_date = sum(1 for e in ents if e["duplicate"])
            date_tag = f"{len(ents)}번" + (f"(중복 {dup_in_date}번)" if dup_in_date else "")
            lines.append(f"   · {d} {date_tag}: {', '.join(times)}")

    total = stats["_total"]
    if total["duplicate"] > 0:
        lines.append(f"\n📌 총 {total['raw']}번 요청(중복 {total['duplicate']}번, 순수 시도 {total['unique']}번)")
    else:
        lines.append(f"\n📌 총 {total['raw']}번 요청")

    return "\n".join(lines)


# ================================================================
# ✨ [추가됨] "/초대통계 [기간]" : 지정 기간 동안 전체 초대인증요청 집계
# ================================================================
def get_invite_stats_by_period(start_date, end_date):
    """
    지정한 기간(start_date~end_date, 'YYYY-MM-DD') 동안 초대인증요청
    (/남초, /여초, /동반)을 누가 몇 번, 어떤 종류로 했는지 전체 집계한다.

    ✨ [변경됨] get_invite_verification_stats()와 동일하게, 같은 사람이 같은
    종류의 요청을 5분 이내에 연속으로 보내면 '중복'으로 판정해서 집계에서
    제외한다. row/total의 각 명령어 수치는 이제 "순수 시도(unique)" 기준이고,
    실제로 입력된 총 횟수와 제외된 중복 횟수는 "_raw합계"/"_dup합계"
    (그리고 명령어별 "_dup")에 별도로 담아 표시용으로 넘긴다.

    ⚠️ 닉네임은 바뀔 수 있으므로 user_id 기준으로 사람을 구분하고,
    표시할 닉네임은 현재 등록된 최신 닉네임을 우선 사용한다
    (탈퇴 등으로 현재 등록 정보가 없으면 로그에 남은 닉네임으로 폴백).

    반환: (rows, total) 또는 기록이 없으면 (None, None)
      rows: [{"user_id":.., "nickname":.., "남초":n(순수), "여초":n, "동반":n,
              "합계":n(순수), "_raw합계":n, "_dup합계":n, "_dup":{cmd:n}}, ...]
            (합계 내림차순 정렬)
      total: {"남초":n(순수), "여초":n, "동반":n, "합계":n(순수),
              "_raw합계":n, "_dup합계":n}
    """
    try:
        q = (
            supabase.table("chat_logs")
            .select("user_id,nickname,message,log_date,log_time,created_at")
            .in_("message", [f"/{c}" for c in INVITE_VERIFICATION_COMMANDS])
            .gte("log_date", start_date)
            .lte("log_date", end_date)
        )
        rows = fetch_all_supabase_query(q)
    except Exception as e:
        print(f"⚠️ 초대통계(기간) 조회 오류: {e}")
        return None, None

    if not rows:
        return None, None

    # key(user_id 또는 닉네임) -> 명령어 -> [datetime, ...]
    by_key_cmd = {}
    key_uid = {}
    key_nickname = {}
    for r in rows:
        msg = str(r.get("message", "")).strip()
        cmd = msg[1:].strip() if msg.startswith("/") else msg
        if cmd not in INVITE_VERIFICATION_COMMANDS:
            continue

        uid = str(r.get("user_id") or "").strip()
        nickname = str(r.get("nickname") or "").strip()
        key = uid if uid else f"nick:{nickname}"

        dt = _parse_chatlog_dt(r)
        if dt is None:
            continue

        key_uid.setdefault(key, uid)
        if nickname:
            key_nickname[key] = nickname  # 로그에 남은 것 중 가장 마지막 닉네임으로 갱신

        by_key_cmd.setdefault(key, {c: [] for c in INVITE_VERIFICATION_COMMANDS})
        by_key_cmd[key][cmd].append(dt)

    if not by_key_cmd:
        return None, None

    try:
        user_map = get_line_user_map()
    except Exception:
        user_map = {}

    result_rows = []
    total_by_cmd = {c: {"raw": 0, "duplicate": 0, "unique": 0} for c in INVITE_VERIFICATION_COMMANDS}
    total_raw, total_dup, total_unique = 0, 0, 0

    for key, cmd_map in by_key_cmd.items():
        uid = key_uid.get(key, "")
        display_nickname = key_nickname.get(key) or "(닉네임 없음)"
        if uid and uid in user_map:
            nn = str(user_map[uid].get("nickname", "")).strip()
            if nn and nn != "아이디 확인" and not nn.startswith("아이디 확인("):
                display_nickname = nn

        row = {"user_id": uid, "nickname": display_nickname, "_dup": {}}
        row_raw, row_dup, row_unique = 0, 0, 0

        for c in INVITE_VERIFICATION_COMMANDS:
            timestamps = sorted(cmd_map[c])
            dup_count, last_dt = 0, None
            for dt in timestamps:
                is_dup = last_dt is not None and (dt - last_dt) <= timedelta(minutes=5)
                if is_dup:
                    dup_count += 1
                last_dt = dt

            raw = len(timestamps)
            unique = raw - dup_count

            row[c] = unique
            row["_dup"][c] = dup_count
            row_raw += raw
            row_dup += dup_count
            row_unique += unique

            total_by_cmd[c]["raw"] += raw
            total_by_cmd[c]["duplicate"] += dup_count
            total_by_cmd[c]["unique"] += unique

        row["합계"] = row_unique
        row["_raw합계"] = row_raw
        row["_dup합계"] = row_dup
        result_rows.append(row)

        total_raw += row_raw
        total_dup += row_dup
        total_unique += row_unique

    result_rows.sort(key=lambda x: x["합계"], reverse=True)

    total = {"합계": total_unique, "_raw합계": total_raw, "_dup합계": total_dup}
    for c in INVITE_VERIFICATION_COMMANDS:
        total[c] = total_by_cmd[c]["unique"]

    return result_rows, total


def format_invite_stats_by_period(label, rows, total):
    """
    ✨ [변경됨] row/total의 각 명령어 수치는 이제 5분 이내 중복을 제외한
    순수 시도(unique) 기준이다. 실제로 몇 번 더 눌렀는지(중복 제외분)는
    괄호로 함께 보여준다.
    """
    lines = [f"📊 초대인증요청 통계 ({label})", ""]
    for i, row in enumerate(rows, start=1):
        dup_map = row.get("_dup", {})
        detail_parts = []
        for c in INVITE_VERIFICATION_COMMANDS:
            if row[c] <= 0 and dup_map.get(c, 0) <= 0:
                continue
            part = f"{c} {row[c]}회"
            if dup_map.get(c, 0) > 0:
                part += f"(중복 {dup_map[c]}회 제외)"
            detail_parts.append(part)
        detail = ", ".join(detail_parts)

        line = f"{i}. {row['nickname']} — 총 {row['합계']}회"
        if row.get("_dup합계", 0) > 0:
            line += f" (중복 {row['_dup합계']}회 제외, 실입력 {row['_raw합계']}회)"
        if detail:
            line += f" ({detail})"
        lines.append(line)

    detail_total = ", ".join(f"{c} {total[c]}회" for c in INVITE_VERIFICATION_COMMANDS)
    lines.append("")
    total_line = f"📌 전체 합계: {total['합계']}회"
    if total.get("_dup합계", 0) > 0:
        total_line += f" (중복 {total['_dup합계']}회 제외, 실입력 {total['_raw합계']}회)"
    total_line += f" ({detail_total})"
    lines.append(total_line)
    return "\n".join(lines)


# ================================================================
# ✨ [변경됨] "/신입통계 [기간]" : 지정 기간 동안 새로 들어온 사람 찾기
#
# 예전엔 chat_logs 전체를 훑어서 "첫 대화 날짜"로 신입 여부를 추정했는데,
# 로그 양이 너무 많아 무거웠다. 이제는 MemberJoinedEvent(입장)/
# MemberLeftEvent(퇴장) 때마다 line_users.join_date / leave_date에 곧바로
# 기록해두므로, chat_logs를 스캔하지 않고 line_users만 조회하면 된다.
# ================================================================
def get_new_members_by_period(start_date, end_date):
    """
    지정한 기간(start_date~end_date) 동안 join_date가 찍힌 사람을
    line_users 테이블에서 직접 조회한다.

    - 방을 나간 사람도 leave_date로부터 CHAT_LOG_DB_RETENTION_WEEKS
      (기본 6주) 동안은 line_users에 남아있으므로("지금은 나감" 표시),
      그 유예기간 안이면 여기서도 함께 조회된다. 6주가 지나면
      purge_expired_left_members()가 실제로 삭제하므로 더는 조회되지 않는다.
    - ⚠️ 이 기능(join_date 기록)이 추가되기 전에 가입한 사람은 join_date가
      비어있어 조회되지 않을 수 있다.

    반환: [{"user_id":.., "nickname":.., "first_date":.., "left": bool}, ...]
          (입장일 오름차순 정렬), 없으면 빈 리스트
    """
    try:
        res = (
            supabase.table("line_users")
            .select("line_user_id,nickname,join_date,leave_date")
            .gte("join_date", start_date)
            .lte("join_date", end_date)
            .order("join_date", desc=False)
            .execute()
        )
        rows = res.data or []
    except Exception as e:
        print(f"⚠️ 신입통계 조회 오류: {e}")
        return []

    new_members = []
    for r in rows:
        uid = str(r.get("line_user_id") or "").strip()
        join_date = r.get("join_date")
        if not uid or not join_date:
            continue

        nickname = str(r.get("nickname") or "").strip()
        if not nickname or nickname == "아이디 확인" or nickname.startswith("아이디 확인("):
            nickname = "(닉네임 없음)"

        new_members.append({
            "user_id": uid,
            "nickname": nickname,
            "first_date": join_date,
            "left": bool(r.get("leave_date")),
        })

    new_members.sort(key=lambda x: x["first_date"])
    return new_members


def format_new_members_by_period(label, new_members):
    lines = [f"🆕 신입 통계 ({label}) — 총 {len(new_members)}명", ""]
    for i, m in enumerate(new_members, start=1):
        tag = " (지금은 나감)" if m["left"] else ""
        lines.append(f"{i}. {m['nickname']}{tag} — {m['first_date']} 입장")
    return "\n".join(lines)

# ================================================================
# ✨ [추가됨] "/성비", "/ㅅㅂ" : 현재 등록된 남/여 비율 조회
#
# line_users.gender(닉네임의 성별 이모지로 자동 판별해 기록해둔 값)를
# 집계한다. 방을 나간 사람(leave_date 있음)은 active_only 기본값에 의해
# 제외되고, 성별이 판별 안 된(빈 값) 사람은 "미확인"으로 따로 표시한다.
# ================================================================
def get_gender_ratio():
    try:
        user_map = get_line_user_map()  # active_only=True 기본값
    except Exception as e:
        print(f"⚠️ 성비 조회 오류: {e}")
        return None

    male_count = 0
    female_count = 0
    unknown_count = 0
    for info in user_map.values():
        gender = str(info.get("gender") or "").strip()
        if gender == "남":
            male_count += 1
        elif gender == "여":
            female_count += 1
        else:
            unknown_count += 1

    return {"male": male_count, "female": female_count, "unknown": unknown_count}


def get_invite_policy_by_ratio(male, female):
    """
    ✨ [추가됨] 남:여 성비(male/female)에 따른 초대 정책 문구를 반환한다.
    - 1.5:1 이하 → 제한 없이 초대 가능
    - 1.5:1 초과 ~ 2:1 미만 → 뚝쓴(검사지 있는) 남자만 초대 가능
    - 2:1 이상 → 남자 초대 금지
    (여자가 0명인데 남자가 1명이라도 있으면 비율이 무한대이므로 "2:1 이상"으로 취급)
    """
    if female == 0:
        m_over_f = float("inf") if male > 0 else 0.0
    else:
        m_over_f = male / female

    if m_over_f <= 1.5:
        return "🟢 제한 없이 초대 가능"
    elif m_over_f < 2:
        return "🟡 뚝쓴(검사지 있는) 남자만 초대 가능"
    else:
        return "🔴 남자 초대 금지"


def format_gender_ratio(ratio):
    male = ratio["male"]
    female = ratio["female"]
    unknown = ratio["unknown"]
    total = male + female + unknown

    # ✨ [변경됨] "약분(gcd)" 대신, 여자를 1로 고정하고 남자를 그 배수로
    # 표시한다 (소수점 둘째 자리까지 반올림, 끝자리가 0이면 생략).
    # 예: 남 8, 여 3 → "2.67:1" / 남 10, 여 4 → "2.5:1" / 남 3, 여 3 → "1:1"
    if male == 0 and female == 0:
        base_ratio_str = "0:0"
    elif female == 0:
        base_ratio_str = "∞:1"  # 여자가 0명이라 비율을 정의할 수 없음
    else:
        male_str = f"{male / female:.2f}".rstrip("0").rstrip(".")
        base_ratio_str = f"{male_str}:1"

    lines = [
        "⚖️ 현재 성비",
        "",
        f"남 : 여 = {male} : {female}  (비율 {base_ratio_str})",
    ]
    if unknown:
        lines.append(f"성별 미확인: {unknown}명 (닉네임에 성별 이모지 없음)")
    lines.append(f"총 {total}명 기준")
    lines.append("")
    lines.append(get_invite_policy_by_ratio(male, female))
    return "\n".join(lines)

def _drop_stale_mentionee_from_error(messages, err_body_text):
    """
    ✨ [추가됨] LINE이 "The mentioned user is not found in the group."로
    멘션 메시지를 통째로 거부했을 때(관리자 시트에는 남아있지만 실제로는
    그룹을 나간 사람이 멘션 대상에 껴있는 경우), 에러 응답에서 문제가 된
    substitution 키(예: "user6")를 찾아 그 사람만 멘션에서 제거한다.

    반환값: 하나라도 제거해서 재시도해볼 만하면 True, 아니면 False.
    """
    try:
        err = json.loads(err_body_text)
    except Exception:
        return False

    top_msg = err.get("message", "")
    idx_match = re.search(r"messages\[(\d+)\]", top_msg)
    if not idx_match:
        return False

    msg_idx = int(idx_match.group(1))
    if msg_idx < 0 or msg_idx >= len(messages):
        return False

    target = messages[msg_idx]
    if not isinstance(target, dict) or target.get("type") != "textV2":
        return False

    changed = False
    for detail in err.get("details", []):
        prop = detail.get("property", "")
        key_match = re.search(r'substitution\["([^"]+)"\]', prop)
        if not key_match:
            continue
        key = key_match.group(1)

        sub = target.get("substitution", {})
        if key in sub:
            del sub[key]
            changed = True

        placeholder = "{" + key + "}"
        if placeholder in target.get("text", ""):
            new_text = target["text"].replace(placeholder, "")
            new_text = re.sub(r"[ \t]{2,}", " ", new_text).strip()
            target["text"] = new_text

    return changed


def reply_line(reply_token, messages, _retry_count=0):
    MAX_MENTIONEE_RETRIES = 10  # ✨ 최대 10명까지 하나씩 제거하며 재시도 (무한루프 방지용 안전장치)
    try:
        body = {
            "replyToken": reply_token,
            "messages": messages
        }
        payload = json.dumps(body).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {LINE_ACCESS_TOKEN}"
        }
        req = urllib.request.Request(
            "https://api.line.me/v2/bot/message/reply",
            data=payload,
            headers=headers,
            method="POST"
        )
        urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        # ✨ [추가됨] 기존엔 str(e)만 찍혀서 "HTTP Error 400: Bad Request"까지만 보이고
        # LINE이 실제로 왜 거부했는지(예: 그룹을 나간 사람이 멘션에 포함됨)는 안 보였다.
        # 응답 본문(e.read())까지 로그에 남겨서 다음엔 바로 원인 파악되게 한다.
        try:
            err_body_text = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body_text = "(응답 본문 읽기 실패)"
        print(f"🚨 메시지 전송 오류: {e} / 응답본문: {err_body_text}", file=sys.stderr)

        # ✨ [추가됨] "그룹에 없는 사용자" 에러라면, 그 사람만 멘션에서 빼고
        # 자동으로 재시도한다 (reply_token은 한 번만 쓸 수 있으므로, 실패한
        # 첫 시도에서는 urlopen이 예외를 던진 상태라 토큰이 아직 소모되지
        # 않았다고 보고 재사용한다).
        if _retry_count < MAX_MENTIONEE_RETRIES and _drop_stale_mentionee_from_error(messages, err_body_text):
            print("↩️ 그룹에 없는 멘션 대상을 제거하고 재전송 시도", file=sys.stderr)
            reply_line(reply_token, messages, _retry_count=_retry_count + 1)
    except Exception as e:
        print(f"🚨 메시지 전송 오류: {e}", file=sys.stderr)

def send_line_reply_text(reply_token, text):
    try:
        reply_line(reply_token, [{"type": "text", "text": text}])
    except Exception as e:
        print("LINE 회신 실패:", e)


def send_line_image(reply_token, image_url):
    try:
        reply_line(
            reply_token,
            [{
                "type": "image",
                "originalContentUrl": image_url,
                "previewImageUrl": image_url
            }]
        )
    except Exception as e:
        print(f"🚨 이미지 전송 오류: {e}")
# ================================================================
# 인사말 / 공지 검색 유틸
# ================================================================
def search_keyword(keyword):
    try:
        ws, _, _, _ = get_eden_sheets()
        if not ws:
            return []
        records = ws.get_all_values()
        results = []
        for i, row in enumerate(records):
            if i == 0:
                continue
            row.extend([""] * 2)
            nick = row[0].strip()
            greet = row[1].strip()
            if keyword in nick or keyword in greet:
                results.append({"닉네임": nick, "인사말": greet})
        return results
    except Exception as e:
        print("인사말 검색 오류:", e)
        return []


def is_admin(user_id):
    if not user_id:
        return False
    if user_id in HIDDEN_ADMIN_IDS:
        return True
    try:
        if supabase:
            res = supabase.table("admin_users").select("line_user_id").eq("line_user_id", user_id).limit(1).execute()
            if res.data:
                return True
    except Exception as e:
        print(f"⚠️ Supabase admin_users 조회 오류: {e}")

    try:
        admin_ws = get_admin_sheet()
        if not admin_ws:
            return False
        records = admin_ws.get_all_values()
        for row in records:
            if not row:
                continue
            for cell in row:
                if cell and str(cell).strip() == user_id:
                    return True
    except Exception as e:
        print(f"⚠️ 관리자 시트 조회 오류: {e}")
    return False

def get_notice_text():
    try:
        if supabase:
            q = supabase.table("공지").select("키워드")
            res_data = fetch_all_supabase_query(q)
            if res_data:
                rows = [r.get("키워드", "").strip() for r in res_data if r.get("키워드")]
                rows = [r for r in rows if r]
                notice_text = "\n".join(rows) if rows else ""
                return notice_text if notice_text else "현재 등록된 공지사항이 없습니다."
    except Exception as e:
        print(f"⚠️ Supabase 공지 조회 오류: {e}")

    try:
        _, notice_ws, _ , _= get_eden_sheets()
        if notice_ws is None:
            return "⚠️ 공지 시트가 연결되지 않았습니다."
        rows = notice_ws.col_values(1)
        notice_text = "\n".join([row for row in rows if row.strip()])
        return notice_text if notice_text else "현재 등록된 공지사항이 없습니다."
    except Exception:
        return "⚠️ 공지사항을 불러오지 못했습니다."

def get_image_url(keyword):
    if not keyword:
        return None

    try:
        res = (
            supabase
            .table("이미지파일")
            .select("파일명")
            .eq("키워드", keyword)
            .limit(1)
            .execute()
        )

        print(f"🖼️ 이미지 DB 조회: keyword={keyword}, data={res.data}")

        if not res.data:
            print(f"⚠️ 이미지파일에서 '{keyword}'를 찾지 못함")
            return None

        filename = str(
            res.data[0].get("파일명") or ""
        ).strip()

        if not filename:
            print(f"⚠️ '{keyword}'의 파일명이 비어 있음")
            return None

        # 혹시 DB에 경로까지 들어있다면 정리
        filename = filename.lstrip("/")

        if filename.startswith("info/"):
            file_path = filename
        else:
            file_path = f"info/{filename}"

        url = (
            supabase.storage
            .from_("image")
            .get_public_url(file_path)
        )

        print(f"🖼️ Storage 경로: {file_path}")
        print(f"🖼️ 이미지 URL: {url}")

        return url

    except Exception as e:
        print(f"⚠️ 이미지 URL 조회 오류: {e}")
        return None

                
def simple_extract_notice_keyword(text):
    """
    LLM 호출 없이 규칙 기반으로 조사/의문형 어미를 제거해
    공지 키워드 후보를 만든다.
    (접두어 없는 자유 문장에서 1차로 빠르게 시도할 때 사용,
    LLM 기반 gemini_extract_notice_keyword보다 가볍고 빠르다.)

    예:
    "원픽은?" → "원픽"
    "선갠라 뭐야?" → "선갠라"
    "동반 가능해?" → "동반"
    """
    text = str(text or "").strip()
    if not text:
        return ""

    cleaned = text.rstrip("?!？！").strip()

    # 흔한 의문형/요청형 어미 (긴 것부터 매칭해야 정확함)
    suffixes = [
        "이 뭐야", "가 뭐야", "은 뭐야", "는 뭐야", " 뭐야", "뭐야",
        "이 뭔가요", "가 뭔가요", " 뭔가요", "뭔가요",
        "알려줄래", "알려주세요", "알려줘",
        "가능한가요", "가능해",
        "어떻게 되나요", "어떻게 해", "어떻게해", "어떻게",
        "되나요", "돼", "인가요", "인가",
        "있어", "없어",
        "이야", "야",
    ]

    for suf in sorted(suffixes, key=len, reverse=True):
        if cleaned.endswith(suf):
            cleaned = cleaned[: -len(suf)].strip()
            break

    # 조사 한 글자 제거 (은/는/이/가/을/를로 끝나는 경우)
    particles = ("은", "는", "이", "가", "을", "를")
    if len(cleaned) > 1 and cleaned.endswith(particles):
        cleaned = cleaned[:-1].strip()

    return cleaned


def query_notice_rows(keyword):
    """
    공지 DB에서 제목(키워드) + 본문(공지) 컬럼을 함께 검색하는 공용 함수.
    execute_gemini_tool의 get_notice_info, 신규 /공지·/ㄱㅈ 빠른 검색이 함께 사용한다.
    """
    if not keyword:
        return []
    safe_keyword = re.sub(r"[,.()%]", "", keyword).strip() or keyword
    try:
        q = (
            supabase
            .table("공지")
            .select("키워드, 공지")
            .or_(f"키워드.like.%{safe_keyword}%,공지.like.%{safe_keyword}%")
        )
        return fetch_all_supabase_query(q) or []
    except Exception as e:
        print(f"⚠️ 공지 DB 검색 오류: {e}")
        return []

def search_notice_keyword(keyword):
    if not keyword:
        return None
    try:
        if supabase:
            q = supabase.table("공지").select("키워드,공지")
            res_data = fetch_all_supabase_query(q)
            if res_data:
                for r in res_data:
                    k = str(r.get("키워드", "")).strip()
                    if k and keyword in k:
                        return str(r.get("공지", "") or "").strip()
    except Exception as e:
        print(f"⚠️ Supabase 공지 검색 오류: {e}")

    try:
        _, notice_ws, _ ,_= get_eden_sheets()
        if notice_ws is None:
            return None
        data = notice_ws.get_all_values()
        for row in data:
            if len(row) >= 2 and keyword in str(row[0]).strip():
                return row[1].strip()
    except Exception:
        pass
    return None

def get_all_notice_keywords():
    data = None
    try:
        if supabase:
            q = supabase.table("공지").select("키워드").order("키워드", desc=False)
            res_data = fetch_all_supabase_query(q)
            if res_data:
                data = [str(r.get("키워드", "")).strip() for r in res_data if r.get("키워드")]
    except Exception as e:
        print(f"⚠️ Supabase 공지 키워드 조회 오류: {e}")

    if not data:
        try:
            _, notice_ws,_ , _ = get_eden_sheets()
            if notice_ws is None:
                return "⚠️ 공지 시트가 연결되지 않았습니다."
            rows = notice_ws.col_values(1)
            data = [row.strip() for row in rows if row.strip()]
        except Exception:
            return "⚠️ 공지 키워드 목록을 불러오지 못했습니다."

    if not data:
        return "현재 등록된 공지 키워드가 없습니다."
    joined_keywords = "\n- ".join(data)
    return (
        "📢 [봇 공지 키워드 목록]\n\n"
        f"- {joined_keywords}\n\n"
        "💡 사용법: /공지 [키워드]"
    )

def cleanup_old_sheets(doc):
    kst = timezone(timedelta(hours=9))
    cutoff_date = datetime.now(kst).date() - timedelta(days=7)

    for ws in doc.worksheets():
        if ws.title in ["마디수 임시", "개별 인사말", "공지"]:
            continue
        sheet_date = None
        match = re.match(r"(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.", ws.title)
        if match:
            try:
                sheet_date = datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))).date()
            except ValueError:
                pass
        else:
            try:
                sheet_date = datetime.strptime(ws.title, "%Y-%m-%d").date()
            except ValueError:
                continue

        if sheet_date and sheet_date < cutoff_date:
            try:
                doc.del_worksheet(ws)
            except Exception:
                pass

def export_logs_to_sheet(target_date, temp=False):
    if not supabase:
        return "⚠️ Supabase 연결이 필요합니다."

    try:
        dt = datetime.strptime(target_date, "%Y-%m-%d")
    except Exception:
        return "⚠️ 날짜 형식이 잘못되었습니다. YYYY-MM-DD 형식으로 입력하세요."

    try:
        # 변경됨: fetch_all_supabase_query 적용
        base_query = supabase.table("chat_logs").select("log_time,nickname,message,user_id,created_at").eq("log_date", target_date).order("created_at", desc=False)
        rows = fetch_all_supabase_query(base_query)
    except Exception as e:
        return f"🚨 DB 로그 조회 오류: {e}"

    if not rows:
        return f"😢 {target_date}에 기록된 대화 내역이 없습니다."

    # ✨ [추가됨] 구글시트로 "옮기는 지금 시점"의 최신 닉네임을 쓰기 위해
    # 라인유저 맵을 한 번만 조회해서 user_id → 현재 닉네임으로 치환한다.
    # (chat_logs에 저장된 nickname은 그 메시지를 "보낸 당시"의 닉네임이라,
    #  그 뒤에 닉네임을 바꾸면 옛날 이름으로 남아있게 된다)
    current_nick_map = {}
    try:
        for uid, info in get_line_user_map().items():
            nn = str(info.get("nickname", "")).strip()
            if nn and nn != "아이디 확인" and not nn.startswith("아이디 확인("):
                current_nick_map[uid] = nn
    except Exception as e:
        print(f"⚠️ 최신 닉네임 맵 로드 실패 (기존 저장된 닉네임으로 폴백): {e}")

    def resolve_export_nickname(row):
        # user_id로 현재 닉네임을 찾을 수 있으면 그걸 우선 사용하고,
        # (탈퇴/삭제 등으로) 찾을 수 없을 때만 당시 저장된 닉네임으로 폴백한다.
        uid = str(row.get("user_id") or "").strip()
        if uid and uid in current_nick_map:
            return current_nick_map[uid]
        return row.get("nickname") or ""

    try:
        def sort_key(r):
            if r.get("created_at"):
                try:
                    return datetime.fromisoformat(r["created_at"])
                except Exception:
                    pass
            return r.get("log_time") or ""
        rows_sorted = sorted(rows, key=sort_key)
    except Exception:
        rows_sorted = rows

    if temp:
        sheet_title = "마디수 임시"
    else:
        weekday = ["월", "화", "수", "목", "금", "토", "일"][dt.weekday()]
        sheet_title = f"{dt.year}. {dt.month}. {dt.day}. ({weekday})"

    try:
        try:
            doc = client.open("글마디 계산")
        except Exception:
            doc = client.create("글마디 계산")
        try:
            cleanup_old_sheets(doc)
        except Exception:
            pass

        try:
            ws = doc.worksheet(sheet_title)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            cols = 4 if temp else 3
            ws = doc.add_worksheet(
                title=sheet_title,
                rows=max(1000, len(rows_sorted) + 100),
                cols=cols
            )

        if temp:
            rows_to_insert = [["시간", "닉네임", "대화내용", "유저아이디"]]
            for r in rows_sorted:
                rows_to_insert.append([
                    r.get("log_time") or "",
                    resolve_export_nickname(r),
                    r.get("message") or "",
                    r.get("user_id") or ""
                ])
        else:
            rows_to_insert = [["시간", "닉네임", "대화내용"]]
            for r in rows_sorted:
                rows_to_insert.append([
                    r.get("log_time") or "",
                    resolve_export_nickname(r),
                    r.get("message") or ""
                ])
        ws.update(values=rows_to_insert, range_name="A1")

        if temp:
            return f"🔄 '마디수 임시' 시트 초기화 후 {target_date} 내역 {len(rows_sorted)}건 기록 완료!"
        else:
            return f"✅ '글마디 계산' 파일의 [{sheet_title}] 시트에 저장 완료! (총 {len(rows_sorted)}건)"
    except Exception as e:
        return f"🚨 시트 저장 중 오류가 발생했습니다: {e}"

# ================================================================
# 글마디 계산 날짜 시트 자동 관리
#
# 기능
# 1. KST 자정마다 전날 chat_logs를 날짜 시트로 백업
# 2. 서버가 자정에 꺼져 있었으면 서버 시작 시 누락 날짜 자동 보충
# 3. 최근 7일 범위의 날짜 시트 유지
# 4. 7일보다 오래된 날짜 시트 자동 삭제
# ================================================================

DAILY_ARCHIVE_LOCK = threading.Lock()


def get_kst_now():
    """현재 한국시간(KST)을 반환합니다."""
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst)


def get_sheet_title_for_date(target_date):
    """
    YYYY-MM-DD → 글마디 계산 날짜 시트명

    예:
    2026-08-24
    → 2026. 8. 24. (월)
    """

    try:
        dt = datetime.strptime(
            target_date,
            "%Y-%m-%d"
        )

        weekday = [
            "월",
            "화",
            "수",
            "목",
            "금",
            "토",
            "일"
        ][dt.weekday()]

        return (
            f"{dt.year}. "
            f"{dt.month}. "
            f"{dt.day}. "
            f"({weekday})"
        )

    except Exception:
        return None


def get_daily_archive_doc():
    """
    '글마디 계산' Google Spreadsheet를 가져옵니다.
    없으면 새로 만듭니다.
    """

    try:
        return client.open("글마디 계산")

    except Exception:
        try:
            print(
                "📄 '글마디 계산' 파일이 없어 "
                "새로 생성합니다."
            )

            return client.create("글마디 계산")

        except Exception as e:

            print(
                f"🚨 '글마디 계산' 파일 생성 실패: {e}"
            )

            return None

def get_existing_archive_dates(doc):
    """
    현재 '글마디 계산' 파일에 존재하는
    날짜 시트들의 날짜를 set으로 반환합니다.

    반환 예:
    {
        "2026-08-18",
        "2026-08-19",
        "2026-08-20"
    }
    """

    existing_dates = set()

    if not doc:
        return existing_dates

    for ws in doc.worksheets():

        # 관리용 시트는 제외
        if ws.title in [
            "마디수 임시",
            "개별 인사말",
            "공지"
        ]:
            continue

        sheet_date = None

        # --------------------------------------------------------
        # 현재 날짜 시트 형식
        #
        # 2026. 8. 24. (월)
        # --------------------------------------------------------
        match = re.match(
            r"^(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.",
            ws.title
        )

        if match:

            try:

                sheet_date = datetime(
                    int(match.group(1)),
                    int(match.group(2)),
                    int(match.group(3))
                ).date()

            except ValueError:
                sheet_date = None

        # --------------------------------------------------------
        # 기존 YYYY-MM-DD 형식도 호환
        # --------------------------------------------------------
        else:

            try:

                sheet_date = datetime.strptime(
                    ws.title,
                    "%Y-%m-%d"
                ).date()

            except ValueError:
                sheet_date = None

        if sheet_date:

            existing_dates.add(
                sheet_date.strftime("%Y-%m-%d")
            )

    return existing_dates

    
def archive_chatlog_date(target_date):
    """
    특정 날짜의 chat_logs를
    '글마디 계산' 날짜 시트로 백업합니다.

    이미 날짜 시트가 있으면 다시 만들지 않습니다.
    """

    if not target_date:
        return False

    with DAILY_ARCHIVE_LOCK:

        try:

            doc = get_daily_archive_doc()

            if not doc:
                return False

            # ----------------------------------------------------
            # 오래된 시트 정리
            # ----------------------------------------------------
            cleanup_old_sheets(doc)

            # ----------------------------------------------------
            # 이미 존재하는 날짜인지 확인
            # ----------------------------------------------------
            existing_dates = get_existing_archive_dates(
                doc
            )

            if target_date in existing_dates:

                print(
                    f"⏭️ 글마디 백업 이미 존재: "
                    f"{target_date}"
                )

                return True

            # ----------------------------------------------------
            # 실제 chat_logs → Google Sheet
            # ----------------------------------------------------
            result = export_logs_to_sheet(
                target_date,
                temp=False
            )

            print(
                f"📚 글마디 날짜 백업: "
                f"{target_date} → {result}"
            )

            return (
                isinstance(result, str)
                and result.startswith("✅")
            )

        except Exception as e:

            print(
                f"🚨 글마디 날짜 백업 오류 "
                f"({target_date}): {e}"
            )

       
            return False


def get_recent_completed_dates(days=7):
    """
    오늘을 제외하고 최근 N일의 날짜를 반환합니다.

    예를 들어 오늘이 8월 25일이면:

    8월 24일
    8월 23일
    8월 22일
    8월 21일
    8월 20일
    8월 19일
    8월 18일

    을 반환합니다.
    """

    now = get_kst_now()

    today = now.date()

    dates = []

    for offset in range(1, days + 1):

        target = today - timedelta(
            days=offset
        )

        dates.append(
            target.strftime("%Y-%m-%d")
        )

    return dates


def backfill_missing_chatlog_dates():
    """
    서버가 자정에 꺼져 있었거나 재시작되어
    백업하지 못한 최근 날짜를 자동 보충합니다.

    최근 7일 중 날짜 시트가 없는 날짜만 처리합니다.
    """

    print(
        "🔎 글마디 날짜 시트 누락 여부 확인 시작"
    )

    with DAILY_ARCHIVE_LOCK:

        try:

            doc = get_daily_archive_doc()

            if not doc:

                print(
                    "⚠️ 글마디 계산 파일을 열 수 없어 "
                    "누락 날짜 확인을 건너뜁니다."
                )

                return

            # ----------------------------------------------------
            # 먼저 오래된 날짜 시트 삭제
            # ----------------------------------------------------
            cleanup_old_sheets(doc)

            # ----------------------------------------------------
            # 현재 존재하는 날짜 시트
            # ----------------------------------------------------
            existing_dates = get_existing_archive_dates(
                doc
            )

            recent_dates = get_recent_completed_dates(
                days=7
            )

            # ----------------------------------------------------
            # 최근 7일 중 누락된 날짜
            # ----------------------------------------------------
            missing_dates = [
                date
                for date in recent_dates
                if date not in existing_dates
            ]

            if not missing_dates:

                print(
                    "✅ 최근 7일 글마디 날짜 시트 "
                    "누락 없음"
                )

                return

            print(
                "📋 글마디 누락 날짜: "
                + ", ".join(missing_dates)
            )

            # ----------------------------------------------------
            # 오래된 날짜부터 백업
            # ----------------------------------------------------
            for target_date in reversed(
                missing_dates
            ):

                try:

                    result = export_logs_to_sheet(
                        target_date,
                        temp=False
                    )

                    print(
                        f"📚 누락 날짜 보충 완료 "
                        f"| {target_date} "
                        f"| {result}"
                    )

                except Exception as e:

                    print(
                        f"⚠️ 누락 날짜 보충 실패 "
                        f"| {target_date} "
                        f"| {e}"
                    )

            # ----------------------------------------------------
            # 마지막 정리
            # ----------------------------------------------------
            try:
                cleanup_old_sheets(doc)
            except Exception as e:
                print(
                    f"⚠️ 오래된 시트 정리 실패: {e}"
                )

        except Exception as e:

            print(
                f"🚨 글마디 누락 날짜 확인 오류: {e}"
            )

        

def run_daily_chatlog_archive():
    """
    KST 자정을 기준으로 계속 실행되는
    백그라운드 글마디 백업 루프입니다.

    서버가 살아 있는 경우:
        00:00 → 전날 날짜 백업

    서버가 재시작된 경우:
        시작 시 backfill_missing_chatlog_dates()
        로 누락 날짜를 보충합니다.
    """

    kst = timezone(
        timedelta(hours=9)
    )

    print(
        "🕛 글마디 자정 자동 백업 루프 시작"
    )

    while True:

        try:

            now = datetime.now(kst)

            # ----------------------------------------------------
            # 다음 KST 자정
            # ----------------------------------------------------
            next_midnight = (
                now
                .replace(
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0
                )
                + timedelta(days=1)
            )

            wait_seconds = (
                next_midnight - now
            ).total_seconds()

            print(
                f"⏰ 다음 글마디 자동 백업: "
                f"{next_midnight.strftime('%Y-%m-%d %H:%M:%S')}"
            )

            # ----------------------------------------------------
            # 자정까지 대기
            # ----------------------------------------------------
            time.sleep(
                max(
                    1,
                    wait_seconds
                )
            )

            # ----------------------------------------------------
            # 자정이 지나면 전날 날짜 계산
            # ----------------------------------------------------
            archive_date = (
                datetime.now(kst)
                - timedelta(days=1)
            ).strftime("%Y-%m-%d")

            print(
                f"🌙 KST 자정 도달"
                f" | 전날 글마디 백업: "
                f"{archive_date}"
            )

            # ----------------------------------------------------
            # 전날 날짜 백업
            # ----------------------------------------------------
            archive_chatlog_date(
                archive_date
            )

            # ----------------------------------------------------
            # 오래된 시트 정리
            # ----------------------------------------------------
            try:

                doc = get_daily_archive_doc()

                if doc:
                    cleanup_old_sheets(doc)

            except Exception as e:

                print(
                    f"⚠️ 자정 오래된 시트 정리 실패: {e}"
                )

            # ----------------------------------------------------
            # DB(chat_logs) 6주 초과분 정리
            # ----------------------------------------------------
            try:
                cleanup_old_chat_logs_db()
            except Exception as e:
                print(
                    f"⚠️ 자정 DB chat_logs 정리 실패: {e}"
                )

            # ----------------------------------------------------
            # ✨ [추가됨] 퇴장 유예기간(6주) 경과자 실제 삭제
            # ----------------------------------------------------
            try:
                purge_expired_left_members()
            except Exception as e:
                print(
                    f"⚠️ 자정 퇴장자 정리 실패: {e}"
                )

        except Exception as e:

            print(
                f"🚨 글마디 자동 백업 루프 오류: {e}"
            )

            # ----------------------------------------------------
            # 오류가 발생해도 스레드가 죽지 않도록
            # 1분 후 다시 시도
            # ----------------------------------------------------
            time.sleep(60)


def start_daily_chatlog_archive():
    """
    서버 시작 시:

    1. 최근 7일 누락 날짜 보충
    2. 자정 자동 백업 스레드 시작
    """

    try:

        print(
            "🚀 글마디 자동 관리 시작"
        )

        # --------------------------------------------------------
        # 서버가 자정에 꺼져 있었던 경우를 대비한
        # 누락 날짜 보충
        # --------------------------------------------------------
        backfill_missing_chatlog_dates()

    except Exception as e:

        print(
            f"⚠️ 서버 시작 시 글마디 보충 실패: {e}"
        )

    # --------------------------------------------------------
    # 서버 시작 시에도 DB(chat_logs) 6주 초과분 정리
    # --------------------------------------------------------
    try:

        cleanup_old_chat_logs_db()

    except Exception as e:

        print(
            f"⚠️ 서버 시작 시 DB chat_logs 정리 실패: {e}"
        )

    # --------------------------------------------------------
    # ✨ [추가됨] 서버 시작 시에도 퇴장 유예기간(6주) 경과자 정리
    # --------------------------------------------------------
    try:

        purge_expired_left_members()

    except Exception as e:

        print(
            f"⚠️ 서버 시작 시 퇴장자 정리 실패: {e}"
        )

    # ------------------------------------------------------------
    # 자정 자동 백업 스레드
    # ------------------------------------------------------------
    archive_thread = threading.Thread(
        target=run_daily_chatlog_archive,
        daemon=True,
        name="daily-chatlog-archive"
    )

    archive_thread.start()

    print(
        "✅ 글마디 자정 자동 백업 스레드 시작 완료"
    )
def get_user_state(user_id):
    """
    사용자별 대화 history와 '퀴즈 모드' 상태, 그리고 '정답 공개 대기 시각'
    (quiz_deadline, ISO 문자열 또는 None), 마지막으로 저장된 시각
    (updated_at, ISO 문자열 또는 None, ✨ 퀴즈 30분 자동종료 판단용)을 함께 가져온다.
    과거에 history가 리스트 형태로만 저장돼 있던 데이터(레거시)도
    자동으로 호환 처리한다 (quiz_mode=False, quiz_deadline=None으로 간주).
    """
    if not user_id:
        return [], False, None, None
    try:
        res = supabase.table("user_histories").select("history, updated_at").eq("user_id", user_id).limit(1).execute()
        if res.data:
            row = res.data[0]
            raw = row.get("history") or []
            updated_at = row.get("updated_at")
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except Exception:
                    raw = []

            # 신규 포맷: {"messages": [...], "quiz_mode": bool, "quiz_deadline": str|None}
            if isinstance(raw, dict):
                messages = raw.get("messages") or []
                if not isinstance(messages, list):
                    messages = []
                return messages, bool(raw.get("quiz_mode", False)), raw.get("quiz_deadline"), updated_at

            # 레거시 포맷: history가 곧 메시지 리스트
            if isinstance(raw, list):
                return raw, False, None, updated_at
    except Exception as e:
        print(f"⚠️ Supabase get_user_state 오류: {e}")
    return [], False, None, None

def save_user_state(user_id, history, quiz_mode, quiz_deadline=None):
    if not user_id:
        return False
    if history is None:
        history = []
    if not isinstance(history, list):
        try:
            history = list(history)
        except Exception:
            history = [history]

    try:
        row = {
            "user_id": user_id,
            "history": {
                "messages": history,
                "quiz_mode": bool(quiz_mode),
                "quiz_deadline": quiz_deadline
            },
            "updated_at": now_utc().isoformat()
        }
        supabase.table("user_histories").upsert(row, on_conflict="user_id").execute()
        return True
    except Exception as e:
        print(f"⚠️ Supabase save_user_state 오류: {e}")
        return False

# ================================================================
# 벙 시작/종료/취소/수정 시, 진행/모집 중인 벙지가 여러 개라
# 번호로 골라달라고 물어봤을 때, 그 "선택 대기" 상태를
# line_users.state 컬럼에 잠깐 저장해둔다.
# ================================================================
def save_bung_selection_state(user_id, state):
    if not user_id:
        return
    try:
        supabase.table("line_users").update({"state": state}).eq("line_user_id", user_id).execute()
    except Exception as e:
        print(f"⚠️ 벙 선택 상태 저장 오류: {e}")


def clear_bung_selection_state(user_id):
    if not user_id:
        return
    try:
        supabase.table("line_users").update({"state": None}).eq("line_user_id", user_id).execute()
    except Exception as e:
        print(f"⚠️ 벙 선택 상태 초기화 오류: {e}")


def resolve_bung_selection_reply(user_id, raw_state, command):
    """
    직전에 '/벙시작', '/벙종', '/벙취소', '/벙수정'에서 진행/모집 중인
    벙지가 여러 개라 번호를 골라달라고 물어봤던 경우, 이번 메시지(command)가
    그 번호 선택에 대한 답인지 확인해서 처리한다.

    ⚠️ 이게 없으면(예전 상태) '🤔 종료할 벙지 선택: 1. ... 2. ...' 이후에
    사용자가 '/1'이라고 답해도 그게 어떤 명령어와도 매칭되지 않아서
    엉뚱하게 공지 질문(/공지) 처리 흐름으로 흘러들어가는 문제가 있었다.

    반환값:
      - None       : 이번 메시지는 번호 선택이 아니거나, 선택 대기 상태가
                     아예 없음 → 평소처럼 명령어 라우팅을 계속 진행하면 됨
      - list[dict] : 선택을 처리 완료함. 이 메시지들을 그대로 응답으로 보내면 됨
    """
    if not raw_state:
        return None

    state = raw_state
    if isinstance(state, str):
        try:
            state = json.loads(state)
        except Exception:
            return None

    if not isinstance(state, dict) or not state.get("bungs"):
        return None

    text = str(command or "").strip()

    # 선택 자체를 취소 (실제 '벙 취소' 액션과는 별개로, 번호 고르는 걸 그만두는 것)
    if text in ("취소", "그만", "cancel", "취소해줘"):
        clear_bung_selection_state(user_id)
        return [{"type": "text", "text": "🚫 선택을 취소했어요."}]

    match = re.fullmatch(r"(\d+)\s*번?", text)
    if not match:
        # 번호 선택 형식이 아니면 선택 대기 상태는 그대로 두고,
        # 평소처럼 다른 명령어로 라우팅되게 한다.
        return None

    bungs = state.get("bungs") or []
    idx = int(match.group(1)) - 1

    if idx < 0 or idx >= len(bungs):
        return [{
            "type": "text",
            "text": f"⚠️ 1~{len(bungs)}번 중에서 골라주세요."
        }]

    chosen = bungs[idx]
    action = state.get("action")

    try:
        _, _, bung_sheet_ws, _ = get_eden_sheets()
    except Exception as e:
        print(f"⚠️ 벙 선택 처리 중 시트 접근 오류: {e}")
        clear_bung_selection_state(user_id)
        return [{"type": "text", "text": "⚠️ 시트에 접근하지 못했어요. 명령어를 다시 입력해주세요."}]

    admin_data = get_admin_members()
    trophy_members = admin_data.get("trophy", [])
    row_num = chosen.get("row")
    title = chosen.get("title")

    if action == "start":
        bung_sheet_ws.update_acell(f"F{row_num}", "진행중")
        supabase.table("meet_log").update({"진행상황": "진행중"}).eq("벙제", title).execute()
        check_and_mark_no_ttuk_female(row_num, chosen.get("members"), chosen.get("date"))
        clear_bung_selection_state(user_id)
        return [
            {"type": "text", "text": f"🚀 '{title}' 시작!"},
            build_multi_mention_payload_v2(trophy_members, "벙시작!")
        ]

    elif action == "end":
        bung_sheet_ws.update_acell(f"F{row_num}", "종료")
        supabase.table("meet_log").update({"진행상황": "종료"}).eq("벙제", title).execute()
        clear_bung_selection_state(user_id)
        return [
            {"type": "text", "text": f"🏁 '{title}' 종료!"},
            build_multi_mention_payload_v2(trophy_members, "벙종!")
        ]

    elif action == "cancel":
        bung_sheet_ws.update_acell(f"F{row_num}", "취소")
        supabase.table("meet_log").update({"진행상황": "취소"}).eq("벙제", title).execute()
        clear_bung_selection_state(user_id)
        return [
            {"type": "text", "text": f"🚫 '{title}' 취소됨"},
            build_multi_mention_payload_v2(trophy_members, "벙취소!")
        ]

    elif action == "edit":
        new_data = state.get("new_data") or {}
        final_date = new_data.get("date") or chosen.get("date")
        members = new_data.get("members")
        location = new_data.get("location")
        new_title = new_data.get("title")
        status = chosen.get("status")
        original_author = chosen.get("author")

        bung_sheet_ws.update(
            values=[[original_author, final_date, members, location, new_title, status]],
            range_name=f"A{row_num}:F{row_num}"
        )
        supabase.table("meet_log").update({
            "날짜": final_date,
            "멤버": members,
            "장소": location,
            "벙제": new_title
        }).eq("벙제", title).execute()
        clear_bung_selection_state(user_id)
        return [
            {"type": "text", "text": f"✅ 벙지 내용이 수정되었습니다.\n- 벙제: {new_title}"},
            build_multi_mention_payload_v2(trophy_members, "벙지수정!")
        ]

    clear_bung_selection_state(user_id)
    return [{"type": "text", "text": "⚠️ 처리할 수 없는 선택 상태라 초기화했어요. 명령어를 다시 입력해주세요."}]

# ================================================================
# 퀴즈 모드 - 시작/종료 키워드 및 시스템 프롬프트
# ================================================================
QUIZ_START_KEYWORDS = [
    "퀴즈", "문제 내줘", "문제내줘", "문제 줘", "문제줘",
    "퀴즈내줘", "퀴즈 내줘", "문제 하나", "문제내", "문제 좀",
    "넌센스", "넌센스퀴즈", "넌센스 퀴즈", "난센스", "난센스퀴즈", "난센스 퀴즈"
]

QUIZ_STOP_KEYWORDS = [
    "퀴즈 그만", "퀴즈그만", "그만할래", "그만할게", "그만 낼래",
    "퀴즈 종료", "퀴즈종료", "퀴즈 끝", "퀴즈끝",
    "안 할래", "안할래", "관둘래", "그만해", "끝", "끝내", "끝낼래", "종료", "안 낼래", "안낼래",
    "스탑", "stop", "그만", "스톱"
]

# ✨ [추가됨] 퀴즈 모드에서 이만큼(분) 동안 사용자 응답이 없으면 자동 종료
QUIZ_AUTO_TIMEOUT_MINUTES = 30

BASE_SYSTEM_PROMPT = (
    "너는 카카오톡/라인 단체방에서 대화하는 챗봇이야. "
    "친하고 짓궂은 친구처럼, 가끔 능청스럽게 드립도 치고 "
    "장난스럽게 놀리기도 하면서 대화해. 단, 상대가 기분 나쁠 정도로 "
    "심하게 놀리진 말고 같이 웃을 수 있는 선을 지켜. "
    "사용자가 퀴즈나 문제를 명시적으로 요청하지 않는 이상, "
    "네가 먼저 퀴즈나 문제를 내지 마.\n\n"
    "[내부 제어 규칙 - 사용자에게 절대 언급하거나 설명하지 말 것]\n"
    "만약 사용자의 메시지가 (정확한 단어를 쓰지 않았더라도) 퀴즈나 문제를 "
    "내달라는 요청으로 해석되고, 그래서 네가 실제로 이번 답변에서 "
    "문제를 하나 냈다면, 답변의 맨 마지막 줄에 [QUIZ_STARTED] 태그를 "
    "단독으로 붙여라. 퀴즈 요청이 아니거나 문제를 내지 않았다면 "
    "이 태그를 절대 붙이지 마라."
)

QUIZ_SYSTEM_PROMPT = (
    "너는 카카오톡/라인 단체방에서 대화하는 챗봇이야. "
    "친하고 짓궂은 친구처럼, 가끔 능청스럽게 드립도 치고 "
    "장난스럽게 놀리기도 하면서 대화해. 단, 상대가 기분 나쁠 정도로 "
    "심하게 놀리진 말고 같이 웃을 수 있는 선을 지켜. "
    "지금은 사용자와 퀴즈를 진행하는 중이야. "
    "아직 문제를 내지 않았다면 먼저 문제를 하나 내고, 정답은 알려주지 말고 답을 기다려. "
    "사용자가 답을 보내면 이전에 낸 문제를 기준으로 정답 여부를 판정해.\n"
    "- 맞았으면: 재밌게 칭찬한 뒤 정답을 간단히 확인하고, 반드시 '다음 문제를 더 낼까, 아니면 여기서 끝낼까?'처럼 계속할지 종료할지 먼저 물어봐. "
    "맞았다는 이유만으로 새 문제를 바로 내지 마. 사용자의 선택을 기다려.\n"
    "- 사용자가 더 내달라고 하면: 새 문제를 하나 내고 계속 진행해. '응', '네', '더 내줘', '계속', '한 문제 더', '다음 문제'처럼 긍정 의사를 보이면 계속하는 것으로 판단해.\n"
    "- 사용자가 그만/끝내기를 원하면: 짧게 마무리하고 퀴즈를 종료해. '아니', '괜찮아', '끝', '그만', '종료', '안 낼래'처럼 부정 의사를 보이면 종료하는 것으로 판단해.\n"
    "- 틀렸으면: 정답을 절대 알려주지 마. 별도로 '지금은 정답을 공개해도 된다'는 안내를 받은 경우가 아니면, 같은 문제를 유지한 채 살짝 놀리듯 힌트만 주고 다시 맞혀보라고 유도해. 다음 문제로 넘어가지 마.\n"
    "- 정답 공개 유예 시간이 지나서 오답인 경우: 정답을 알려준 뒤 바로 새 문제를 내지 말고, 역시 다음 문제를 더 낼지 여기서 끝낼지 물어봐.\n"
    "직전 대화 맥락(이전에 낸 문제, 정답, 사용자가 계속할지 끝낼지 묻는 단계)을 반드시 참고해.\n\n"
    "[할 수 없는 것 - 헷갈리지 말고 그냥 무시할 것]\n"
    "너는 스스로 메시지를 예약해서 나중에 먼저 말을 걸 수는 없어(사용자가 메시지를 보내야만 답할 수 있어). 하지만 사용자가 '정답은 10분 뒤에 알려줘'처럼 정답 공개를 미뤄달라고 요청하면, 그 요청 자체는 서버가 별도로 처리해서 기억해두니 이런 요청 때문에 퀴즈를 끝난 것으로 취급하지 마.\n"
    "같은 방의 다른 사람이 '/정답 답'처럼 보내면 그 답도 이 퀴즈에 대한 답으로 처리해.\n\n"
    "[내부 제어 규칙 - 사용자에게 절대 언급하거나 설명하지 말 것]\n"
    "매 답변의 맨 마지막 줄에는 아래 두 태그 중 하나만 단독으로 반드시 붙여라:\n"
    "- 퀴즈를 계속 진행하거나, 사용자의 선택을 기다리는 중이면 [QUIZ_CONTINUE]\n"
    "- 사용자가 퀴즈 종료를 선택했거나 퀴즈와 무관한 다른 주제로 넘어갔으면 [QUIZ_END]\n"
    "(주의: 정답을 맞혔다고 해서 [QUIZ_END]를 붙이지 마라. 맞힌 뒤에는 계속/종료 선택을 물어보고 [QUIZ_CONTINUE]와 [QUIZ_CHOICE]를 붙여라.)"
    "[QUIZ_CHOICE]는 방금 문제를 해결했고 사용자의 계속/종료 선택을 기다리는 상태라는 내부 태그다. 이 상태에서는 새 문제를 먼저 내지 마라."
)

# 사용자가 "N분 뒤/후에 (정답) 알려줘" 처럼 정답 공개를 미뤄달라고
# 요청하는 패턴을 감지한다. (예: "5분 뒤에 알려줘", "10분 후 정답 공개",
# "3분 있다가 알려줘", "이따 알려줘")
QUIZ_DEADLINE_PATTERN = re.compile(r"(\d{1,3})\s*분\s*(뒤|후|있다가|이따가|이따)")

def parse_quiz_deadline_minutes(prompt):
    """
    프롬프트에서 '정답 공개를 미뤄줄 시간(분)'을 추출한다.
    못 찾으면 None을 반환한다. 상한선(24시간)을 넘기는 값은 무시한다.
    """
    text = (prompt or "").strip()
    if not text:
        return None
    m = QUIZ_DEADLINE_PATTERN.search(text)
    if not m:
        return None
    try:
        minutes = int(m.group(1))
    except Exception:
        return None
    if minutes <= 0 or minutes > 24 * 60:
        return None
    return minutes

def wants_quiz(prompt):
    text = (prompt or "").strip()
    return any(kw in text for kw in QUIZ_START_KEYWORDS)

def wants_quiz_stop(prompt):
    text = (prompt or "").strip()
    return any(kw in text for kw in QUIZ_STOP_KEYWORDS)
    
def ask_gemini(user_id, prompt):
    global API_KEY_INDEX, MODEL_INDEX

    user_history, quiz_mode, quiz_deadline, quiz_updated_at = get_user_state(user_id)

    # ------------------------------------------------------------
    # ✨ [추가됨] 퀴즈 모드 30분 자동 종료
    # 마지막으로 상태가 저장된 시각(updated_at)으로부터 QUIZ_AUTO_TIMEOUT_MINUTES
    # 이상 지났으면, 사용자가 그동안 응답이 없었다는 뜻이므로 퀴즈를 자동으로
    # 종료 처리한다. (봇이 스스로 먼저 말을 걸 순 없으니, 다음에 아무 메시지든
    # 보내오는 시점에 조용히 종료 처리하고 일반 대화로 진행한다.)
    # ------------------------------------------------------------
    if quiz_mode and quiz_updated_at:
        try:
            last_dt = datetime.fromisoformat(quiz_updated_at)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            if now_utc() - last_dt >= timedelta(minutes=QUIZ_AUTO_TIMEOUT_MINUTES):
                quiz_mode = False
                quiz_deadline = None
        except Exception as e:
            print(f"⚠️ 퀴즈 자동종료 시각 파싱 오류: {e}")

    # ------------------------------------------------------------
    # 퀴즈 모드 on/off 결정
    # - "퀴즈"/"문제 내줘" 등 명시적 요청 → 켬
    # - "그만"/"stop" 등 명시적 종료 요청 → 끔
    # - 그 외에는 이전 턴에 저장된 quiz_mode를 그대로 이어받고,
    #   실제 종료 시점은 모델이 매 응답 끝에 붙이는 [QUIZ_END] 태그로 판단
    # ------------------------------------------------------------
    # ✨ 이 메시지에서 "새로" 퀴즈가 시작된 것인지 구분한다.
    # (이전 턴까지는 quiz_mode가 아니었는데, 이번 prompt로 새로 켜졌다면
    #  아직 실제로 낸 문제가 하나도 없는 상태다.)
    quiz_just_started = False
    if wants_quiz_stop(prompt):
        quiz_mode = False
        quiz_deadline = None
    elif wants_quiz(prompt):
        quiz_just_started = not quiz_mode
        quiz_mode = True

    # ------------------------------------------------------------
    # ✨ 퀴즈 중 "N분 뒤에 알려줘" 같은 정답 공개 유예 요청 처리
    #
    # 이 요청은 Gemini 호출 없이 서버가 직접 시각을 계산해서 저장한다.
    # 저장된 시각이 지나기 전까지는(quiz_deadline) 오답이어도 절대 정답을
    # 공개하지 않고, 지난 뒤에만 다음 오답 판정 턴에서 공개를 허용한다.
    # (봇은 스스로 먼저 말을 걸 수 없으므로, "그 시간이 지난 뒤 사용자가
    # 다시 답을 보낼 때" 공개된다.)
    #
    # ⚠️ 단, "퀴즈 내줘 + 5분 뒤에 알려줘"처럼 퀴즈 시작 요청과 유예 요청이
    # 한 메시지에 같이 오면(quiz_just_started=True), 여기서 바로 return해
    # 버리면 Gemini가 한 번도 호출되지 않아 실제 문제가 안 나가는 문제가
    # 있었다. 이 경우엔 유예 시각만 저장해두고, return하지 않고 아래로
    # 흘려보내 Gemini가 실제 문제를 생성하도록 한다.
    # ------------------------------------------------------------
    if quiz_mode:
        requested_minutes = parse_quiz_deadline_minutes(prompt)
        if requested_minutes:
            deadline_ts = now_utc() + timedelta(minutes=requested_minutes)
            quiz_deadline = deadline_ts.isoformat()

            if not quiz_just_started:
                user_history.append({"role": "user", "parts": [{"text": prompt}]})
                confirm_text = (
                    f"⏳ 오케이! {requested_minutes}분 동안은 정답 공개 안 하고 계속 문제 풀 "
                    f"기회 줄게. 그 안에 틀려도 힌트만 줄 테니까 계속 맞혀봐~ "
                    f"{requested_minutes}분 지나고 나서 또 틀리면 그때 정답 알려줄게!"
                )
                user_history.append({"role": "model", "parts": [{"text": confirm_text}]})

                if len(user_history) > MAX_HISTORY_LEN:
                    user_history = user_history[-MAX_HISTORY_LEN:]
                    while user_history and user_history[0].get("role") != "user":
                        user_history.pop(0)

                save_user_state(user_id, user_history, quiz_mode, quiz_deadline)
                return confirm_text

    user_history.append({"role": "user", "parts": [{"text": prompt}]})

    if len(user_history) > MAX_HISTORY_LEN:
        user_history = user_history[-MAX_HISTORY_LEN:]
        # 잘라낸 뒤 첫 항목이 "model"이면 role 순서가 깨져 Gemini가 혼란스러워하므로
        # 맨 앞에 남은 model 턴을 마저 제거해 항상 "user"로 시작하게 함
        while user_history and user_history[0].get("role") != "user":
            user_history.pop(0)

    # ------------------------------------------------------------
    # ✨ 이번 턴에 오답이어도 정답 공개를 허용할지 여부를 시스템 프롬프트에
    # 덧붙여서 매 턴 전달한다. (deadline이 없거나 아직 안 지났으면 항상 비공개)
    # ------------------------------------------------------------
    reveal_allowed_this_turn = False
    if quiz_mode:
        if quiz_deadline:
            try:
                deadline_dt = datetime.fromisoformat(quiz_deadline)
                if now_utc() >= deadline_dt:
                    reveal_allowed_this_turn = True
            except Exception:
                pass

        if reveal_allowed_this_turn:
            system_prompt = QUIZ_SYSTEM_PROMPT + (
                "\n\n[현재 턴 전용 안내 - 사용자에게 절대 언급하거나 설명하지 말 것]\n"
                "사용자가 아까 요청한 정답 공개 유예 시간이 이미 지났다. 이번 사용자의 "
                "답이 오답이면, 이번에는 정답을 알려줘도 된다. 정답을 알려준 뒤 바로 새 문제를 내지 말고, 다음 문제를 더 낼지 여기서 끝낼지 물어봐라."
            )
        else:
            system_prompt = QUIZ_SYSTEM_PROMPT + (
                "\n\n[현재 턴 전용 안내 - 사용자에게 절대 언급하거나 설명하지 말 것]\n"
                "이번 사용자의 답이 오답이더라도 정답을 절대로 알려주지 마라. 같은 "
                "문제를 그대로 유지하면서 힌트만 주고 다시 맞혀보라고 유도해라."
            )
            if quiz_just_started and quiz_deadline:
                system_prompt += (
                    "\n\n사용자가 이번 메시지에서 퀴즈를 요청하면서 동시에 정답 공개를 "
                    "미뤄달라고 했다. 정답 공개를 미룬다는 걸 짧게 확인해준 뒤, 바로 "
                    "이어서 실제 문제를 하나 내라. 문제를 생략하지 마라."
                )
    else:
        system_prompt = BASE_SYSTEM_PROMPT

    # ✨ [변경됨] 모델 2개 x 키 개수 만큼만 돌면 503/429처럼 "잠깐" 겹치는
    # 일시적 오류에 너무 쉽게 다 실패 처리돼서, 한 바퀴 더(*2) 돌게 늘렸다.
    # (404처럼 영구적으로 안 되는 모델은 그대로 죽은 걸로 취급하고 건너뜀)
    for _ in range(len(API_KEYS) * len(MODELS) * 2):
        current_key = API_KEYS[API_KEY_INDEX]
        current_model = MODELS[MODEL_INDEX]
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{current_model}:generateContent?key={current_key}"

        try:
            response = session.post(
                url,
                json={
                    "systemInstruction": {
                        "parts": [{"text": system_prompt}]
                    },
                    "contents": user_history,
                    "generationConfig": {"temperature": 0.6, "topP": 0.8, "maxOutputTokens": 1024}
                },
                timeout=(3, 10)
            )
            response.raise_for_status()
            res_data = response.json()
            candidates = res_data.get("candidates", [])

            if candidates:
                parts = candidates[0].get("content", {}).get("parts", [])
                answer_parts = [p for p in parts if p.get("text") and not p.get("thought")]
                if answer_parts and answer_parts[-1].get("text", ""):
                    raw_text = answer_parts[-1].get("text", "").strip()

                    # 퀴즈 모드일 때는 진행/종료 태그를, 아닐 때는 (키워드로 못 걸러낸
                    # 요청을 제미나이가 스스로 퀴즈로 인식해서 문제를 냈는지) 시작 태그를
                    # 해석하고, 어느 쪽이든 사용자에게는 노출하지 않는다
                    if quiz_mode:
                        if "[QUIZ_END]" in raw_text:
                            quiz_mode = False
                            quiz_deadline = None
                        elif reveal_allowed_this_turn:
                            # 유예 시간이 지나 이번 턴에 정답을 공개했다면(=새 문제로
                            # 넘어갔다면) 이전 유예 시각은 더 이상 유효하지 않으니 초기화
                            quiz_deadline = None
                        waiting_for_choice = "[QUIZ_CHOICE]" in raw_text
                        if waiting_for_choice:
                            # 현재 문제가 해결되어 계속/종료 선택을 기다리는 상태이므로
                            # 이전 문제에 걸려 있던 정답 공개 유예 시각은 무효화한다.
                            quiz_deadline = None
                        text = (
                            raw_text.replace("[QUIZ_CONTINUE]", "")
                                    .replace("[QUIZ_END]", "")
                                    .replace("[QUIZ_CHOICE]", "")
                                    .strip()
                        )
                    else:
                        if "[QUIZ_STARTED]" in raw_text:
                            quiz_mode = True
                        quiz_deadline = None
                        text = raw_text.replace("[QUIZ_STARTED]", "").strip()

                    # history에는 원본(태그 포함)을 남겨 모델이 다음 턴에도
                    # 같은 형식을 유지하도록 돕는다.
                    user_history.append({"role": "model", "parts": [{"text": raw_text}]})
                    save_user_state(user_id, user_history, quiz_mode, quiz_deadline)
                    return text

            if user_history:
                user_history.pop()

            return "😢 답변을 생성하지 못했습니다."
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code
            if status in [503, 404]:
                if status == 503:
                    time.sleep(1)  # ✨ [추가됨] 일시적 과부하니 살짝 쉬었다가 재시도
                MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            elif status == 429 or is_gemini_auth_error(status, e.response):
                API_KEY_INDEX = (API_KEY_INDEX + 1) % len(API_KEYS)
                time.sleep(1)
            continue
        except Exception:
            MODEL_INDEX = (MODEL_INDEX + 1) % len(MODELS)
            continue

    return "😢 오늘 체력이 모두 바닥났어요!"

# ================================================================
# LINE Webhook
# ================================================================
handler = WebhookHandler(LINE_CHANNEL_SECRET)

@app.route("/")
def home():
    return "FLIRTY LINE Bot is Running!"

@app.route("/api", methods=["POST"])
def callback():
    try:
        handler.handle(
            request.get_data(as_text=True),
            request.headers.get("X-Line-Signature")
        )
    except InvalidSignatureError:
        abort(400)
    return "OK"

@handler.add(MemberJoinedEvent)
def handle_member_joined(event):
    """
    ✨ [추가됨] 방(그룹)에 새 멤버가 입장하면, 채팅을 치기 전이라도
    즉시 라인유저 시트/DB에 아이디+닉네임을 등록한다.
    (등록 로직은 기존에 메시지 수신 시 쓰던 get_line_user_nickname을 그대로 재사용:
     LINE 프로필 조회 → 라인유저 시트/DB 저장 → 실패 시 "아이디 확인"으로 저장)
    """
    try:
        source_type = getattr(event.source, "type", None)
        group_id = getattr(event.source, "group_id", None) if source_type == "group" else None

        members = getattr(event.joined, "members", None) or []
        for member in members:
            user_id = getattr(member, "user_id", None)
            if not user_id:
                continue
            try:
                nickname = get_line_user_nickname(user_id, group_id)
                # ✨ [추가됨] get_line_user_nickname은 캐시된 닉네임이 있으면
                # 그 값을 그대로 반환하고 시트/DB 갱신(save_user_to_line_sheet)을
                # 건너뛴다. 그러면 "나갔다가 다시 들어온" 사람의 leave_date가
                # 안 지워지는 문제가 생기므로, 입장 이벤트에서는 캐시 여부와
                # 상관없이 항상 한 번 더 명시적으로 저장 처리를 해서
                # 재입장 시 leave_date가 확실히 지워지도록 한다.
                if nickname and not nickname.startswith("아이디 확인"):
                    save_user_to_line_sheet(user_id, nickname)
            except Exception as e:
                print(f"⚠️ 입장 멤버 등록 실패 ({user_id}): {e}")
    except Exception as e:
        print(f"⚠️ 멤버 입장 이벤트 처리 오류: {e}")

@handler.add(MemberLeftEvent)
def handle_member_left(event):
    """
    ✨ [변경됨] 방(그룹)에서 멤버가 나가면 즉시 삭제하지 않고,
    라인유저 DB(line_users)에 leave_date만 오늘 날짜로 기록한다.
    (즉시 삭제하면 "/신입통계"에서 최근 퇴장자 정보를 알 수 없어지므로,
    CHAT_LOG_DB_RETENTION_WEEKS(기본 6주) 동안은 남겨뒀다가
    purge_expired_left_members()가 그 이후에 실제로 삭제한다)
    """
    try:
        members = getattr(event.left, "members", None) or []
        left_ids = [getattr(m, "user_id", None) for m in members]
        left_ids = [uid for uid in left_ids if uid]

        if left_ids:
            ok, marked_nicknames = mark_line_users_left(left_ids)
            if ok and marked_nicknames:
                print(f"👋 퇴장 기록 완료(유예기간 시작): {', '.join(marked_nicknames)}")
            elif not ok:
                print(f"⚠️ 퇴장 기록 실패: {left_ids}")
    except Exception as e:
        print(f"⚠️ 멤버 퇴장 이벤트 처리 오류: {e}")

@handler.add(MessageEvent, message=StickerMessageContent)
@handler.add(MessageEvent, message=AudioMessageContent)
@handler.add(MessageEvent, message=LocationMessageContent)
def handle_ignored_messages(event):
    pass

@handler.add(MessageEvent, message=ImageMessageContent)
def handle_image_message(event):
    save_target_user_media(event, "image")

@handler.add(MessageEvent, message=VideoMessageContent)
def handle_video_message(event):
    save_target_user_media(event, "video")

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_message = event.message.text.strip()
    if not user_message:
        return

    source_type = event.source.type
    group_id = event.source.group_id if source_type == "group" else None
    user_id = event.source.user_id

    # ✅ 대화 로그 저장
    # (베르셀 서버리스 환경에서는 응답 반환 즉시 함수 실행이 끝날 수 있어
    #  fire-and-forget 스레드는 로그 유실 위험이 있음 → 동기 실행으로 변경)
    log_message_to_db(
        user_id=user_id,
        message=user_message,
        source_type=source_type,
        group_id=group_id,
        message_id=event.message.id,
        timestamp=event.timestamp
    )

    admin_data = get_admin_members()
    TROPHY_MEMBERS = admin_data.get("trophy", [])
    PIN_MEMBERS = admin_data.get("pin", [])
    BOOK_MEMBERS = admin_data.get("book", [])
    ALL_ADMINS = TROPHY_MEMBERS + PIN_MEMBERS + BOOK_MEMBERS + HIDDEN_ADMIN_IDS
    Real_ADMIN = TROPHY_MEMBERS + PIN_MEMBERS + HIDDEN_ADMIN_IDS

    # 1. Supabase에서 해당 유저의 현재 상태(state)를 조회합니다.
    state_val = None
    try:
        state_res = supabase.table("line_users").select("state").eq("line_user_id", user_id).execute()
        if state_res.data and len(state_res.data) > 0:
            state_val = state_res.data[0].get("state")
    except Exception as e:
        print(f"⚠️ 유저 상태 조회 오류: {e}")
    
    if state_val:
        print(f"현재 유저 상태: {state_val}")
    else:
        pass
        
    if not user_message.startswith("/"):
        return
    
    command = user_message[1:].strip()
    if not command:
        return

    messages_to_send = []
    admin_only_commands = [
        "방아이디", "방id", "내ID", "id", "아이디",
        "임시저장", "마디확인", "임시확인",
        "미등록갱신", "아이디갱신", "닉네임갱신", "미등록확인",
        "운영진갱신", "디비갱신", "ㄷㅂㄳ" , "시트정리", "아이디정리"
    ]
    
    all_only_commands = [
        "1번", "2번"
    ]

    # ✨ '/벙시작', '/벙종', '/벙취소', '/벙수정'에서 후보가 여러 개라
    # 번호를 골라달라고 물어봤던 경우, 이번 메시지가 그 번호 선택인지 먼저 확인한다.
    # (예: "🤔 종료할 벙지 선택: 1. ... 2. ..." → "/1") 다른 명령어 라우팅보다
    # 항상 먼저 검사해서, 엉뚱하게 공지 질문 등으로 흘러가지 않게 한다.
    bung_selection_reply = resolve_bung_selection_reply(user_id, state_val, command)

    if bung_selection_reply is not None:
        messages_to_send.extend(bung_selection_reply)

    elif command in admin_only_commands and user_id not in Real_ADMIN:
        messages_to_send.append({"type": "text", "text": "⛔ 관리자만 사용할 수 있는 명령어입니다."})

    elif command in all_only_commands and user_id not in ALL_ADMINS:
        messages_to_send.append({"type": "text", "text": "⛔ 관리자 및 인증자만 사용할 수 있는 명령어입니다."})

    # elif group_id == INVITE_STAT_ALLOWED_GROUP_ID and (command.startswith("초대통계") or is_invite_stat_question(command)):
    #     # ✨ 초대인증요청(/남초, /여초, /동반) 횟수 조회 — 관리자 전용
    #     if user_id not in Real_ADMIN:
    #         messages_to_send.append({"type": "text", "text": "⛔ 초대인증요청 기록은 관리자만 조회할 수 있어요."})
    #     else:
    #         if command.startswith("초대통계"):
    #             # 명시적 명령어: "/초대통계 모모" 또는 "/초대통계 이번주" (LLM 없이 바로 판별)
    #             raw_arg = command[len("초대통계"):].strip()
    #         else:
    #             raw_arg = ""

    #         # ✨ [추가됨] 닉네임이 아니라 "기간" 표현(이번주/저번주/N주/N일/날짜범위 등)이면
    #         # 특정 인원이 아니라 그 기간 전체 초대인증요청 집계를 보여준다.
    #         period = parse_meet_list_period(raw_arg) if raw_arg else None

    #         if period:
    #             start_date, end_date, label = period
    #             rows, total = get_invite_stats_by_period(start_date, end_date)
    #             if not rows:
    #                 messages_to_send.append({
    #                     "type": "text",
    #                     "text": f"😢 {label} 동안 초대인증요청(남초/여초/동반) 기록이 없어요."
    #                 })
    #             else:
    #                 messages_to_send.append({
    #                     "type": "text",
    #                     "text": format_invite_stats_by_period(label, rows, total)
    #                 })
    #         else:
    #             target_nickname = raw_arg

    #             if not target_nickname:
    #                 # 구어체 질문("모모 초대시도 몇번?")에서 닉네임만 추출 (LLM 1회)
    #                 target_nickname = gemini_extract_invite_stat_target(command)

    #             if not target_nickname:
    #                 messages_to_send.append({
    #                     "type": "text",
    #                     "text": "🤔 누구의 초대인증요청 기록을 확인할지 닉네임을 못 찾았어요.\n예: '/모모 초대시도 몇번?', '/초대통계 모모', '/초대통계 이번주'"
    #                 })
    #             else:
    #                 stats, matched_nicknames = get_invite_verification_stats(target_nickname)
    #                 if stats is None:
    #                     candidates = [n for n in find_nicknames_like(target_nickname) if n != target_nickname]
    #                     if candidates:
    #                         cand_str = ", ".join(candidates)
    #                         messages_to_send.append({
    #                             "type": "text",
    #                             "text": f"😢 '{target_nickname}'님의 기록을 찾지 못했어요. 혹시 이 중에 있을까요?\n{cand_str}"
    #                         })
    #                     else:
    #                         messages_to_send.append({
    #                             "type": "text",
    #                             "text": f"😢 '{target_nickname}'님의 초대인증요청(남초/여초/동반) 기록을 찾지 못했어요."
    #                         })
    #                 else:
    #                     messages_to_send.append({
    #                         "type": "text",
    #                         "text": format_invite_verification_stats(target_nickname, stats, matched_nicknames)
    #                     })

    # elif command.startswith("신입통계"):
    #     # ✨ [추가됨] "/신입통계 [기간]" : 지정 기간 동안 새로 들어온 사람 찾기
    #     # (나가서 라인유저 시트/DB에서 삭제된 사람도 chat_logs를 참고해서 찾아준다)
    #     if user_id not in Real_ADMIN:
    #         messages_to_send.append({"type": "text", "text": "⛔ 신입 통계는 관리자만 조회할 수 있어요."})
    #     else:
    #         raw_arg = command[len("신입통계"):].strip()
    #         period = parse_meet_list_period(raw_arg) if raw_arg else None

    #         if not period:
    #             messages_to_send.append({
    #                 "type": "text",
    #                 "text": (
    #                     "🤔 기간을 이해하지 못했어요.\n"
    #                     "예: '/신입통계 이번주', '/신입통계 저번주', '/신입통계 2주', "
    #                     "'/신입통계 09.01~09.14'"
    #                 )
    #             })
    #         else:
    #             start_date, end_date, label = period
    #             new_members = get_new_members_by_period(start_date, end_date)
    #             if not new_members:
    #                 messages_to_send.append({
    #                     "type": "text",
    #                     "text": f"😢 {label} 동안 새로 들어온 사람이 없어요."
    #                 })
    #             else:
    #                 messages_to_send.append({
    #                     "type": "text",
    #                     "text": format_new_members_by_period(label, new_members)
    #                 })

    # elif command in ("성비", "ㅅㅂ"):
    #     # ✨ [추가됨] "/성비", "/ㅅㅂ" : 현재 등록된 남/여 비율 조회
    #     ratio = get_gender_ratio()
    #     if ratio is None:
    #         messages_to_send.append({"type": "text", "text": "🚨 성비 조회 중 오류가 발생했어요."})
    #     else:
    #         messages_to_send.append({"type": "text", "text": format_gender_ratio(ratio)})

    elif command == "디비갱신" or command == "ㄷㅂㄳ":
        target_group_id = resolve_sync_group_id(source_type, group_id)
        result_message = sync_all_sheets_to_db(target_group_id)
        if source_type != "group":
            result_message = f"(1:1 대화창 → 연결된 그룹방 {str(target_group_id)[:8]}… 기준으로 갱신)\n" + result_message
        try:
            reply_url = "https://api.line.me/v2/bot/message/reply"
            headers = {
                "Authorization": f"Bearer {LINE_ACCESS_TOKEN}",
                "Content-Type": "application/json"
            }
            payload = {
                "replyToken": event.reply_token,
                "messages": [
                    {
                        "type": "text",
                        "text": f"✅ 디비 갱신 완료!\n\n{result_message}"
                    }
                ]
            }
            requests.post(reply_url, headers=headers, json=payload)
        except Exception as e:
            print(f"⚠️ 답장 전송 오류: {e}")
        return

    # elif re.match(r'^벙지?\s*작성', command):
    #     content = re.sub(r'^벙지?\s*작성\s*', '', command, count=1).strip()
    #     parts = [p.strip() for p in content.split("/")] if content else []

    #     def _parse_date_range(text):
    #         """
    #         '08.29~30' 또는 '08.29~09.02' 처럼 물결(~)로 표시된
    #         날짜 범위를 'YYYY-MM-DD~YYYY-MM-DD' 형태로 변환한다.
    #         범위 표현이 아니면 None을 반환한다.
    #         """
    #         t = (text or "").strip().replace(" ", "")
    #         if "~" not in t:
    #             return None

    #         kst = timezone(timedelta(hours=9))
    #         this_year = datetime.now(kst).year

    #         left, _, right = t.partition("~")
    #         if not left or not right:
    #             return None

    #         m_start_full = re.match(r'^(\d{4})[-./년](\d{1,2})[-./월](\d{1,2})일?$', left)
    #         m_start_short = re.match(r'^(\d{1,2})[-./월](\d{1,2})일?$', left)

    #         if m_start_full:
    #             y, mo, d = int(m_start_full.group(1)), int(m_start_full.group(2)), int(m_start_full.group(3))
    #         elif m_start_short:
    #             y, mo, d = this_year, int(m_start_short.group(1)), int(m_start_short.group(2))
    #         else:
    #             return None

    #         try:
    #             start_dt = datetime(y, mo, d)
    #         except ValueError:
    #             return None

    #         # 종료일: '30일?'처럼 일자만 오거나, '09.02'처럼 월.일이 같이 오는 경우 둘 다 지원
    #         m_end_day_only = re.match(r'^(\d{1,2})일?$', right)
    #         m_end_month_day = re.match(r'^(\d{1,2})[-./월](\d{1,2})일?$', right)

    #         if m_end_day_only:
    #             end_y, end_mo, end_d = y, mo, int(m_end_day_only.group(1))
    #         elif m_end_month_day:
    #             end_y, end_mo, end_d = y, int(m_end_month_day.group(1)), int(m_end_month_day.group(2))
    #         else:
    #             return None

    #         try:
    #             end_dt = datetime(end_y, end_mo, end_d)
    #         except ValueError:
    #             return None

    #         # 종료일이 시작일보다 앞서면(예: 12.30~1) 해가 넘어간 것으로 처리
    #         if end_dt < start_dt:
    #             end_dt = end_dt.replace(year=end_dt.year + 1)

    #         return f"{start_dt.strftime('%Y-%m-%d')}~{end_dt.strftime('%Y-%m-%d')}"

    #     def _looks_like_date(text):
    #         t = (text or "").strip().replace(" ", "")
    #         if t in ["오늘", "내일", "모레", "내일모레", "글피"]:
    #             return True
    #         if re.match(r'^\d{4}[-./년]?\d{1,2}[-./월]?\d{1,2}일?$', t):
    #             return True
    #         if re.match(r'^\d{1,2}[-./월]\d{1,2}일?$', t):
    #             return True
    #         if _parse_date_range(t):
    #             return True
    #         return False

    #     format_guide = (
    #         "[작성 예시]\n"
    #         "/벙지 작성 오늘 / 멤버 / 장소 / 벙제\n"
    #         "/벙지 작성 08.28 / 멤버 / 장소 / 벙제\n"
    #         "/벙지 작성 08.29~30 / 멤버 / 장소 / 벙제  (날짜 범위도 가능)"
    #     )

    #     # 날짜는 '오늘'로 자동 채우지 않고 항상 명시적으로 입력받는다.
    #     # (숫자 날짜든 '오늘'이든 반드시 맨 앞에 써야 함 → 뒤 항목이 밀려서
    #     #  날짜가 멤버/장소로 잘못 들어가는 사고를 방지)
    #     members = location = title = date_str = None
    #     error_msg = None

    #     if not parts or not parts[0] or not _looks_like_date(parts[0]):
    #         error_msg = f"⚠️ 맨 앞에 날짜를 꼭 입력해주세요. (오늘이어도 '오늘'이라고 쓰거나, 08.28처럼 숫자로 표기)\n\n{format_guide}"
    #     else:
    #         _range_str = _parse_date_range(parts[0])
    #         date_str = _range_str if _range_str else parse_date_string(parts[0])
    #         rest = parts[1:]

    #         if len(rest) >= 3:
    #             members, location = rest[0], rest[1]
    #             title = "/".join(rest[2:]).strip()  # 벙제 안에 '/'가 더 있어도 살아남도록
    #             missing = []
    #             if not members:
    #                 missing.append("멤버")
    #             if not location:
    #                 missing.append("장소")
    #             if not title:
    #                 missing.append("벙제")
    #             if missing:
    #                 error_msg = f"⚠️ {', '.join(missing)}이(가) 빠졌습니다.\n\n{format_guide}"
    #                 members = location = title = None
    #         elif len(rest) == 2:
    #             error_msg = f"⚠️ 장소가 빠졌습니다. (입력하신 순서상 멤버: '{rest[0]}', 벙제: '{rest[1]}'로 인식됨)\n\n{format_guide}"
    #         elif len(rest) == 1:
    #             error_msg = f"⚠️ 장소와 벙제가 빠졌습니다. (입력하신 멤버: '{rest[0]}')\n\n{format_guide}"
    #         else:
    #             error_msg = f"⚠️ 멤버, 장소, 벙제를 모두 입력해주세요.\n\n{format_guide}"

    #     if error_msg:
    #         messages_to_send.append({"type": "text", "text": error_msg})
    #     else:
    #         nickname = get_line_user_nickname(user_id, group_id)
    #         try:
    #             _, _, bung_sheet_ws, _ = get_eden_sheets()
    #             if bung_sheet_ws:
    #                 records = bung_sheet_ws.get_all_values()
    #                 next_row = len(records) + 1
    #                 bung_sheet_ws.update(
    #                     values=[[nickname, date_str, members, location, title, "모집중"]],
    #                     range_name=f"A{next_row}:F{next_row}"
    #                 )
    #                 supabase.table("meet_log").insert({
    #                     "작성자": nickname,
    #                     "날짜": date_str,
    #                     "멤버": members,
    #                     "장소": location,
    #                     "벙제": title,
    #                     "진행상황": "모집중"
    #                 }).execute()
    #                 messages_to_send.append({
    #                     "type": "text",
    #                     "text": f"✅ '{title}' 벙지가 성공적으로 등록되었습니다!\n- 날짜: {date_str.replace('~', ' ~ ')}\n- 멤버: {members}\n- 장소: {location}\n(현재 상태: 모집중)"
    #                 })
    #                 messages_to_send.append(build_multi_mention_payload_v2(TROPHY_MEMBERS, "벙지작성!"))
    #             else:
    #                 messages_to_send.append({"type": "text", "text": "🚨 벙지 시트를 찾을 수 없어 등록하지 못했습니다."})
    #         except Exception as e:
    #             messages_to_send.append({"type": "text", "text": f"🚨 벙지 등록 중 오류 발생: {e}"})
                
    # elif command.startswith(("벙지 수정", "벙수정", "벙지수정")):
    #     content = command.replace("벙지 수정", "").replace("벙지수정", "").replace("벙수정", "").strip()

    #     if not content:
    #         messages_to_send.append({"type": "text", "text": "⚠️ 양식에 맞게 입력해주세요. 예) /벙지 수정 멤버 / 장소 / 벙제"})
    #     else:
    #         parts = [p.strip() for p in content.split("/")]
    #         if len(parts) >= 4:
    #             raw_date, members, location, title = parts[0], parts[1], parts[2], parts[3]
    #             date_str = parse_date_string(raw_date)
    #         elif len(parts) == 3:
    #             date_str = None
    #             members, location, title = parts[0], parts[1], parts[2]
    #         else:
    #             members = location = title = date_str = None

    #         if not members or not location or not title:
    #             messages_to_send.append({"type": "text", "text": "⚠️ 양식에 맞게 입력해주세요."})
    #         else:
    #             nickname = get_line_user_nickname(user_id, group_id)
    #             active_bungs = get_active_bungs(nickname)

    #             if not active_bungs:
    #                 messages_to_send.append({"type": "text", "text": "😢 현재 권한이 있는 진행/모집 중인 벙지가 없습니다."})
    #             elif len(active_bungs) == 1:
    #                 _, _, bung_sheet_ws, _ = get_eden_sheets()
    #                 row_num = active_bungs[0]["row"]
    #                 existing_date = active_bungs[0]["date"]
    #                 final_date = date_str if date_str else existing_date
    #                 status = active_bungs[0]["status"]
    #                 original_author = active_bungs[0]["author"]

    #                 bung_sheet_ws.update(
    #                     values=[[original_author, final_date, members, location, title, status]],
    #                     range_name=f"A{row_num}:F{row_num}"
    #                 )
    #                 supabase.table("meet_log").update({
    #                     "날짜": final_date,
    #                     "멤버": members,
    #                     "장소": location,
    #                     "벙제": title
    #                 }).eq("벙제", active_bungs[0]["title"]).execute()
    #                 messages_to_send.append({"type": "text", "text": f"✅ 벙지 내용이 수정되었습니다.\n- 벙제: {title}"})
    #                 messages_to_send.append(build_multi_mention_payload_v2(TROPHY_MEMBERS, "벙지수정!"))
    #             else:
    #                 state = {
    #                     "action": "edit",
    #                     "new_data": {"date": date_str, "members": members, "location": location, "title": title},
    #                     "bungs": active_bungs
    #                 }
    #                 save_bung_selection_state(user_id, state)
    #                 list_str = "\n".join([f"{i+1}. {b['title']} ({b['status']})" for i, b in enumerate(active_bungs)])
    #                 messages_to_send.append({"type": "text", "text": f"🤔 수정할 벙지 번호를 선택해주세요.\n\n{list_str}\n\n(취소하려면 '취소')"})

    # elif command in ["벙시작", "벙스타트", "벙시"]:
    #     nickname = get_line_user_nickname(user_id, group_id)
    #     active_bungs = get_active_bungs(nickname)

    #     if not active_bungs:
    #         messages_to_send.append({"type": "text", "text": "⚠️ 현재 시작할 수 있는 벙지가 없습니다."})
    #     elif len(active_bungs) == 1:
    #         _, _, bung_sheet_ws, _ = get_eden_sheets()
    #         bung_sheet_ws.update_acell(f"F{active_bungs[0]['row']}", "진행중")
    #         supabase.table("meet_log").update({"진행상황": "진행중"}).eq("벙제", active_bungs[0]["title"]).execute()
    #         check_and_mark_no_ttuk_female(active_bungs[0]["row"], active_bungs[0]["members"], active_bungs[0]["date"])
    #         messages_to_send.append({"type": "text", "text": f"🚀 '{active_bungs[0]['title']}' 시작!"})
    #         messages_to_send.append(build_multi_mention_payload_v2(TROPHY_MEMBERS, "벙시작!"))
    #     else:
    #         state = {"action": "start", "bungs": active_bungs}
    #         save_bung_selection_state(user_id, state)
    #         list_str = "\n".join([f"{i+1}. {b['title']}" for i, b in enumerate(active_bungs)])
    #         messages_to_send.append({"type": "text", "text": f"🤔 시작할 벙지 선택:\n\n{list_str}"})

    # elif command in ["벙종", "벙종료", "벙끝"]:
    #     nickname = get_line_user_nickname(user_id, group_id)
    #     active_bungs = get_active_bungs(nickname)

    #     if not active_bungs:
    #         messages_to_send.append({"type": "text", "text": "⚠️ 현재 종료할 수 있는 벙지가 없습니다."})
    #     elif len(active_bungs) == 1:
    #         _, _, bung_sheet_ws, _ = get_eden_sheets()
    #         bung_sheet_ws.update_acell(f"F{active_bungs[0]['row']}", "종료")
    #         supabase.table("meet_log").update({"진행상황": "종료"}).eq("벙제", active_bungs[0]["title"]).execute()
    #         messages_to_send.append({"type": "text", "text": f"🏁 '{active_bungs[0]['title']}' 종료!"})
    #         messages_to_send.append(build_multi_mention_payload_v2(TROPHY_MEMBERS, "벙종!"))
    #     else:
    #         state = {"action": "end", "bungs": active_bungs}
    #         save_bung_selection_state(user_id, state)
    #         list_str = "\n".join([f"{i+1}. {b['title']}" for i, b in enumerate(active_bungs)])
    #         messages_to_send.append({"type": "text", "text": f"🤔 종료할 벙지 선택:\n\n{list_str}"})
    # elif command in ["벙취소", "벙 취소", "벙지취소"]:
    #     nickname = get_line_user_nickname(user_id, group_id)
    #     active_bungs = get_active_bungs(nickname)

    #     if not active_bungs:
    #         messages_to_send.append({"type": "text", "text": "⚠️ 취소할 벙지가 없습니다."})
    #     elif len(active_bungs) == 1:
    #         _, _, bung_sheet_ws, _ = get_eden_sheets()
    #         bung_sheet_ws.update_acell(f"F{active_bungs[0]['row']}", "취소")
    #         supabase.table("meet_log").update({"진행상황": "취소"}).eq("벙제", active_bungs[0]["title"]).execute()
    #         messages_to_send.append({"type": "text", "text": f"🚫 '{active_bungs[0]['title']}' 취소됨"})
    #         messages_to_send.append(build_multi_mention_payload_v2(TROPHY_MEMBERS, "벙취소!"))
    #     else:
    #         state = {"action": "cancel", "bungs": active_bungs}
    #         save_bung_selection_state(user_id, state)
    #         list_str = "\n".join([f"{i+1}. {b['title']}" for i, b in enumerate(active_bungs)])
    #         messages_to_send.append({"type": "text", "text": f"🤔 취소할 벙지 선택:\n\n{list_str}"})
            
    elif command.startswith("봇") or command.startswith("챗봇"):
        messages_to_send.append({"type": "text", "text": "📢 이제 /봇 없이 '/공지 키워드', '/벙', '/마디' 로 바로 물어보면 더 빨라요! (이 명령어는 곧 사라질 예정)"})
        
    elif command in ["부방장", "방장"]:
        messages_to_send.append(build_multi_mention_payload_v2(TROPHY_MEMBERS))

    # elif command == "시트정리":
    #     ok, msg = compact_line_user_sheet()
    #     prefix = "✅ " if ok else "❌ "
    #     messages_to_send.append({"type": "text", "text": prefix + msg})

    elif command in ["아이디정리"]:
        ok, msg = cleanup_inactive_line_users()
        prefix = "✅ " if ok else "❌ "
        messages_to_send.append({"type": "text", "text": prefix + msg})

    # elif command in ["벙지확인", "벙지"]:
    #     messages_to_send.append(build_multi_mention_payload_v2(TROPHY_MEMBERS, "벙지확인!"))

    elif command in ["관리자", "관리진"]:
        messages_to_send.append(build_multi_mention_payload_v2(TROPHY_MEMBERS + PIN_MEMBERS, "여기까지가 관리자"))

    elif command in ["인증자"]:
        messages_to_send.append(build_multi_mention_payload_v2(BOOK_MEMBERS, "고생하는 인증자"))

    elif command in ["인증", "남초", "여초", "동반"]:
        kst_now_hour = datetime.now(timezone(timedelta(hours=9))).hour
        is_late_night = 0 <= kst_now_hour < 8

        # ✨ [추가됨] 관리자 시트/DB에는 남아있지만 실제로는 방을 나간 사람이
        # 멘션 대상에 섞여 있으면 LINE이 textV2 메시지 전체를 400으로 거부한다.
        # reply_line()의 사후 재시도(최대 10명)로도 못 걸러내는 경우(탈퇴자가
        # 10명 넘게 겹치는 등)까지 대비해, 애초에 "현재 방에 있는" 사람만
        # 멘션 대상에 넣도록 active_only 라인유저 목록으로 사전 필터링한다.
        try:
            active_user_map = get_line_user_map(active_only=True)
            active_ids = set(active_user_map.keys())
        except Exception as e:
            print(f"⚠️ 활성 유저 목록 조회 실패(필터링 생략): {e}")
            active_user_map = {}
            active_ids = None

        mention_targets = TROPHY_MEMBERS + PIN_MEMBERS + BOOK_MEMBERS
        if active_ids is not None:
            mention_targets = [uid for uid in mention_targets if uid in active_ids]

        if is_late_night:
            # ✨ [추가됨] 00시~08시(심야시간)에는 멘션 없이 닉네임만 나열해서
            # 알림이 울리지 않도록 한다. (기존에는 아예 요청 자체를 막았음)
            nicknames = []
            for uid in mention_targets:
                cached = str(active_user_map.get(uid, {}).get("nickname") or "").strip()
                nicknames.append(cached if cached else get_line_user_nickname(uid, group_id))

            text = ", ".join(nicknames) if nicknames else "지정된 대상자가 없습니다."
            text += "\n심야시간이라 멘션없이 요청합니다~"
            messages_to_send.append({"type": "text", "text": text})
        else:
            messages_to_send.append(build_multi_mention_payload_v2(mention_targets, "인증요청이요~!"))

    elif command in ["ㅁㄴㅂ", "매너봉", "ㅁㄴ", "매너", "봉"]:
        for _ in range(4):
            messages_to_send.append({"type": "sticker", "packageId": "446", "stickerId": "1988"})

    elif command == "내아이디":
        # ✨ 1:1 대화창에서 누구나(권한 무관) 자기 LINE 유저 아이디를 확인
        if source_type == "user":
            messages_to_send.append({"type": "text", "text": f"내 유저 ID: {user_id}"})
        else:
            messages_to_send.append({"type": "text", "text": "🙅 /내아이디는 봇과의 1:1 대화창에서만 사용할 수 있어요."})

    elif command in ["방아이디", "방id"]:
        if source_type == "group":
            messages_to_send.append({"type": "text", "text": f"이 그룹방의 ID: {group_id}"})
        else:
            messages_to_send.append({"type": "text", "text": "그룹방이 아닙니다."})

    elif command in ["운영진", "운영진목록", "운영진확인"]:
        admins = get_admin_members()
        OWNER_ID = "Ub847444353c5cca11c9febbdb0c8398f"  #방장
        def get_names(id_list):
            if not id_list:
                return "- 없음"
            return "\n".join([f"- {get_line_user_nickname(uid, group_id)}" for uid in id_list])

        trophy_ids = [uid for uid in admins.get("trophy", []) if uid != OWNER_ID]
        text_output = "👑 **운영진 목록** 👑\n\n"
        text_output += f"👑 **방장**\n- {get_line_user_nickname(OWNER_ID, group_id)}\n\n"
        text_output += f"🏆 **부방장**\n{get_names(trophy_ids)}\n\n"
        # text_output += f"📌 **관리자**\n{get_names(admins.get('pin', []))}\n\n"
        # text_output += f"📢 **홍보대장**\n{get_names(admins.get('promoter', []))}\n\n"
        text_output += f"📖 **인증자**\n{get_names(admins.get('book', []))}"

        messages_to_send.append({"type": "text", "text": text_output.strip()})

    elif command in ["공지갱신", "ㄱㅈ갱신", "공지리셋"]:
        ok, msg = sync_notice_to_db()
        if ok:
            messages_to_send.append({"type": "text", "text": "🔄 공지 DB 갱신 완료\n" + msg})
        else:
            messages_to_send.append({"type": "text", "text": "❌ 공지 갱신 실패\n" + msg})

    elif command in ["미등록갱신", "아이디갱신", "닉네임갱신", "미등록확인"]:
        target_group_id = resolve_sync_group_id(source_type, group_id)
        refresh_text = refresh_unregistered_users(target_group_id)
        if source_type != "group":
            refresh_text = f"(1:1 대화창 → 연결된 그룹방 {str(target_group_id)[:8]}… 기준으로 갱신)\n" + refresh_text
        messages_to_send.append({"type": "text", "text": refresh_text})

    elif command == "운영진갱신":
        ok, msg = sync_admins_to_db_from_sheet()
        if ok:
            messages_to_send.append({"type": "text", "text": "✅ 운영진 갱신 완료\n" + msg})
        else:
            messages_to_send.append({"type": "text", "text": "❌ 운영진 갱신 실패\n" + msg})

    elif command.startswith(("인사 ", "ㅇㅅ ")):
        # ✨ "/ㅇㅅ 모모" → 모모의 인사말 검색
        # ✨ "/ㅇㅅ 모모 민수" → 모모의 인사말을 검색해서, 민수를 멘션하고
        #    그 다음 줄부터 인사말을 보여준다.
        # ✨ "/ㅇㅅ 모모 / 민수 영희, 철수" → 모모의 인사말을 검색해서,
        #    민수·영희·철수를 한꺼번에 멘션하고 그 다음 줄부터 인사말을 보여준다.
        #    ("/" 뒤를 공백이나 ,로 구분하면 멘션 대상을 여러 명 지정할 수 있다)
        rest = command[3:].strip()

        if "/" in rest:
            target_part, mention_part = rest.split("/", 1)
            target = target_part.strip()
            mention_targets = [m for m in re.split(r"[,\s]+", mention_part.strip()) if m]
        else:
            parts = rest.split(maxsplit=1)
            target = parts[0] if parts else ""
            mention_targets = [parts[1].strip()] if len(parts) > 1 else []

        results = search_keyword(target)
        if not results:
            messages_to_send.append({"type": "text", "text": f"😢 '{target}'님을 찾지 못했습니다."})
        else:
            # 검색 결과가 1명이면 굳이 "닉네임: "을 안 붙여도 헷갈리지 않으니 생략하고,
            # 여러 명이 걸리면 누구 건지 구분해야 하니 그대로 붙인다.
            if len(results) == 1:
                greet_text = results[0]["인사말"]
            else:
                greet_text = "\n".join([f" {r['닉네임']}: {r['인사말']}" for r in results])

            if mention_targets:
                mention_user_ids = []
                not_found = []
                for mt in mention_targets:
                    uid = find_user_id_by_nickname_hangul(mt)
                    if uid:
                        mention_user_ids.append(uid)
                    else:
                        not_found.append(mt)

                if mention_user_ids:
                    messages_to_send.append(
                        build_multi_mention_payload_v2(mention_user_ids, greet_text)
                    )
                    if not_found:
                        messages_to_send.append({
                            "type": "text",
                            "text": f"😢 멘션할 대상 '{', '.join(not_found)}'님을 찾지 못했어요."
                        })
                else:
                    messages_to_send.append({
                        "type": "text",
                        "text": f"😢 멘션할 대상 '{', '.join(not_found)}'님을 찾지 못했어요.\n\n{greet_text}"
                    })
            else:
                messages_to_send.append({"type": "text", "text": greet_text})

    elif command in ["공지 키워드", "공지 사용법", "ㄱㅈ 키워드", "ㄱㅈ 사용법", "공지", "ㄱㅈ"]:
        messages_to_send.append({"type": "text", "text": get_all_notice_keywords()})

    elif command.startswith(("공지 ", "ㄱㅈ ")):
        search_query = command[3:].strip()
        if not search_query:
            messages_to_send.append({"type": "text", "text": get_all_notice_keywords()})
        else:
            # ✨ 직전에 되물어본 게 있으면, 이번 메시지를 그 답으로 먼저 이어붙여본다.
            pending_notice = get_pending_notice(user_id)
            combined_reply = None
            if pending_notice:
                combined_reply = resolve_notice_clarification(pending_notice, search_query)
                clear_pending_notice(user_id)

            if not combined_reply:
                # ✨ 먼저 여러 공지 항목을 조합해서 추론하는 경로를 시도한다.
                # ("노미클도 단벙 참석 가능하지?" 처럼 항목이 여러 개 얽힌 질문 대응)
                combined_reply = handle_notice_question(search_query, user_id)

            if combined_reply:
                messages_to_send.append({"type": "text", "text": combined_reply})
            else:
                # 폴백: 기존 단일 키워드 + 원문 그대로 보여주는 방식
                rows = query_notice_rows(search_query)
                if not rows:
                    # 직접 검색으로 못 찾으면 그때만 AI에게 키워드 추출을 맡긴다 (1회 호출)
                    smart_keyword = gemini_extract_notice_keyword(search_query)
                    if smart_keyword:
                        rows = query_notice_rows(smart_keyword)
                        search_query = smart_keyword
                reply_text = format_notice_rows_reply(search_query, rows)
                if rows:
                    # 원문은 그대로 두고, 이해하기 쉬운 설명을 추가로 덧붙인다 (1회 호출)
                    explain = gemini_explain_notice(search_query, rows)
                    if explain:
                        reply_text += f"\n\n💬 {explain}"
                messages_to_send.append({"type": "text", "text": reply_text})

    # elif command in ["벙", "ㅂ"] or command.startswith(("벙 ", "ㅂ ")):
    #     query_text = command[2:].strip() if command.startswith(("벙 ", "ㅂ ")) else ""
    #     nickname = get_line_user_nickname(user_id, group_id)
    #     tool_result = execute_gemini_tool("get_my_meets", {}, nickname)
    #     reply_text = tool_result.get("message", "벙 기록을 불러오지 못했어 🥲")
    #     if tool_result.get("success") and any(w in query_text for w in ["기록", "내역", "목록", "상세", "리스트"]):
    #         meets = tool_result.get("meets", [])

    #         # ✨ "/벙 리스트 이번달", "/벙 리스트 08.01~08.15" 처럼 기간이 같이 오면
    #         # 벙일정(날짜) 기준으로 그 기간에 해당하는 것만 추려서 보여준다.
    #         period = parse_meet_list_period(query_text)
    #         period_note = ""
    #         if period:
    #             p_start, p_end, p_label = period
    #             meets = [
    #                 m for m in meets
    #                 if p_start <= str(m.get("날짜") or "").split("~")[0].strip() <= p_end
    #             ]
    #             period_note = f" ({p_label})"

    #         # ✨ 최신 날짜가 위로 오도록 정렬 (날짜에 "~" 범위가 있으면 시작일 기준)
    #         meets = sorted(
    #             meets,
    #             key=lambda m: str(m.get("날짜") or "").split("~")[0].strip(),
    #             reverse=True,
    #         )

    #         if meets:
    #             lines = [f"- {m.get('날짜','')} {m.get('벙제','')} @ {m.get('장소','')}" for m in meets[:20]]
    #             reply_text += f"\n\n📋 벙지 리스트{period_note}\n" + "\n".join(lines)
    #             if len(meets) > 20:
    #                 reply_text += f"\n...외 {len(meets) - 20}건 더"
    #         elif period:
    #             reply_text += f"\n\n📋 {period[2]} 기간에는 벙 기록이 없어요."
    #     messages_to_send.append({"type": "text", "text": reply_text})

    elif (
        command in ["마디", "ㅁㄷ"]
        or command.startswith(("마디 ", "ㅁㄷ "))
        or "마디" in command
        or "ㅁㄷ" in command
    ):
        # "/마디 어제" 처럼 접두어가 붙어도, 접두어 없이 자연어로 와도
        # Gemini의 Function Calling이 문장을 이해해서 날짜/기간을 스스로 판단하게 한다.
        nickname = get_line_user_nickname(user_id, group_id)
        reply_text = get_smart_reply(nickname, command, user_id)
        messages_to_send.append({"type": "text", "text": reply_text})

    elif command in ["공지탭", "공지사항", "최신공지"]:
        messages_to_send.append({"type": "text", "text": get_notice_text()})

    elif command.startswith(("임시저장", "마디확인", "임시확인")):
        parts = user_message.split()
        if len(parts) >= 2:
            target_date = parts[1].strip()
        else:
            kst = timezone(timedelta(hours=9))
            target_date = datetime.now(kst).strftime("%Y-%m-%d")
        result = export_logs_to_sheet(target_date, temp=True)
        messages_to_send.append({"type": "text", "text": result})

    elif command == "정답" or command.startswith(("정답 ", "정답:")):
        # ============================================================
        # ✨ "/정답 <답>" : 방 안에서 누가 보내든, 그 방에서 지금
        # 진행 중인 퀴즈를 "낸 사람"이 답한 것처럼 처리한다.
        # (퀴즈 대화 history/quiz_mode는 여전히 문제를 낸 사람 기준으로
        #  저장/판정되고, 이 명령어는 그 사람에게 답만 대신 전달해준다.)
        # ============================================================
        answer_text = command[len("정답"):].strip(" :")

        if not answer_text:
            messages_to_send.append({
                "type": "text",
                "text": "🤔 '/정답 답내용' 형식으로 보내줘! (예: /정답 양파)"
            })
        else:
            quiz_target_id = get_quiz_owner(group_id) if group_id else user_id

            if not quiz_target_id:
                messages_to_send.append({
                    "type": "text",
                    "text": "🙅 지금 진행 중인 퀴즈가 없어! '퀴즈 내줘'라고 먼저 요청해봐~"
                })
            else:
                reply_ai = ask_gemini(quiz_target_id, answer_text)

                # 이 답으로 퀴즈가 끝났으면(quiz_mode=False) 방의 진행자 기록도 정리
                if group_id:
                    _, still_quiz, _, _ = get_user_state(quiz_target_id)
                    if not still_quiz:
                        clear_quiz_owner(group_id, quiz_target_id)

                messages_to_send.append({"type": "text", "text": reply_ai})

    else:
        # ============================================================
        # 기존 명령어가 없는 경우에만 이미지 키워드 검색
        # ============================================================

        image_url = get_image_url(command)

        if image_url:
            send_line_image(event.reply_token, image_url)
        else:
            # ========================================================
            # 접두어("공지 "/"ㄱㅈ ") 없이도 공지 DB를 1차로 빠르게 시도한다.
            #
            # ✨ 먼저 여러 공지 항목을 조합/추론해서 답하는 경로를 시도한다.
            # ("우리 노미클도 단벙 참석 가능하지?" 처럼 한 질문에 여러 공지
            #  항목(단벙 + 노미클 갠벙권 등)을 함께 참고해야 하는 경우 대응)
            # 여기서 답을 만들지 못하면(None) 조용히 아래 경로들로 폴백한다.
            #
            # ✨ 그보다 먼저, 직전에 봇이 되물어본 게 있으면 이번 메시지를
            # 그 되묻기에 대한 답으로 우선 이어붙여본다. (예: "동반초대로
            # 미클되나?" → 봇이 "동반으로 오신 건가요, 초대하신 건가요?"
            # 되물음 → 사용자가 "제가 초대한 거예요" 라고 답하면 그걸 반영)
            # ========================================================
            pending_notice = get_pending_notice(user_id)
            combined_reply = None
            if pending_notice:
                combined_reply = resolve_notice_clarification(pending_notice, command)
                clear_pending_notice(user_id)

            if not combined_reply:
                combined_reply = handle_notice_question(command, user_id)

            if combined_reply:
                messages_to_send.append({"type": "text", "text": combined_reply})
            else:
                # 폴백 1: LLM 호출 없이 규칙 기반 키워드 정제만 사용 (빠르고 저비용)
                simple_keyword = simple_extract_notice_keyword(command)
                notice_rows = query_notice_rows(simple_keyword) if simple_keyword else []

                if notice_rows:
                    reply_text = format_notice_rows_reply(simple_keyword, notice_rows)
                    # 결과를 찾았을 때만 자연스러운 설명을 덧붙임 (LLM 1회, 기존 /공지 경로와 동일)
                    explain = gemini_explain_notice(simple_keyword, notice_rows)
                    if explain:
                        reply_text += f"\n\n💬 {explain}"
                    messages_to_send.append({"type": "text", "text": reply_text})
                else:
                    # ========================================================
                    # 공지도 아니면 기존 Gemini 일반 대화 처리
                    # ========================================================

                    # user_message.split 대신 슬래시가 제거된 command를 그대로 전달합니다.
                    prompt = command.strip()

                    if not prompt:
                        messages_to_send.append({
                            "type": "text",
                            "text": "질문을 입력해주세요. (예: /안녕)"
                        })
                    else:
                        reply_ai = ask_gemini(user_id, prompt)

                        # ✨ 방(그룹)에서 퀴즈가 새로 시작되거나 계속되고 있으면
                        # "이 사람이 이 방의 퀴즈 진행자"라고 기록해서,
                        # 다른 사람이 "/정답"으로 답해도 이 사람 기준으로 채점되게 한다.
                        # 반대로 퀴즈가 끝났으면(그만하기 등) 기록을 정리한다.
                        if group_id:
                            _, quiz_mode_now, _, _ = get_user_state(user_id)
                            if quiz_mode_now:
                                set_quiz_owner(group_id, user_id)
                            else:
                                clear_quiz_owner(group_id, user_id)

                        messages_to_send.append({
                            "type": "text",
                            "text": reply_ai
                        })



    if messages_to_send:
        reply_line(event.reply_token, messages_to_send)
# ================================================================
# 프로그램 시작
# ================================================================

if __name__ == "__main__":

    # ------------------------------------------------------------
    # 글마디 자동 관리 시작
    #
    # 서버 시작 시:
    #   최근 7일 누락 날짜 자동 보충
    #
    # 서버 실행 중:
    #   매일 KST 00:00 전날 자동 백업
    # ------------------------------------------------------------
    start_daily_chatlog_archive()

    # ------------------------------------------------------------
    # Flask 서버 시작
    # ------------------------------------------------------------
    port = int(
        os.environ.get(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )

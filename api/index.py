import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import time
import random
import json
import gspread
import datetime
import threading
import requests
from contextlib import contextmanager
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, abort

# Supabase 클라이언트 라이브러리
from supabase import create_client, Client

# Line SDK v3 컴포넌트
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, ReplyMessageRequest, TextMessage, PushMessageRequest
)
# ✨ [추가됨] 인증방 전체 멘션(@전체) 완료 알림용. 설치된 line-bot-sdk 버전이 텍스트 메시지 v2(멘션)를
# 지원하지 않는 경우를 대비해 임포트 실패 시에도 나머지 기능은 정상 동작하도록 예외처리합니다.
try:
    from linebot.v3.messaging import TextMessageV2, MentionSubstitutionObject, AllMentionTarget
    _MENTION_SUPPORTED = True
except ImportError:
    _MENTION_SUPPORTED = False
    print("⚠️ 설치된 line-bot-sdk 버전이 텍스트 메시지 v2(멘션)를 지원하지 않아, 완료 알림 시 멘션 없이 일반 텍스트로 전송됩니다.")
from linebot.v3.webhooks import (
    MessageEvent, TextMessageContent, MemberJoinedEvent, MemberLeftEvent, AudioMessageContent
)

# ✨ [추가됨] 음성 자동분석(성별 추정 / 블랙리스트 화자 유사도 검색) 모듈
from voice_analysis import submit_voice_analysis_job

app = Flask(__name__)


def warmup_voice_service():
    """신입 입장 시 Cloud Run 음성분석 서비스를 미리 깨워둔다 (콜드스타트 완화용).
    응답을 기다리지 않고 짧은 타임아웃으로 요청만 보내고, 실패/타임아웃 나도 무시한다.
    (요청이 타임아웃 나더라도 Cloud Run 쪽 컨테이너 기동은 이미 시작됨)"""
    voice_service_url = os.environ.get("VOICE_SERVICE_URL", "")
    if not voice_service_url:
        return
    try:
        requests.get(f"{voice_service_url.rstrip('/')}/healthz", timeout=3)
    except Exception:
        pass


# 인증자방(관리자 그룹방) ID 고정 설정
ADMIN_GROUP_CHAT_ID = "C1fdb3b771a6bd0686fa7dbf1b5145a70"

# 환경변수 설정 및 핸들러 초기화
configuration = Configuration(access_token=os.environ.get("LINE_CHANNEL_ACCESS_TOKEN"))
handler = WebhookHandler(os.environ.get("LINE_CHANNEL_SECRET"))

# 구글 서비스 계정 인증
json_key_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
json_key_dict = json.loads(json_key_str) if json_key_str else {}
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key_dict, scope) if json_key_dict else None
client = gspread.authorize(creds) if creds else None

# [Supabase 클라이언트 초기화]
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")

if supabase_url and supabase_key:
    supabase: Client = create_client(supabase_url, supabase_key)
else:
    supabase = None
    print("⚠️ Supabase 환경변수가 설정되지 않아 메모리 폴백으로 작동합니다.")

# 메모리 폴백(Fallback) 변수
notified_users = {}
room_states_memory = {}

# 스레드 락
local_sheet_lock = threading.Lock()

# 구글 스프레드시트 연결
GOOGLE_SHEET_ID = "1ZOBZX7tZvEsDmIU1QwIlg54mqaRAVKj1aMOy_zIpFYg"

if client:
    try:
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        sheet = spreadsheet.worksheet("멘트")
        validation_sheet = spreadsheet.worksheet("검증")
        room_manage_sheet = spreadsheet.worksheet("방관리")
    except Exception as e:
        print(f"⚠️ 시트 연결 중 일부 실패: {e}")
        room_manage_sheet = None
else:
    sheet = validation_sheet = room_manage_sheet = None


# ==========================================
# [Supabase 1000개 이상 데이터 처리용 헬퍼 함수]
# ==========================================
def get_all_supabase_data(table_name, select_query="*"):
    """Supabase에서 1000개 이상의 데이터를 모두 가져오는 함수 (페이지네이션 적용)"""
    if not supabase:
        return []
    
    all_data = []
    limit = 1000
    offset = 0
    
    while True:
        try:
            res = supabase.table(table_name).select(select_query).range(offset, offset + limit - 1).execute()
            data = res.data if res.data else []
            all_data.extend(data)
            
            # 받아온 데이터가 limit보다 적으면 모든 데이터를 다 가져온 것
            if len(data) < limit:
                break
            offset += limit
        except Exception as e:
            print(f"{table_name} 테이블 조회 에러: {e}")
            break
            
    return all_data

def process_in_chunks(table_name, data_list, chunk_size=500, is_insert=False):
    """1000개 이상 데이터 삽입/업데이트 시 발생하는 에러를 방지하는 청크(분할) 처리 헬퍼 함수"""
    if not supabase or not data_list:
        return
    for i in range(0, len(data_list), chunk_size):
        chunk = data_list[i:i + chunk_size]
        try:
            if is_insert:
                supabase.table(table_name).insert(chunk).execute()
            else:
                supabase.table(table_name).upsert(chunk).execute()
        except Exception as e:
            print(f"{table_name} 테이블 데이터 분할 처리 에러 ({i}~{i+chunk_size}): {e}")


def supabase_execute(query_fn, retries=2, delay=0.4, default=None, label=""):
    """Supabase 쿼리 실행 시 504 Gateway Timeout 등 일시적 에러에 대해 짧게 재시도하는 헬퍼.

    query_fn: 인자 없이 호출하면 .execute()까지 실행하는 콜러블
              예) lambda: supabase.table('user_validations').select('status').eq('user_id', uid).execute()
    실패 시 default를 반환하고, 호출부에서는 반환값이 None/default인지 확인해서 처리하면 됨.
    """
    if not supabase:
        return default
    last_err = None
    for attempt in range(retries + 1):
        try:
            return query_fn()
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(delay)
    print(f"Supabase 쿼리 재시도 실패{f' ({label})' if label else ''}: {last_err}")
    return default


def cas_update_status(user_id, from_status, to_status, label=""):
    """user_validations.status를 from_status일 때만 to_status로 '원자적으로' 전환합니다.

    ✨ [추가됨] 음성 자동분석과 신입의 '문제없음' 답장은 서로 다른 요청(타이밍)으로 들어오고
    어느 쪽이 먼저 끝날지 알 수 없습니다. update(...).eq('status', from_status)로 조건부 업데이트를
    걸면, 두 요청이 동시에 들어와도 실제로 status가 from_status였던 '딱 한쪽'만 성공하는 간단한
    CAS(compare-and-swap) 락 역할을 합니다.

    반환값: 성공(=이 요청이 전환을 해낸 쪽)이면 True, 이미 다른 상태로 바뀌어 있어(=다른 요청이
    먼저 처리함) 조건이 맞지 않으면 False.
    """
    if not supabase or not user_id:
        return False
    res = supabase_execute(
        lambda: supabase.table('user_validations')
            .update({"status": to_status})
            .eq('user_id', user_id)
            .eq('status', from_status)
            .execute(),
        label=label or f"상태 전환 CAS({from_status}->{to_status})"
    )
    return bool(res and res.data)


def sync_status_to_sheet(user_id, status_text):
    """구글 시트(검증 탭) L열의 상태값만 별도로 동기화합니다. 실패해도 무시합니다."""
    try:
        with sheet_sync_lock():
            if validation_sheet:
                raw_user_ids = validation_sheet.col_values(5)
                clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                if user_id in clean_user_ids:
                    row_index = clean_user_ids.index(user_id) + 1
                    validation_sheet.update(range_name=f'L{row_index}', values=[[status_text]])
    except Exception as e:
        print(f"상태 시트 동기화 에러({status_text}): {e}")


# 음성 확인이 모두 끝났을 때(자동분석 완료 + 신입의 '문제없음' 확인) 보내는 최종 안내 문구.
# analyze_new_member_voice가 끝난 시점과 신입이 '문제없음'이라고 답장한 시점 중
# '나중에' 완료되는 쪽에서 이 문구를 전송합니다.
FINAL_APPROVAL_WAIT_TEXT = (
    "✅ 음성 확인까지 모두 완료되었습니다!\n\n"
    "기본 인증과 음성인증 결과를 운영진이 확인 후 안내해 드릴 예정이니 잠시만 기다려 주세요."
)


# ==========================================
# [동시성 제어 - Supabase 분산 락]
# ==========================================
@contextmanager
def sheet_sync_lock(timeout=10, wait_time=7):
    acquired = False
    lock_name = "sheet_write"
    
    with local_sheet_lock:
        if supabase:
            end_time = time.time() + wait_time
            while time.time() < end_time:
                try:
                    now = datetime.datetime.now(datetime.timezone.utc)
                    res = supabase.table('app_locks').select('locked_until').eq('lock_name', lock_name).execute()
                    
                    can_lock = False
                    if not res.data:
                        can_lock = True
                    else:
                        locked_until_str = res.data[0].get('locked_until')
                        if locked_until_str:
                            locked_until = datetime.datetime.fromisoformat(locked_until_str.replace("Z", "+00:00"))
                            if now > locked_until:
                                can_lock = True
                        else:
                            can_lock = True

                    if can_lock:
                        new_locked_until = now + datetime.timedelta(seconds=timeout)
                        supabase.table('app_locks').upsert({
                            "lock_name": lock_name, 
                            "locked_until": new_locked_until.isoformat()
                        }).execute()
                        acquired = True
                        break
                except Exception as e:
                    print(f"Lock error: {e}")
                time.sleep(0.25)
        try:
            yield
        finally:
            if acquired and supabase:
                try:
                    supabase.table('app_locks').delete().eq('lock_name', lock_name).execute()
                except Exception:
                    pass


# ==========================================
# [Supabase 유저/방 세션 관리 및 신입 검증 함수]
# ==========================================
def set_user_session(user_id, data, ttl=3600):
    if supabase:
        try:
            supabase.table('user_sessions').upsert({"user_id": user_id, "data": data}).execute()
            return
        except Exception as e:
            print(f"user_session 저장 에러: {e}")
    notified_users[user_id] = data

def get_user_session(user_id):
    if supabase:
        try:
            res = supabase.table('user_sessions').select('data').eq('user_id', user_id).execute()
            if res.data:
                return res.data[0]['data']
        except Exception as e:
            print(f"user_session 조회 에러: {e}")
    return notified_users.get(user_id)

def del_user_session(user_id):
    if supabase:
        try:
            supabase.table('user_sessions').delete().eq('user_id', user_id).execute()
        except Exception as e:
            print(f"user_session 삭제 에러: {e}")
    notified_users.pop(user_id, None)

def set_room_state(room_id, data, ttl=3600):
    if supabase:
        try:
            supabase.table('room_states').upsert({"room_id": room_id, "data": data}).execute()
            return
        except Exception as e:
            print(f"room_state 저장 에러: {e}")
    room_states_memory[room_id] = data

def get_room_state(room_id):
    if supabase:
        try:
            res = supabase.table('room_states').select('data').eq('room_id', room_id).execute()
            if res.data:
                return res.data[0]['data']
        except Exception as e:
            print(f"room_state 조회 에러: {e}")
    return room_states_memory.get(room_id)

def del_room_state(room_id):
    if supabase:
        try:
            supabase.table('room_states').delete().eq('room_id', room_id).execute()
        except Exception as e:
            print(f"room_state 삭제 에러: {e}")
    room_states_memory.pop(room_id, None)


def reset_verification_state(source_id, target_user_id=None):
    """해당 방의 인증 진행 상태(room_state, 세션)만 초기화합니다. (신입 유저 퇴장(MemberLeftEvent)에서 사용)
    - user_validations의 status는 더 이상 건드리지 않습니다. 재입장 시 마지막 status를 기준으로
      대화가 어디서 끊겼는지 안내하는 기능(send_join_welcome)이 이 값을 그대로 활용합니다.
    - target_user_id를 명시하지 않으면 현재 room_state에 기록된 유저를 대상으로 합니다.
    - 반환값: 실제로 초기화된 user_id (없으면 None)
    """
    room_state = get_room_state(source_id)
    if target_user_id is None:
        target_user_id = room_state.get('user_id') if room_state else None

    if target_user_id:
        del_user_session(target_user_id)

    del_room_state(source_id)
    return target_user_id

def complete_verification_state(source_id, target_user_id=None):
    """관리자가 '/ㅇㅈ 퇴장' 또는 '/ㅇㅈ ㅌㅈ'을 입력했을 때, 진행 중이던 인증 상태를 '완료'로 종료합니다.
    - target_user_id를 명시하지 않으면 현재 room_state에 기록된 유저를 대상으로 합니다.
    - 반환값: 실제로 완료 처리된 user_id (없으면 None)
    """
    room_state = get_room_state(source_id)
    if target_user_id is None:
        target_user_id = room_state.get('user_id') if room_state else None

    if target_user_id:
        del_user_session(target_user_id)
        try:
            if supabase:
                supabase.table('user_validations').update({"status": "완료"}).eq('user_id', target_user_id).execute()

            with sheet_sync_lock():
                if validation_sheet:
                    raw_user_ids = validation_sheet.col_values(5)
                    clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                    if target_user_id in clean_user_ids:
                        row_index = clean_user_ids.index(target_user_id) + 1
                        validation_sheet.update(range_name=f'L{row_index}', values=[["완료"]])
        except Exception as e:
            print(f"유저 상태 완료 처리 중 에러: {e}")

        del_room_state(source_id)

    return target_user_id

def is_last_joined_user(source_id, user_id):
    """그룹방에서 메시지를 보낸 유저가 가장 최근에 입장한 유저인지 검증합니다."""
    if source_id == user_id:
        return True  # 1:1 개인 대화는 통과

    room_state = get_room_state(source_id)
    if room_state and room_state.get('user_id'):
        return room_state.get('user_id') == user_id
    
    return True


def get_room_target_user_id(source_id):
    """해당 방에서 현재 인증이 진행 중으로 추적되고 있는 유저의 user_id를 반환합니다."""
    room_state = get_room_state(source_id)
    return room_state.get('user_id') if room_state else None


# ==========================================
# [DB 전용 조회 함수]
# ==========================================
def get_room_id_by_name(room_name):
    if supabase:
        try:
            target_name = room_name.replace(" ", "")
            res = supabase.table('room_management').select('room_id').eq('room_name', target_name).execute()
            if res.data:
                return res.data[0]['room_id']
        except Exception as e:
            print(f"방 DB 조회 실패: {e}")
    return None


# ==========================================
# ✨ [추가됨] 구글시트 → DB 동기화용 안전장치
# ==========================================
def find_header_col_index(header_row, keywords):
    """헤더 행(1행)에서 keywords 중 하나라도 포함된 첫 번째 열의 인덱스(0-based)를 찾습니다.
    관리자가 '검증' 시트에 열을 삽입/삭제/이동해도, 위치가 아니라 '이름'으로 찾기 때문에
    엉뚱한 열의 값이 잘못된 필드에 저장되는 사고를 막기 위한 안전장치입니다.
    못 찾으면 None을 반환합니다.
    """
    for idx, cell in enumerate(header_row):
        cell_clean = str(cell).replace(" ", "")
        for kw in keywords:
            if kw in cell_clean:
                return idx
    return None


def build_validation_sheet_col_map(header_row):
    """'검증' 시트 헤더 행을 보고 각 필드가 몇 번째 열인지 찾아 dict로 돌려줍니다.
    user_id 열을 못 찾으면 동기화 자체가 불가능하므로 None을 반환해서 호출부가
    (엉뚱한 데이터를 쓰는 대신) 동기화를 안전하게 중단하도록 합니다.
    """
    col_map = {
        "user_id": find_header_col_index(header_row, ["아이디", "ID", "유저", "id"]),
        "nickname": find_header_col_index(header_row, ["닉네임"]),
        "gender": find_header_col_index(header_row, ["성별"]),
        "region": find_header_col_index(header_row, ["지역"]),
        "birth_year": find_header_col_index(header_row, ["년생", "생년"]),
        "entry_date": find_header_col_index(header_row, ["입장"]),
        "black_reason": find_header_col_index(header_row, ["블랙"]),
        "retry_count": find_header_col_index(header_row, ["재시도", "횟수"]),
        "status": find_header_col_index(header_row, ["상태"]),
    }
    if col_map["user_id"] is None:
        return None
    return col_map


def get_cell(row, idx, default=""):
    """row[idx]를 안전하게 꺼냅니다. idx가 None이거나 행이 그 길이만큼 없으면 default."""
    if idx is None or len(row) <= idx:
        return default
    return str(row[idx]).strip()

def get_recording_ments():
    if supabase:
        try:
            # 1000개 이상 데이터 안전 조회를 위해 get_all_supabase_data 사용
            all_ments = get_all_supabase_data('recording_ments', 'gender, ment')
            col_male = [row['ment'] for row in all_ments if row['gender'] == 'male']
            col_female = [row['ment'] for row in all_ments if row['gender'] == 'female']
            return col_male, col_female
        except Exception as e:
            print(f"녹음 멘트 DB 조회 실패: {e}")
    return [], []

def search_keyword(keyword):
    if supabase:
        try:
            res = supabase.table('auth_ments').select('reply_text').eq('keyword', str(keyword).strip()).execute()
            if res.data:
                return res.data[0]['reply_text']
        except Exception as e:
            print(f"키워드 DB 검색 오류: {e}")
    return None

def list_all_keywords():
    """등록된 인증 멘트 키워드를 전부 조회합니다. ('/인증 목록', '/ㅇㅈ 목록' 명령어용)"""
    if supabase:
        try:
            all_ments = get_all_supabase_data('auth_ments', 'keyword')
            keywords = sorted({str(row.get('keyword', '')).strip() for row in all_ments if row.get('keyword')})
            return keywords
        except Exception as e:
            print(f"키워드 목록 조회 오류: {e}")
    return []


def start_voice_auth(user_id, require_status='입장대기'):
    """대상 유저를 음성인증 대기 상태로 전환하고 음성인증 안내 멘트를 만들어 돌려줍니다.
    - require_status를 지정하면 유저의 현재 status가 그 값일 때만 진행합니다. (신입의 '확인' 자동 흐름용)
    - require_status=None이면 현재 status와 무관하게 강제로 진행합니다. (관리자 수동 트리거용: /ㅇㅈ 음성인증, /ㅇㅅㅇㅈ)
    반환값: (성공여부, 안내 멘트 또는 None)
    """
    if not supabase or not user_id:
        return False, None

    try:
        u_res = supabase.table('user_validations').select('*').eq('user_id', user_id).execute()
    except Exception as e:
        print(f"음성인증 시작 - 유저 조회 에러: {e}")
        return False, None

    if not u_res.data:
        return False, None

    u_data = u_res.data[0]
    if require_status is not None and u_data.get('status') != require_status:
        return False, None

    # ✨ [추가됨] 콜드스타트 완화용 웜업 호출 시점을 "입장" 시점에서 "음성인증 시작(확인 답장)"
    # 시점으로 옮김 — 신입이 실제로 녹음해서 보내기까지 걸리는 시간이 자연스러운 버퍼가 되어,
    # '문제없음' 답장 때 크라우드런을 호출할 즈음엔 이미 웜업이 끝나 있을 확률이 높다.
    threading.Thread(target=warmup_voice_service, daemon=True).start()

    user_nickname = u_data.get('nickname', '신입')
    user_gender = u_data.get('gender', '')
    details = u_data.get('details') or {}
    inviter = details.get('inviter', '없음')

    col_male, col_female = get_recording_ments()
    rec_ment = ""
    if user_gender in ["남", "남자"] and col_male:
        rec_ment = random.choice(col_male)
    elif user_gender in ["여", "여자"] and col_female:
        rec_ment = random.choice(col_female)

    if not rec_ment:
        rec_ment = "잘 부탁드립니다."

    # 한국 시간(KST) 기준 오늘 날짜 계산
    kst = datetime.timezone(datetime.timedelta(hours=9))
    today = datetime.datetime.now(kst)
    date_str = f"{today.month}월 {today.day}일"

    reply_text = (
        f"⭕️ 작성이 완료되었다면 음성인증을 진행합니다.\n\n"
        f"키보드 상단 음성메시지를 활용해서 진행합니다.\n\n"
        f"아래 문구를 정확하게 읽어주세요.\n\n"
        f"\"제 닉네임은 {user_nickname}입니다. 오늘은 {date_str}, 초대자 {inviter}입니다. {rec_ment}\"\n\n"
        f"조용한 곳에서 천천히 또박또박 부탁드립니다."
    )

    try:
        supabase.table('user_validations').update({"status": "음성대기"}).eq('user_id', user_id).execute()
    except Exception as e:
        print(f"음성인증 시작 - user_validations 상태 업데이트 에러: {e}")

    # 시트 동기화는 부가 기능(백업)이므로, 여기서 실패해도 안내 멘트 전송에는 영향이 없도록 분리합니다.
    try:
        with sheet_sync_lock():
            if validation_sheet:
                raw_user_ids = validation_sheet.col_values(5)
                clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                if user_id in clean_user_ids:
                    row_index = clean_user_ids.index(user_id) + 1
                    validation_sheet.update(range_name=f'L{row_index}', values=[["음성대기"]])
    except Exception as e:
        print(f"음성인증 시작 - 시트 동기화 에러(무시하고 계속 진행): {e}")

    return True, reply_text


# ==========================================
# ✨ [추가됨] "닉변" → "변경완료" → 헤르페스 확인 플로우용 헬퍼 함수
# ==========================================
def build_all_mention_message(text_body):
    """방 전체(@전체)를 멘션하면서 안내 문구를 붙인 메시지 객체를 만듭니다.
    설치된 line-bot-sdk 버전이 텍스트 메시지 v2(멘션)를 지원하지 않으면 멘션 없이 일반 텍스트로 대체합니다."""
    if _MENTION_SUPPORTED:
        try:
            return TextMessageV2(
                text="{전체} " + text_body,
                substitution={
                    "전체": MentionSubstitutionObject(
                        type="mention",
                        mentionee=AllMentionTarget(type="all")
                    )
                }
            )
        except Exception as e:
            print(f"⚠️ 멘션 메시지 생성 실패(일반 텍스트로 대체): {e}")
    return TextMessage(text=text_body)


def get_validation_row(user_id, fields="*"):
    """user_validations에서 지정한 컬럼만 조회합니다. 실패/미존재 시 빈 dict."""
    if not supabase or not user_id:
        return {}
    res = supabase_execute(
        lambda: supabase.table('user_validations').select(fields).eq('user_id', user_id).execute(),
        label="유저 검증정보 조회"
    )
    return (res.data[0] if res and res.data else {}) or {}


def get_current_display_name(source_id, user_id):
    """신입의 현재 LINE 그룹 표시 닉네임을 조회합니다. 조회 실패 시 빈 문자열."""
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            profile = line_bot_api.get_group_member_profile(source_id, user_id)
            return getattr(profile, 'display_name', '') or ''
    except Exception as e:
        print(f"닉네임 변경 확인 - 프로필 조회 실패: {e}")
        return ''


def is_nickname_changed_correctly(source_id, user_id, birth_year, nickname):
    """실제 LINE 닉네임에 신청 시 적어낸 '생년'과 '닉네임'이 모두 반영되었는지 확인합니다.
    (공백 유무 등 사소한 차이는 무시하고 두 값이 포함되어 있는지만 느슨하게 검사합니다.)"""
    current_name = get_current_display_name(source_id, user_id)
    if not current_name or not birth_year or not nickname:
        return False
    current_clean = current_name.replace(" ", "")
    return (str(birth_year).strip() in current_clean) and (str(nickname).strip() in current_clean)


def has_blacklist_issue(user_id):
    """DB에 저장된 음성 자동분석 결과 기준으로 블랙리스트 유사 매칭(강한 의심)이 남아있는지 재확인합니다."""
    row = get_validation_row(user_id, "voice_analysis_result")
    voice_result = row.get('voice_analysis_result') or {}
    matches = voice_result.get('matches') or []
    return any((m.get('similarity') or 0) >= VOICE_MATCH_ALERT_THRESHOLD for m in matches)


def finalize_after_nickname_check(source_id, user_id, reply_token):
    """'헤르페스확인대기' 상태에서 (헤르페스 무증상 답변, 또는 닉네임 재수정 후 '확인')이 왔을 때
    실제 LINE 닉네임 변경 여부와 블랙리스트 재조회 결과를 확인해 최종 분기 처리합니다.

    - 닉네임 미변경          -> 닉네임/프사 재설정 요청 후 대기 (상태는 '헤르페스확인대기' 유지)
    - 닉네임 변경 + 블랙리스트 문제 없음 -> '/ㅇㅈ 퇴장' 멘트 출력, 상태를 '퇴장대기'로 전환
      (실제 퇴장 시 handle_member_left에서 운영진에게 완료 알림)
    - 닉네임 변경 + 블랙리스트 문제 있음 -> 운영진 확인 대기 안내, 상태를 '관리자검토대기'로 전환
    """
    row = get_validation_row(user_id, "nickname, birth_year")
    nickname = row.get('nickname') or ""
    birth_year = row.get('birth_year') or ""

    if not is_nickname_changed_correctly(source_id, user_id, birth_year, nickname):
        reply_text = "닉네임을 변경하여 주시고 프사를 설정하여 주세요. 그리고 다시 확인이라고 입력하여 주세요."
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(reply_token=reply_token, messages=[TextMessage(text=reply_text)])
            )
        return

    if has_blacklist_issue(user_id):
        if supabase:
            supabase_execute(
                lambda: supabase.table('user_validations').update({"status": "관리자검토대기"}).eq('user_id', user_id).execute(),
                label="관리자검토대기 상태 전환"
            )
        sync_status_to_sheet(user_id, "관리자검토대기")

        reply_text = "인증자의 확인 후 인증과정이 마무리 될 예정이니 잠시대기하여주세요."
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(reply_token=reply_token, messages=[TextMessage(text=reply_text)])
            )
        return

    # 닉네임 변경 확인 + 블랙리스트 문제 없음 -> 퇴장 안내, 실제 퇴장 시(handle_member_left) 운영진에게 완료 알림
    if supabase:
        supabase_execute(
            lambda: supabase.table('user_validations').update({"status": "퇴장대기"}).eq('user_id', user_id).execute(),
            label="퇴장대기 상태 전환"
        )
    sync_status_to_sheet(user_id, "퇴장대기")

    exit_text = search_keyword("퇴장") or search_keyword("ㅌㅈ") or "✅ 인증이 모두 완료되었습니다. 안내에 따라 방을 나가주세요."
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(reply_token=reply_token, messages=[TextMessage(text=exit_text)])
        )


@app.route("/api", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature')
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    except Exception as e:
        # 예상 못한 에러(예: Supabase 일시 장애)로 500이 나가면 LINE이 재시도를 반복하며
        # reply_token 만료 등으로 결국 유저가 응답을 못 받게 되므로, 로그만 남기고 200으로 응답합니다.
        print(f"⚠️ 콜백 처리 중 예외 발생(무시하고 200 응답): {e}")
    return 'OK'


# ==========================================
# [핸들러 1] 일반 유저 및 관리자 명령어 처리 핸들러
# ==========================================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id
    if not user_id:
        return
    source_id = getattr(event.source, 'group_id', getattr(event.source, 'room_id', event.source.user_id))
    user_message = event.message.text.strip()
    reply_text = ""

    # ✨ [추가됨] 인증자방(ADMIN_GROUP_CHAT_ID)에서는 신입 검증 플로우
    # ("." 초기화, 1번 양식 제출, "확인" 답장 처리)를 전혀 실행/기록하지 않는다.
    # 이 방에서는 관리진 명령어(/인증, /ㅇㅈ, /디비업데이트, /O번방 확인 등)와
    # 그 외 슬래시(/)로 시작하는 일반 명령어(DB 키워드로 등록된 멘트 포함)만 동작한다.
    is_admin_room = (source_id == ADMIN_GROUP_CHAT_ID)

    # 관리자 명령어('.', '/')가 아닌 일반 채팅에 한해 검증을 진행합니다.
    if not (user_message == "." or user_message.startswith("/")):
        if not is_last_joined_user(source_id, user_id):
            return

    # 📌 [친구추가 후 재시작] 프로필 조회 실패로 '친구추가+시작'을 안내받은 유저가
    # 실제로 친구추가 후 '시작'을 입력하면, 최초 입장 시 로직(1번 환영 멘트)부터 다시 진행합니다.
    if not is_admin_room and user_message == "시작":
        room_state = get_room_state(source_id)
        if room_state and room_state.get('user_id') == user_id and room_state.get('status') == 'pending_friend':
            send_join_welcome(source_id, user_id, event.reply_token)
            return

    # 0. 점(.) 입력 시 해당 방의 인증 진행 상태(room_state)만 초기화 (DB status는 변경하지 않음)
    if not is_admin_room and user_message == ".":
        room_state = get_room_state(source_id)
        tracked_user_id = room_state.get('user_id') if room_state else None
        if tracked_user_id:
            del_user_session(tracked_user_id)
        del_room_state(source_id)

        reply_text = "🔄 해당 방의 인증 진행 상태가 초기화되었습니다.\n신입 유저는 양식을 처음부터 다시 작성해 주세요!"
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=event.reply_token, 
                    messages=[TextMessage(text=reply_text)]
                )
            )
        return

    # 슬래시(/) 명령어 로직
    if user_message.startswith("/"):
        command = user_message[1:].strip()
        parts = command.split(maxsplit=1)
        cmd_prefix = parts[0]

        # 음성인증 멘트 수동 발송: /인증 음성인증, /ㅇㅈ 음성인증, /ㅇㅅㅇㅈ
        # (자동으로 "확인" 답장을 받아 진행되지 않았거나, 관리자가 직접 재발송해야 할 때 사용)
        voice_manual_trigger = (
            cmd_prefix == "ㅇㅅㅇㅈ"
            or (cmd_prefix in ("인증", "ㅇㅈ") and len(parts) >= 2 and parts[1].strip() in ("음성인증", "음성", "ㅇㅅㅇㅈ"))
        )
        if voice_manual_trigger:
            voice_target_id = get_room_target_user_id(source_id)
            if voice_target_id:
                v_success, v_reply_text = start_voice_auth(voice_target_id, require_status=None)
                reply_text = v_reply_text if v_success else "❌ 해당 유저의 인증 정보를 DB에서 찾을 수 없어 음성인증 멘트를 보낼 수 없습니다."
            else:
                reply_text = "❌ 현재 이 방에서 인증 진행 중인 신입 유저를 찾을 수 없습니다."

            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
            return

        # 진행 중이던 인증을 완료 처리: /인증 퇴장, /ㅇㅈ 퇴장, /인증 ㅌㅈ, /ㅇㅈ ㅌㅈ
        complete_trigger = (
            cmd_prefix in ("인증", "ㅇㅈ") and len(parts) >= 2 and parts[1].strip() in ("퇴장", "ㅌㅈ")
        )
        if complete_trigger:
            completed_user_id = complete_verification_state(source_id)
            if completed_user_id:
                reply_text = search_keyword("퇴장") or search_keyword("ㅌㅈ") or "✅ 인증 상태를 완료로 변경했습니다."
            else:
                reply_text = "❌ 현재 이 방에서 인증 진행 중인 신입 유저를 찾을 수 없습니다."

            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
            return

        if cmd_prefix in ("인증", "ㅇㅈ"):
            if len(parts) < 2 or not parts[1].strip():
                reply_text = "사용법: /인증 [키워드] 형태로 입력해 주세요.\n전체 키워드 목록은 /인증 목록, 음성인증 멘트 수동 발송은 /인증 음성인증(또는 /ㅇㅅㅇㅈ) 으로 확인할 수 있습니다."
            else:
                keyword = parts[1].strip()
                if keyword in ("목록", "리스트", "list", "List"):
                    keywords = list_all_keywords()
                    if keywords:
                        keyword_list_str = ", ".join(keywords)
                        reply_text = f"📋 등록된 인증 키워드 목록 ({len(keywords)}개)\n\n{keyword_list_str}"
                        if len(reply_text) > 4900:
                            reply_text = reply_text[:4900] + "\n...(이하 생략)"
                    else:
                        reply_text = "📋 등록된 인증 키워드가 없습니다."
                else:
                    res_text = search_keyword(keyword)
                    reply_text = res_text if res_text else f"'{keyword}'에 해당하는 인증 멘트를 찾을 수 없습니다."

            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
            return

    # [관리자 전용 명령어]
    if user_message.startswith("/") and source_id == ADMIN_GROUP_CHAT_ID:
        command_body = user_message[1:].strip()

        # 1) /디비업데이트 명령어 (1000개 이상 데이터 대응 완료)
        if "디비업데이트" in command_body:
            try:
                sync_reports = []

                # A. '멘트' 시트 동기화 (전체삭제 대신 차이만 반영 - 타임아웃 방지)
                if sheet:
                    ments_data = sheet.get_all_records()
                    ments_records = []
                    for row in ments_data:
                        k_raw = str(row.get('인증', '')).strip()
                        v_text = str(row.get('출력', '')).strip()
                        if k_raw and v_text:
                            for k in [x.strip() for x in k_raw.split(',') if x.strip()]:
                                ments_records.append({"keyword": k, "reply_text": v_text})

                    if ments_records and supabase:
                        new_keywords = {r["keyword"] for r in ments_records}
                        existing_rows = get_all_supabase_data('auth_ments', 'keyword')
                        existing_keywords = {str(r.get('keyword', '')).strip() for r in existing_rows if r.get('keyword')}
                        removed_keywords = list(existing_keywords - new_keywords)

                        # 시트에서 사라진 키워드만 삭제 (전체삭제 아님)
                        if removed_keywords:
                            for i in range(0, len(removed_keywords), 200):
                                chunk = removed_keywords[i:i + 200]
                                supabase.table('auth_ments').delete().in_('keyword', chunk).execute()

                        # 나머지는 upsert (있으면 갱신, 없으면 추가)
                        process_in_chunks('auth_ments', ments_records, is_insert=False)
                        sync_reports.append(f"• 멘트: {len(ments_records)}개 키워드 (삭제 {len(removed_keywords)}개)")

                # B. '방관리' 시트 동기화
                if room_manage_sheet:
                    room_data = room_manage_sheet.get_all_records()
                    room_records = []
                    for row in room_data:
                        keys = list(row.keys())
                        if len(keys) >= 2:
                            r_name = str(row[keys[0]]).replace(" ", "")
                            r_id = str(row[keys[1]]).strip()
                            if r_name and r_id:
                                room_records.append({"room_name": r_name, "room_id": r_id})
                    if room_records and supabase:
                        supabase.table('room_management').delete().neq('room_name', '_DELETE_ALL_KEY_').execute()
                        process_in_chunks('room_management', room_records, is_insert=True)
                        sync_reports.append(f"• 방관리: {len(room_records)}개 방 목록")

                # C. '녹음' 시트 동기화
                try:
                    if client:
                        rec_sheet = client.open("인증멘트").worksheet("녹음")
                        males = [c.strip() for c in rec_sheet.col_values(1)[1:] if c and c.strip()]
                        females = [c.strip() for c in rec_sheet.col_values(2)[1:] if c and c.strip()]
                        rec_records = [{"gender": "male", "ment": m} for m in males] + [{"gender": "female", "ment": f} for f in females]
                        
                        if rec_records and supabase:
                            supabase.table('recording_ments').delete().gte('id', 0).execute()
                            process_in_chunks('recording_ments', rec_records, is_insert=True)
                            sync_reports.append(f"• 녹음멘트: 남성({len(males)}) / 여성({len(females)})")
                except Exception as e:
                    print(f"녹음 시트 동기화 패스: {e}")

                # D. '검증' 시트 동기화 (기존 유저 정보 백업)
                if validation_sheet:
                    all_val_data = validation_sheet.get_all_values()
                    if all_val_data and len(all_val_data) > 1:
                        col_map = build_validation_sheet_col_map(all_val_data[0])

                        if col_map is None:
                            sync_reports.append(
                                "• ⚠️ 검증이력: 건너뜀 — 헤더 행에서 'user_id/아이디' 열을 찾지 못했습니다. "
                                "시트 1행(헤더)이 지워졌거나 이름이 바뀐 것 같아요. 잘못된 데이터가 저장될 위험이 있어 동기화를 중단했습니다."
                            )
                        else:
                            # ✨ [변경됨] 위치(인덱스) 하드코딩 대신 헤더 이름으로 찾은 열에서 값을 읽습니다.
                            # (시트에 열이 삽입/삭제/이동돼도 엉뚱한 필드에 값이 들어가지 않도록)
                            grouped_records = {}
                            skipped_short_rows = 0
                            for row in all_val_data[1:]:
                                u_id = get_cell(row, col_map["user_id"])
                                if not u_id:
                                    continue

                                record = {
                                    "user_id": u_id,
                                    "nickname": get_cell(row, col_map["nickname"]),
                                    "gender": get_cell(row, col_map["gender"]),
                                    "region": get_cell(row, col_map["region"]),
                                    "birth_year": get_cell(row, col_map["birth_year"]),
                                    "entry_date": get_cell(row, col_map["entry_date"]),
                                    "black_reason": get_cell(row, col_map["black_reason"]),
                                }

                                # ✨ [변경됨] 재시도횟수/상태는 그 유저의 '진행 단계'를 의미하는 값이라,
                                # 셀이 아예 없어서(=행이 짧아서) 못 읽은 경우엔 기본값(1, 입장대기)으로
                                # 덮어쓰지 않고 아예 이 dict에서 빼버립니다. → upsert 시 DB에 남아있던
                                # 기존 값(예: 승인대기/완료, retry_count 3 등)이 그대로 보존됩니다.
                                retry_idx = col_map["retry_count"]
                                if retry_idx is not None and len(row) > retry_idx and row[retry_idx].strip().isdigit():
                                    record["retry_count"] = int(row[retry_idx])
                                else:
                                    skipped_short_rows += 1

                                status_idx = col_map["status"]
                                if status_idx is not None and len(row) > status_idx:
                                    record["status"] = row[status_idx].strip()

                                # 같은 필드 구성(key 조합)끼리 묶어서 upsert (배치 안에서 필드가
                                # 들쭉날쭉하면 일부 행이 의도치 않게 NULL로 덮어써질 수 있어서, 반드시
                                # 완전히 동일한 key 조합끼리만 같은 배치로 묶습니다.)
                                sig = tuple(sorted(record.keys()))
                                grouped_records.setdefault(sig, []).append(record)

                            total_synced = 0
                            if supabase:
                                for records in grouped_records.values():
                                    process_in_chunks('user_validations', records, is_insert=False)
                                    total_synced += len(records)

                            report_line = f"• 검증이력: {total_synced}명 유저 데이터"
                            if skipped_short_rows:
                                report_line += f" (재시도횟수/상태 정보가 없어 기존 값을 유지한 행 {skipped_short_rows}건 포함)"
                            sync_reports.append(report_line)

                report_str = "\n".join(sync_reports)
                reply_text = f"✅ 모든 구글 시트 데이터가 DB에 동기화되었습니다!\n\n{report_str}"
            except Exception as e:
                reply_text = f"❌ DB 업데이트 중 오류 발생: {e}"

            if reply_text:
                safe_reply_text = reply_text[:4900] if len(reply_text) > 4900 else reply_text
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=safe_reply_text)]))
                return

        # 2) /O번방 확인 명령어
        elif "확인" in command_body:
            room_name_input = command_body.replace("확인", "").strip()
            target_room_id = get_room_id_by_name(room_name_input)
            if target_room_id:
                room_state = get_room_state(target_room_id)
                if not room_state:
                    reply_text = f"📭 [{room_name_input}] 현재 대기 중인 신규 인증 멤버가 없습니다."
                else:
                    status = room_state.get('status')
                    is_known = room_state.get('is_known', False)
                    tracked_user_id = room_state.get('user_id')
                    if status == 'joined':
                        if is_known:
                            reply_text = f"⚠️ [{room_name_input}]\n기존 방문/블랙리스트 이력이 있는 유저가 방금 입장했습니다!\n(현재 상대방이 양식을 입력 중입니다)"
                        else:
                            reply_text = f"⏳ [{room_name_input}]\n완전한 신규 유저가 현재 양식을 입력 중입니다."
                    elif status == 'form_submitted':
                        alert_report = room_state.get('report', f"[{room_name_input}] 양식이 접수되었습니다.")
                        reply_text = alert_report

                    # ✨ [추가됨] 음성검증까지 확인되었는지 여부 + 확인된 시점이라면 그 확인 내용을 함께 출력
                    if reply_text and tracked_user_id:
                        reply_text = f"{reply_text}\n\n{format_voice_check_status(tracked_user_id)}"
            else:
                reply_text = f"❌ '{room_name_input}' 정보를 DB/방관리 시트에서 찾을 수 없습니다."

            if reply_text:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
            return

    # 📌 [핵심 검증 1] 1번 양식 제출 처리 (마지막 입장 유저만 작동)
    if not is_admin_room and all(k in user_message for k in ["닉네임", "년생", "성별", "지역"]):
        if not is_last_joined_user(source_id, user_id):
            return 

        extracted_data = {}
        for line in user_message.split("\n"):
            delimiter = ":" if ":" in line else ("：" if "：" in line else None)
            if delimiter:
                parts = line.split(delimiter, 1)
                key_name = parts[0].replace("-", "").strip()
                if "(" in key_name:
                    key_name = key_name.split("(", 1)[0].strip()
                if "/" in key_name:
                    key_name = key_name.split("/", 1)[0].strip()
                extracted_data[key_name] = parts[1].strip()

        required_fields = ["닉네임", "년생", "나이", "성별", "지역", "결혼유무", "군필여부", "초대자", "야단라경험유무", "기존 다른방에서 나온이유", "다른 방에서 킥을 당한적 있는지"]
        missing_fields = []
        user_gender = extracted_data.get("성별", "").strip()

        for req_field in required_fields:
            val = extracted_data.get(req_field, "").strip()
            if not val:
                if req_field == "군필여부" and user_gender in ["여", "여자"]:
                    continue
                missing_fields.append(req_field)

        if missing_fields:
            reply_text = f"⚠️ 양식 작성 내용 중 다음 항목이 누락되었습니다:\n- {', '.join(missing_fields)}\n\n해당 항목을 빠짐없이 작성 후 다시 제출해 주세요!"
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
            return

        nickname = extracted_data.get("닉네임", "").strip()
        birth_year = extracted_data.get("년생", "").strip()
        age = extracted_data.get("나이", "").strip()
        gender = extracted_data.get("성별", "").strip()
        region = extracted_data.get("지역", "").strip()
        marriage = extracted_data.get("결혼유무", "").strip()
        military = extracted_data.get("군필여부", "").strip()
        inviter = extracted_data.get("초대자", "").strip()
        yadan = extracted_data.get("야단라경험유무", "").strip()
        leave_reason = extracted_data.get("기존 다른방에서 나온이유", "").strip()
        kick_reason = extracted_data.get("다른 방에서 킥을 당한적 있는지", "").strip()
        
        # 한국 시간(KST) 기준 어제 날짜 계산
        kst = datetime.timezone(datetime.timedelta(hours=9))
        current_date = (datetime.datetime.now(kst) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        save_success = False
        alert_text = None

        # A. DB 중복/블랙리스트 조회 (1000명 이상 안전 조회 처리 적용)
        found_duplicates = []
        highest_alert_level = 0
        alert_status_text = ""
        color_emoji = ""

        # 현재 세션에서 이미 1번 양식을 제출한 뒤, 내용을 고쳐서 재제출하는 것인지 확인
        # (같은 방의 room_state가 이미 form_submitted 상태 + 같은 유저라면 '수정 재제출'로 간주)
        current_room_state = get_room_state(source_id)
        is_edit_resubmission = bool(
            current_room_state
            and current_room_state.get('user_id') == user_id
            and current_room_state.get('status') == 'form_submitted'
        )

        if supabase:
            try:
                # 1000개 이상 제약 해결: 페이지네이션 기반 전체 데이터 호출
                all_val_data = get_all_supabase_data('user_validations')

                for row in all_val_data:
                    rec_id = str(row.get('user_id', '')).strip()

                    # 본인이 이번 세션에서 양식을 수정 재제출하는 경우, 방금 전 자신이 남긴
                    # 기록은 '중복 유저'가 아니므로 검증 대상에서 제외한다.
                    if is_edit_resubmission and rec_id == str(user_id).strip() and rec_id != "":
                        continue

                    rec_name = str(row.get('nickname', '')).strip()
                    rec_year = str(row.get('birth_year', '')).strip()
                    rec_gender = str(row.get('gender', '')).strip()
                    rec_region = str(row.get('region', '')).strip()
                    rec_black = str(row.get('black_reason', '')).strip()

                    # details(JSON) 필드에서 값이 채워진 항목만 추출 (없으면 빈 dict로 처리)
                    rec_details = row.get('details') or {}
                    if isinstance(rec_details, str):
                        try:
                            rec_details = json.loads(rec_details)
                        except Exception:
                            rec_details = {}

                    is_id_matched = (rec_id == str(user_id).strip() and rec_id != "")
                    is_name_matched = (rec_name == nickname and rec_name != "")

                    if is_id_matched or is_name_matched:
                        match_reasons = []
                        if is_id_matched: match_reasons.append("고유ID 일치")
                        if is_name_matched: match_reasons.append("닉네임 일치")

                        match_score = 0
                        if rec_year == birth_year: match_score += 1
                        if rec_gender == gender: match_score += 1
                        if rec_region == region: match_score += 1

                        row_info = f"📍 [기존 DB 기록] ({', '.join(match_reasons)})\n - 기존정보: {rec_name} / {rec_year}년생 / {rec_gender} / {rec_region}"
                        if rec_black:
                            row_info += f"\n - 💀 블랙사유: {rec_black}"

                        # 블랙사유 유무와 상관없이, 기존에 입력되어 있던 내용(나온이유/킥이력/야단라경험 우선 순)을
                        # 개수 제한 없이 함께 보여준다. (없는 항목은 자동으로 제외됨)
                        detail_priority = [
                            ("leave_reason", "나온이유"),
                            ("kick_reason", "킥이력"),
                            ("yadan", "야단라경험"),
                            ("inviter", "초대자"),
                            ("marriage", "결혼유무"),
                            ("military", "군필여부"),
                            ("age", "나이"),
                        ]
                        detail_lines = []
                        for key, label in detail_priority:
                            val = str(rec_details.get(key, "")).strip()
                            if val:
                                detail_lines.append(f"{label}: {val}")
                        if detail_lines:
                            row_info += f"\n - 📝 기존 입력 내용: {' / '.join(detail_lines)}"
                        found_duplicates.append(row_info)

                        current_level = 0
                        if is_name_matched:
                            if match_score == 3: current_level = 4
                            elif match_score > 0: current_level = 2
                            else: current_level = 1
                        if is_id_matched:
                            if current_level < 3: current_level = 3
                        if rec_black:
                            current_level = 5

                        if current_level > highest_alert_level:
                            highest_alert_level = current_level
                            if current_level == 5: alert_status_text, color_emoji = "💀 [위험] 블랙리스트 유저 감지", "⚫"
                            elif current_level == 4: alert_status_text, color_emoji = "🚨 [적색 경고] 닉네임 및 모든 정보 일치", "🔴"
                            elif current_level == 3: alert_status_text, color_emoji = "🔄 [주의] 재입장 유저 (동일 ID 확인)", "🟪"
                            elif current_level == 2: alert_status_text, color_emoji = "⚠️ [황색 경고] 닉네임 및 정보 일부 일치", "🟡"
                            elif current_level == 1: alert_status_text, color_emoji = "🔵 [주의] 닉네임 일치 유저", "🟦"

                if highest_alert_level > 0:
                    dup_details_str = "\n\n".join(found_duplicates)
                    alert_text = (
                        f"{color_emoji} 신입 양식 작성 중복/블랙 필터링 결과\n\n"
                        f"📌 상태: {alert_status_text}\n"
                        f"👤 신규입력: {nickname} ({birth_year}년생) / {region} / {gender}\n\n"
                        f"📑 [중복/기존 내역 상세]\n{dup_details_str}\n\n"
                        f"💡 관리자분들께서는 위 상세 내역을 기반으로 승인 여부를 검토하시기 바랍니다."
                    )
            except Exception as db_err:
                print(f"DB 검증 에러: {db_err}")

        # B. DB 업서트(Upsert) 저장
        update_data_details = {
            "age": age, "marriage": marriage, "military": military,
            "inviter": inviter, "yadan": yadan, "leave_reason": leave_reason, "kick_reason": kick_reason
        }
        
        target_status = "입장대기"
        if supabase:
            try:
                user_res = supabase.table('user_validations').select('retry_count, status, entry_date').eq('user_id', user_id).execute()
                retry_cnt = 1
                existing_entry_date = None
                if user_res.data:
                    retry_cnt = user_res.data[0].get('retry_count', 1) + 1
                    existing_entry_date = user_res.data[0].get('entry_date')

                # 기존 입장일 기록에 이번 날짜를 콤마로 이어붙임 (덮어쓰지 않고 누적)
                entry_date_to_save = f"{existing_entry_date},{current_date}" if existing_entry_date else current_date

                supabase.table('user_validations').upsert({
                    "user_id": user_id,
                    "nickname": nickname,
                    "gender": gender,
                    "region": region,
                    "birth_year": birth_year,
                    "entry_date": entry_date_to_save,
                    "retry_count": retry_cnt,
                    "status": target_status,
                    "details": update_data_details
                }).execute()
                save_success = True
            except Exception as e:
                print(f"Supabase 유저 저장 에러: {e}")

        # C. 구글 시트 백업
        with sheet_sync_lock():
            try:
                if validation_sheet:
                    all_data = validation_sheet.get_all_values()
                    clean_user_ids = [str(row[4]).strip() if len(row) > 4 else "" for row in all_data]
                    details_list = [age, marriage, military, inviter, yadan, leave_reason, kick_reason]

                    if user_id in clean_user_ids:
                        found_row_index = clean_user_ids.index(user_id) + 1
                        row_data = all_data[found_row_index - 1]
                        count_val = row_data[7] if len(row_data) >= 8 else "0"
                        current_retry_count = int(count_val) if count_val.isdigit() else 0

                        # F열(입장일)도 덮어쓰지 않고 콤마로 이어붙여 누적
                        existing_entry_date_sheet = row_data[5].strip() if len(row_data) > 5 and row_data[5].strip() else ""
                        entry_date_to_save_sheet = f"{existing_entry_date_sheet},{current_date}" if existing_entry_date_sheet else current_date

                        update_data_basic = [nickname, gender, region, birth_year, user_id, entry_date_to_save_sheet, "", current_retry_count + 1]
                        validation_sheet.update(range_name=f'A{found_row_index}:H{found_row_index}', values=[update_data_basic])
                        validation_sheet.update(range_name=f'K{found_row_index}:L{found_row_index}', values=[[user_id, target_status]])
                        validation_sheet.update(range_name=f'M{found_row_index}:S{found_row_index}', values=[details_list])
                    else:
                        found_row_index = len(all_data) + 1
                        row_to_insert_basic = [nickname, gender, region, birth_year, user_id, current_date, "", 1]
                        validation_sheet.update(range_name=f'A{found_row_index}:H{found_row_index}', values=[row_to_insert_basic])
                        validation_sheet.update(range_name=f'K{found_row_index}:L{found_row_index}', values=[[user_id, target_status]])
                        validation_sheet.update(range_name=f'M{found_row_index}:S{found_row_index}', values=[details_list])
                    save_success = True
            except Exception as sheet_err:
                print(f"구글 시트 백업 에러: {sheet_err}")

        if save_success:
            if alert_text:
                state_data = {"user_id": user_id, "status": "form_submitted", "report": alert_text}
            else:
                state_data = {"user_id": user_id, "status": "form_submitted", "report": "✅ 해당 유저는 중복/블랙 이력이 없는 깨끗한 신규 회원입니다.\n양식이 정상 접수되었습니다."}

            set_room_state(source_id, state_data, ttl=7200)
            set_user_session(user_id, {"nickname": nickname, "gender": gender})

            if is_edit_resubmission:
                # 이미 1번 양식을 제출한 상태에서 내용만 고쳐서 재제출한 경우: DB만 갱신하고 2번 멘트는 다시 보내지 않음
                reply_text = f"✏️ [{nickname}]님, 수정하신 내용이 반영되었습니다."
            else:
                form2_text = search_keyword("2") or search_keyword("2번")
                if form2_text:
                    reply_text = form2_text.replace("{닉네임}", nickname).replace("{nickname}", nickname)
                else:
                    reply_text = f"[{nickname}]님, 1번 양식이 정상 접수되었습니다.\n\n안내 사항을 읽으신 후 '확인'이라고 답장해 주세요."
        else:
            reply_text = "⚠️ 서버 통신 문제로 저장에 실패했습니다. 점(.)을 입력하여 처음부터 다시 시도해 주세요!"

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
        return

    # 📌 [핵심 검증 2] 신입이 "확인" 답장 입력 시 (마지막 입장 유저만 작동)
    if not is_admin_room and not user_message.startswith("/") and any(word in user_message for word in ["확인", "확인했습니다", "확인완료"]):
        if not is_last_joined_user(source_id, user_id):
            return  

        current_row_for_check = get_validation_row(user_id, "status")
        current_status_for_check = current_row_for_check.get('status')

        # ✨ [추가됨] '승인대기' 상태에서 온 "확인" -> '/ㅇㅈ 닉변' 멘트를 신입이 입력한 생년/닉네임으로 채워서 전송하고 '닉변대기'로 전환
        # (남성은 보통 운영진이 먼저 '/ㅇㅈ 4번' 멘트를 보낸 뒤 이 "확인"을 받지만, 여성은 '4번' 과정 없이
        #  '문제없음' 확인 직후 바로 이 "확인"을 받아도 동일하게 진행됩니다 — 코드상 '4번' 발송 여부는 확인하지 않습니다.)
        if current_status_for_check == "승인대기":
            row = get_validation_row(user_id, "nickname, birth_year, gender")
            nickname = row.get('nickname') or ""
            birth_year = row.get('birth_year') or ""
            gender = row.get('gender') or ""
            # 여성은 닉네임 뒤에 붙는 이모지만 다르게(⚖️), 그 외(남성 등)는 기존과 동일(🧨)
            nickname_emoji = "⚖️" if gender in ("여", "여자") else "🧨"

            nickchange_template = search_keyword("닉변")
            if nickchange_template:
                nickchange_text = (
                    nickchange_template
                    .replace("{생년}", birth_year).replace("{birth_year}", birth_year)
                    .replace("{닉네임}", nickname).replace("{nickname}", nickname)
                    .replace("{닉네임이모지}", nickname_emoji).replace("{emoji}", nickname_emoji)
                )
            else:
                nickchange_text = (
                    "닉네임을 아래 형식으로 복사하여 변경해 주세요.\n\n"
                    f"{birth_year} {nickname}{nickname_emoji}\n\n"
                    "변경 후 프로필 사진도 도용 사진이 아닌 사진으로 설정해 주시고, '변경완료'라고 답장해 주세요."
                )

            if supabase:
                supabase_execute(
                    lambda: supabase.table('user_validations').update({"status": "닉변대기"}).eq('user_id', user_id).execute(),
                    label="닉변대기 상태 전환"
                )
            sync_status_to_sheet(user_id, "닉변대기")

            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=nickchange_text)])
                )
            return

        # ✨ [추가됨] '헤르페스확인대기' 상태에서 (닉네임/프사를 다시 고친 뒤) 온 "확인" -> 최종 판정 재시도
        if current_status_for_check == "헤르페스확인대기":
            finalize_after_nickname_check(source_id, user_id, event.reply_token)
            return

        try:
            success, voice_reply_text = start_voice_auth(user_id, require_status='입장대기')

            if success:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(
                        ReplyMessageRequest(
                            reply_token=event.reply_token, 
                            messages=[TextMessage(text=voice_reply_text)]
                        )
                    )
                return
        except Exception as e:
            print(f"확인 답변 처리 에러: {e}")

    # 📌 [핵심 검증 2-1] 신입이 "문제없음" 답장 입력 시 (음성 파일 제출 후, 마지막 입장 유저만 작동)
    # ✨ [변경됨] 클라우드런을 여기서 다시 호출하지 않는다. 분석은 이미 오디오 도착 시점에
    # 클라우드런 쪽에서 백그라운드로 시작돼 있으므로, 여기선 그 결과를 "조회"만 한다.
    # (만약 이 요청이 타임아웃/에러로 죽으면 상태를 "음성확인중"으로 되돌려 두므로,
    #  신입이 "문제없음"을 다시 보내면 그대로 재시도된다 — 영구히 붕 뜨는 상태가 없음)
    if not is_admin_room and not user_message.startswith("/") and any(
        word in user_message for word in ["문제없음", "문제 없음", "문제없어요", "문제 없어요"]
    ):
        if not is_last_joined_user(source_id, user_id):
            return

        # 동시에 두 번 들어와도(예: LINE 웹훅 재시도) 중복 처리되지 않도록
        # "음성확인중" -> "분석중"으로 CAS 선점. 실패하면 이미 처리 중이거나 이미 끝난 것.
        # (여기서의 "분석중"은 클라우드런 작업 상태가 아니라 이 요청 자체의 처리 락 용도)
        if not cas_update_status(user_id, "음성확인중", "분석중", label="문제없음 확인(결과 조회 시작)"):
            return

        try:
            reply_text = format_voice_analysis_reply_text(user_id)
            cas_update_status(user_id, "분석중", "승인대기", label="문제없음 확인(결과 조회 완료)")
            sync_status_to_sheet(user_id, "승인대기")
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
                )
        except Exception as e:
            print(f"⚠️ 문제없음 처리(결과 조회) 중 예외 — 재시도 가능하도록 상태 되돌림: {e}")
            cas_update_status(user_id, "분석중", "음성확인중", label="문제없음 처리 실패 롤백")
            sync_status_to_sheet(user_id, "음성확인중")
            try:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(
                        ReplyMessageRequest(
                            reply_token=event.reply_token,
                            messages=[TextMessage(text="⚠️ 확인 중 일시적인 오류가 발생했습니다. '문제없음'이라고 다시 한번 답장해 주세요.")]
                        )
                    )
            except Exception:
                pass
        return

    # 📌 [핵심 검증 2-2] 신입이 "변경완료" 답장 입력 시 ('닉변대기' 상태, 마지막 입장 유저만 작동)
    if not is_admin_room and not user_message.startswith("/") and any(
        word in user_message for word in ["변경완료", "변경 완료", "닉변완료", "닉변 완료"]
    ):
        if not is_last_joined_user(source_id, user_id):
            return

        current_row = get_validation_row(user_id, "status")
        if current_row.get('status') == "닉변대기":
            # ✨ [추가됨] 한 번 더 음성 자동분석 결과를 DB에서 재확인 (완료 상태가 아니어도 흐름은 막지 않고 로그만 남김
            #  - 최종 블랙리스트 판정은 어차피 헤르페스 확인 단계에서 has_blacklist_issue()로 다시 검사함)
            voice_row = get_validation_row(user_id, "voice_analysis_state")
            if voice_row.get('voice_analysis_state') != "완료":
                print(f"⚠️ user_id={user_id} 변경완료 시점에도 음성 자동분석이 아직 '완료' 상태가 아님 (재확인 필요)")

            if supabase:
                supabase_execute(
                    lambda: supabase.table('user_validations').update({"status": "헤르페스확인대기"}).eq('user_id', user_id).execute(),
                    label="헤르페스확인대기 상태 전환"
                )
            sync_status_to_sheet(user_id, "헤르페스확인대기")

            reply_text = "✅ 확인되었습니다. 위 안내 사항(질문 포함)에 답변해 주세요."
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
                )
        return

    # 📌 [핵심 검증 2-3] 헤르페스 질문에 "없음(무증상/미감염)"류로 답변 시 ('헤르페스확인대기' 상태)
    if not is_admin_room and not user_message.startswith("/") and any(
        word in user_message for word in ["없습니다", "없어요", "없다", "없음", "아니요", "아니오"]
    ):
        if not is_last_joined_user(source_id, user_id):
            return

        current_row = get_validation_row(user_id, "status")
        if current_row.get('status') == "헤르페스확인대기":
            finalize_after_nickname_check(source_id, user_id, event.reply_token)
        return

    # 3. 일반 DB 키워드 검색
    matched_reply = search_keyword(user_message)
    if matched_reply:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=matched_reply)]))
        return


# ==========================================
# [핸들러 2] 방 입장 이벤트 처리 핸들러
# ==========================================
def send_join_welcome(source_id, user_id, reply_token):
    """신입 입장 시 최초 안내(1번 멘트 전송 + room_state 세팅) 로직.
    - MemberJoinedEvent 정상 처리 시 그리고
    - 프로필 조회 실패 후 '시작' 재입력으로 재시도할 때 양쪽에서 공용으로 사용합니다.
    - 재입장인 경우, DB에 남아있는 마지막 status를 확인해서 대화가 끊긴 지점을 안내하고
      해당 단계부터 이어서 진행할 수 있도록 합니다.
    """
    existing = None
    if supabase:
        try:
            res = supabase.table('user_validations').select('status, nickname').eq('user_id', user_id).execute()
            if res.data:
                existing = res.data[0]
        except Exception as e:
            print(f"user_validations 조회 에러(무시하고 진행): {e}")

    is_known = existing is not None
    last_status = (existing or {}).get('status')
    nickname = (existing or {}).get('nickname') or ""
    name_prefix = f"{nickname}님, " if nickname else ""

    set_room_state(source_id, {
        "user_id": user_id,
        "status": "joined",
        "is_known": is_known
    }, ttl=3600)

    welcome_message = None

    # 재입장 + 이전에 진행 중이던 status가 남아있는 경우 -> 끊긴 지점 안내 + 해당 단계로 이어서 진행
    if last_status == "음성대기":
        v_success, v_reply_text = start_voice_auth(user_id, require_status=None)
        if v_success:
            welcome_message = f"🔄 {name_prefix}이전 대화가 [음성인증] 단계에서 끊겼어요. 이어서 진행할게요.\n\n{v_reply_text}"
    elif last_status in ("음성확인중", "분석중"):
        # ✨ [추가됨] 음성 파일은 받았지만 아직 '문제없음' 확인이 안 끝난 중간 단계
        three_text = search_keyword("3") or search_keyword("3번")
        resume_text = three_text or "음성 파일이 정상적으로 접수되었는지 확인 중입니다.\n문제가 없다면 '문제없음'이라고 답장해 주세요."
        welcome_message = f"🔄 {name_prefix}이전 대화가 [음성 확인] 단계에서 끊겼어요.\n\n{resume_text}"
    elif last_status == "승인대기":
        welcome_message = f"🔄 {name_prefix}이전 대화가 [운영진 승인 대기] 단계에서 끊겼어요.\n운영진 확인 후 승인될 예정이니 잠시만 기다려 주세요."
    elif last_status == "입장대기" and nickname:
        # nickname이 있다는 건 1번 양식까지는 이미 제출했었다는 뜻 -> '확인' 답장 대기 단계로 이어서 진행
        form2_text = search_keyword("2") or search_keyword("2번")
        if form2_text:
            resume_text = form2_text.replace("{닉네임}", nickname).replace("{nickname}", nickname)
        else:
            resume_text = f"[{nickname}]님, 1번 양식이 정상 접수되었습니다.\n\n안내 사항을 읽으신 후 '확인'이라고 답장해 주세요."
        welcome_message = f"🔄 {name_prefix}이전 대화가 [1번 양식 제출 후 확인 대기] 단계에서 끊겼어요.\n\n{resume_text}"
    # last_status가 "완료"이거나, 기록이 없거나, 위에서 안내문을 못 만든 경우 -> 처음 입장한 것처럼 1번 멘트부터 진행

    if not welcome_message:
        welcome_message = search_keyword("1") or search_keyword("1번")
    if not welcome_message:
        welcome_message = "인증봇을 추가해주세요\nhttps://lin.ee/ttJ0cUk"

    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=reply_token,
                    messages=[TextMessage(text=welcome_message)]
                )
            )
    except Exception as e:
        print(f"신입 안내 메시지 전송 실패: {e}")


def fetch_member_profile(line_bot_api, source, user_id):
    """그룹채팅방 신입 유저 프로필 조회. 유저의 프라이버시 설정(예: '봇의 프로필 조회 허용' 꺼짐)
    등으로 인해 친구추가가 안 되어 있으면 조회가 실패(예외 발생)할 수 있습니다."""
    group_id = getattr(source, 'group_id', None)
    if group_id:
        return line_bot_api.get_group_member_profile(group_id, user_id)
    return None


@handler.add(MemberJoinedEvent)
def handle_member_joined(event):
    source_id = getattr(event.source, 'group_id', getattr(event.source, 'room_id', None))
    if not source_id:
        return

    # ✨ [추가됨] 인증자방(ADMIN_GROUP_CHAT_ID)은 신입 검증 대상 방이 아니므로,
    # 여기서 누가 입장하든 환영 멘트/프로필 조회/room_state 기록을 하지 않는다.
    if source_id == ADMIN_GROUP_CHAT_ID:
        return

    # ✨ [변경됨] Cloud Run 웜업은 더 이상 입장 시점이 아니라 start_voice_auth()(=신입이
    # "확인"이라고 답장해서 음성인증이 실제로 시작되는 시점)에서 호출한다. 입장~확인 사이에는
    # 시간차가 커서 웜업이 식어버릴 수 있어, 실제 녹음 직전에 깨우는 게 더 효과적이다.

    joined_members = event.joined.members
    for member in joined_members:
        user_id = member.user_id
        if not user_id:
            continue

        # 신입 유저 프로필 조회 시도 (설정값/친구추가 여부 등으로 실패할 수 있음)
        try:
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                fetch_member_profile(line_bot_api, event.source, user_id)
        except Exception as e:
            print(f"신입 프로필 조회 실패(친구추가 필요 추정): {e}")

            set_room_state(source_id, {
                "user_id": user_id,
                "status": "pending_friend"
            }, ttl=3600)

            # /ㅇㅈ 1ㅂㅊㄱ, /ㅇㅈ 2ㅂㅊㄱ 두 키워드 멘트를 순서대로 전송
            ment1 = search_keyword("1ㅂㅊㄱ")
            ment2 = search_keyword("2ㅂㅊㄱ")
            guide_messages = [TextMessage(text=t) for t in (ment1, ment2) if t]

            if not guide_messages:
                guide_messages = [TextMessage(text=(
                    "저를 추가해주셔야 인증진행이 가능해요.\n"
                    "친구추가 해주시고 채팅창에 '시작'이라고 입력해 주세요."
                ))]

            try:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(
                        ReplyMessageRequest(
                            reply_token=event.reply_token,
                            messages=guide_messages
                        )
                    )
            except Exception as e2:
                print(f"친구추가 안내 메시지 전송 실패: {e2}")
            continue

        send_join_welcome(source_id, user_id, event.reply_token)


# ==========================================
# [핸들러 3] 방 퇴장 이벤트 처리 핸들러
# ==========================================
@handler.add(MemberLeftEvent)
def handle_member_left(event):
    source_id = getattr(event.source, 'group_id', getattr(event.source, 'room_id', None))
    if not source_id:
        return

    # ✨ [추가됨] 인증자방(ADMIN_GROUP_CHAT_ID)은 신입 검증 대상 방이 아니므로,
    # 여기서 누가 나가든 room_state/인증 상태를 건드리지 않는다.
    if source_id == ADMIN_GROUP_CHAT_ID:
        return

    left_user_ids = {m.user_id for m in event.left.members if getattr(m, 'user_id', None)}

    room_state = get_room_state(source_id)
    tracked_user_id = room_state.get('user_id') if room_state else None

    if tracked_user_id and tracked_user_id in left_user_ids:
        # ✨ [추가됨] '/ㅇㅈ 닉변' -> '변경완료' -> 헤르페스 확인까지 모두 통과해서
        # 자동으로 '/ㅇㅈ 퇴장' 멘트가 나간 뒤 신입이 실제로 나간 경우인지 확인
        leave_row = get_validation_row(tracked_user_id, "status, nickname")
        was_auto_completed = leave_row.get('status') == "퇴장대기"
        leaving_nickname = leave_row.get('nickname') or "신입"

        # 인증 진행 중이던 신입이 실제로 나간 경우 -> user_validations 상태까지 함께 초기화
        reset_verification_state(source_id, tracked_user_id)

        if was_auto_completed:
            if supabase:
                supabase_execute(
                    lambda: supabase.table('user_validations').update({"status": "완료"}).eq('user_id', tracked_user_id).execute(),
                    label="자동 인증완료 상태 반영"
                )
            sync_status_to_sheet(tracked_user_id, "완료")
            try:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.push_message_with_http_info(
                        PushMessageRequest(
                            to=source_id,
                            messages=[build_all_mention_message(f"[{leaving_nickname}]님 인증이 완료되었습니다.")]
                        )
                    )
            except Exception as e:
                print(f"인증방 완료 알림 전송 실패: {e}")
    elif not left_user_ids or tracked_user_id is None:
        # 누가 나갔는지 알 수 없거나, 추적 중인 인증 대상이 없는 경우에는 기존처럼 room_state만 정리
        del_room_state(source_id)
    # else: 인증과 무관한 다른 멤버가 나간 경우 -> 진행 중인 신입의 room_state를 건드리지 않음


# ==========================================
# ✨ [추가됨] 음성 자동분석 결과를 운영진방에 참고용으로 알리는 헬퍼
# ==========================================
# 블랙리스트 유사도가 이 값 이상이면 "동일인 의심"으로 강조 표시한다.
VOICE_MATCH_ALERT_THRESHOLD = 0.90

# ✨ [추가됨] 오토튠/피치보정 의심도(%)가 이 값 이상이면 운영진방 알림 조건에 포함시킨다.
AUTOTUNE_ALERT_THRESHOLD = 50.0

# 신청서 성별 표기("남"/"남자"/"여"/"여자")를 "남"/"여"로 정규화하기 위한 매핑
_GENDER_NORM_MAP = {"남": "남", "남자": "남", "여": "여", "여자": "여"}


def build_voice_check_report_lines(*, claimed_gender, voice_result):
    """음성 자동분석 결과에서 '참고할 만한 내용'만 사람이 읽기 좋은 줄 리스트로 뽑아냅니다.
    (운영진방 알림, 'N번방 확인' 저장용 리포트에서 공통으로 사용)
    특이사항이 없으면 빈 리스트를 반환합니다.
    """
    lines = []
    if not voice_result or voice_result.get("error"):
        return lines

    est_gender = voice_result.get("estimated_gender")
    claimed_gender_norm = _GENDER_NORM_MAP.get((claimed_gender or "").strip())
    gender_mismatch = bool(est_gender and claimed_gender_norm and est_gender != claimed_gender_norm)

    if est_gender:
        pitch_note = f" (추정 피치 {voice_result.get('pitch_hz')}Hz)" if voice_result.get("pitch_hz") else ""
        mismatch_note = " ⚠️ 신청서 성별과 다름" if gender_mismatch else ""
        lines.append(f"- 음성 기반 추정 성별: {est_gender}{pitch_note}{mismatch_note}")

    # ✨ [추가됨] 오토튠/피치보정 의심도. 확정 판정이 아니라 정황 지표이므로 항상 그 취지를 함께 남긴다.
    autotune_prob = voice_result.get("autotune_probability")
    if autotune_prob is not None:
        if autotune_prob >= AUTOTUNE_ALERT_THRESHOLD:
            level = "⚠️ 높음(의심)"
        elif autotune_prob >= 20:
            level = "중간"
        else:
            level = "낮음"
        lines.append(f"- 오토튠/피치보정 의심도: {autotune_prob:.1f}% ({level}) — 참고용 정황 지표, 확정 판정 아님")

    matches = voice_result.get("matches") or []
    strong_matches = [m for m in matches if (m.get("similarity") or 0) >= VOICE_MATCH_ALERT_THRESHOLD]
    if strong_matches:
        lines.append("- ⚠️⚠️ 블랙리스트 음성과 매우 유사 (동일인 의심):")
        for m in strong_matches:
            similarity = (m.get("similarity") or 0) * 100
            nickname = m.get("nickname", "알 수 없음")
            match_gender = m.get("gender", "알 수 없음")
            match_status = m.get("status", "상태 없음")
            lines.append(f"  └ 닉네임: {nickname} (과거 성별: {match_gender} / 상태: {match_status} / 일치율: {similarity:.1f}%)")

    # ✨ [추가됨] Cloud Run(app.py)이 80% 이상 일치로 판단해 신규 저장을 생략한 경우 — 에러는 아니지만
    # voice_profiles에 새 레코드가 없다는 뜻이므로 운영진이 참고할 수 있게 남긴다.
    if voice_result.get("is_strong_match"):
        lines.append("- ℹ️ 블랙리스트 일치율 80% 이상으로 판단되어 신규 음성 프로필 저장은 생략됨")

    # ✨ [추가됨] 분석 자체는 성공했지만 Storage 업로드/voice_profiles insert가 실패한 경우.
    # 신입에게는 노출되지 않고(🔵 완료로만 보임) 운영진만 이 리포트로 확인할 수 있다.
    save_error = voice_result.get("save_error")
    if save_error:
        lines.append(f"- ⚠️ 음성 원본/프로필 저장 실패: {save_error} (voice_profiles 미등록 — 재확인 필요)")

    return lines


def build_voice_check_report_text(*, claimed_gender, voice_result):
    """'N번방 확인' 명령어 응답 및 DB(voice_check_report) 저장에 쓰는 한 덩어리 요약 텍스트."""
    if not voice_result or voice_result.get("error"):
        reason = (voice_result or {}).get("error", "결과 없음")
        return f"자동분석 실패({reason}) — 운영진이 음성을 직접 듣고 판단해 주세요."

    lines = build_voice_check_report_lines(claimed_gender=claimed_gender, voice_result=voice_result)
    if not lines:
        return "자동분석 결과 특이사항 없음 (블랙리스트 유사 음성 없음 / 신청 성별과 일치)"
    return "\n".join(lines)


# ✨ [추가됨] '처리중' 상태 워치독
# 클라우드런 쪽에서 OOM/강제종료/네이티브 크래시처럼 파이썬 try/except로도 못 잡는 방식으로
# 죽으면, voice_analysis_state가 '완료'도 '에러'도 아닌 '처리중'에 영원히 멈춘 채 방치될 수 있다.
# voice_analysis_synced_at(마지막 상태 갱신 시각) 기준으로 일정 시간 이상 그대로면 "멈춘 것"으로
# 간주해서 운영진에게만 알려준다 (신입에게는 노출하지 않음 — 기존 analysis_error_note와 동일 원칙).
VOICE_ANALYSIS_STALE_MINUTES = 5


def _voice_analysis_is_stale(row):
    """row(voice_analysis_state, voice_analysis_synced_at 포함)를 보고, '처리중'인 채로
    VOICE_ANALYSIS_STALE_MINUTES 이상 갱신이 없으면 True를 반환한다."""
    if row.get('voice_analysis_state') != "처리중":
        return False
    synced_at = row.get('voice_analysis_synced_at')
    if not synced_at:
        return False
    try:
        synced_dt = datetime.datetime.fromisoformat(str(synced_at).replace('Z', '+00:00'))
        if synced_dt.tzinfo is None:
            synced_dt = synced_dt.replace(tzinfo=datetime.timezone.utc)
    except Exception:
        return False
    elapsed = datetime.datetime.now(datetime.timezone.utc) - synced_dt
    return elapsed > datetime.timedelta(minutes=VOICE_ANALYSIS_STALE_MINUTES)


def format_voice_check_status(user_id):
    """'N번방 확인' 명령어에서 특정 유저의 음성검증 진행 상태를 사람이 읽기 좋은 문장으로 만들어 돌려줍니다.
    - 음성검증까지 확인이 끝났는지 여부
    - 확인이 된 시점이라면(=음성 파일이 제출된 시점) 그때 저장해 둔 확인 내용(자동분석 요약)
    항상 문자열을 반환합니다 (조회 실패/정보 없음이어도 안내 문구를 반환).
    """
    if not supabase or not user_id:
        return "🎙️ 음성검증: 조회 불가 (DB 연결 없음)"

    res = supabase_execute(
        lambda: supabase.table('user_validations')
            .select('status, nickname, gender, voice_checked_at, voice_check_report, '
                    'voice_analysis_state, voice_analysis_result, voice_analysis_error, '
                    'voice_analysis_synced_at')
            .eq('user_id', user_id).execute(),
        label="음성검증 상태 조회(방확인)"
    )
    if not res or not res.data:
        return "🎙️ 음성검증: 해당 유저의 인증 기록을 찾을 수 없습니다."

    row = res.data[0]
    status = row.get('status')
    checked_at = row.get('voice_checked_at')
    report = row.get('voice_check_report')

    # ✅ [수정] '문제없음' 답장이 클라우드런 분석 완료보다 먼저 와서 voice_check_report가
    # DB에 저장되지 못한 경우를 구제한다. voice_analysis_result(클라우드런이 저장한 원본
    # 결과)가 있으면 그 자리에서 읽기 전용으로만 리포트 문자열을 재구성해서 보여준다
    # — DB에는 아무것도 쓰지 않는다 (DB 기록은 여전히 클라우드런의 몫).
    if not report and row.get('voice_analysis_state') == "완료" and row.get('voice_analysis_result'):
        report = build_voice_check_report_text(
            claimed_gender=row.get('gender'),
            voice_result=row.get('voice_analysis_result'),
        )

    # ✨ [추가됨] 자동분석 에러는 신입에게는 절대 보여주지 않고, 운영진이 'N번방 확인'을 했을 때만
    # 노출한다. voice_analysis_state는 '문제없음' 답장 여부(status)와 무관하게 오디오 도착 시점부터
    # 백그라운드로 갱신되므로, 아래에서 status 분기와 별개로 항상 먼저 체크한다.
    analysis_error_note = ""
    if row.get('voice_analysis_state') == "에러":
        analysis_error_note = f"\n🔴 자동분석 오류: {row.get('voice_analysis_error') or '(사유 미상)'}"
    elif _voice_analysis_is_stale(row):
        # ✨ [추가됨] OOM/강제종료 등으로 조용히 멈춘 '처리중' 감지 — 운영진에게만 노출
        analysis_error_note = (
            f"\n🔴 자동분석이 {VOICE_ANALYSIS_STALE_MINUTES}분 넘게 '처리중'에서 멈춰 있습니다 "
            "(응답 없이 중단된 것으로 추정 — 운영진이 직접 확인해 주세요)"
        )

    if status in (None, "", "입장대기"):
        return "🎙️ 음성검증: ❌ 아직 진행 전 (양식 작성 단계)"
    if status == "음성대기":
        return "🎙️ 음성검증: ⏳ 안내 멘트는 발송됨, 아직 음성 파일 미제출"
    if status in ("음성확인중", "분석중"):
        # ✨ [추가됨] 음성 파일은 받았지만 아직 '문제없음' 확인 전(=분석도 아직 실행 전) 중간 단계.
        # 분석은 '문제없음' 답장 시점에 실행되므로, 이 상태에서는 아직 자동분석 결과가 없는 게 정상.
        note = "자동분석 진행 중" if status == "분석중" else "'문제없음' 답장 대기 중"
        return f"🎙️ 음성검증: ⏳ 음성 파일 제출 완료, {note}{analysis_error_note}"
    if status in ("승인대기", "완료"):
        if status == "승인대기":
            header = "🎙️ 음성검증: ✅ 음성 파일 제출 완료 (운영진 최종 승인 대기 중)"
        else:
            header = "🎙️ 음성검증: ✅ 음성 파일 제출 + 운영진 최종 승인까지 완료"
        lines = [header]
        if checked_at:
            lines.append(f"- 확인 시점: {checked_at}")
        if report:
            lines.append(f"- 확인 내용:\n{report}")
        else:
            lines.append("- 확인 내용: (자동분석 기록 없음)")
        return "\n".join(lines) + analysis_error_note

    return f"🎙️ 음성검증: 상태 확인 불가 (status={status}){analysis_error_note}"


def notify_admin_voice_analysis(*, claimed_nickname, claimed_gender, user_id, voice_result):
    """음성 자동분석 결과 중 운영진이 참고할 만한 내용이 있을 때만 운영진방에 push한다.

    알림 조건:
    - 블랙리스트 유사도가 VOICE_MATCH_ALERT_THRESHOLD(기본 90%) 이상인 경우
    - 신청서에 적은 성별과 음성 기반 추정 성별이 다른 경우
    - ✨ [추가됨] 음성 원본/프로필 저장 자체가 실패한 경우 (분석은 성공했지만 DB에는 안 남음 — 방치하면
      나중에 'N번방 확인'을 할 때까지 아무도 모를 수 있으므로 발생 즉시 알린다)
    - ✨ [추가됨] 오토튠/피치보정 의심도가 AUTOTUNE_ALERT_THRESHOLD(기본 50%) 이상인 경우
      (확정 판정 아님 — 운영진이 직접 들어보고 최종 판단해야 함)
    넷 다 아니면 조용히 넘어간다 (매번 알림이 오면 운영진이 피로해지므로).

    ⚠️ 어디까지나 참고 정보다. 자동 판정/자동 승인·차단이 아니며,
    최종 판단은 반드시 운영진이 음성을 직접 듣고 내려야 한다.
    """
    if not voice_result or voice_result.get("error"):
        return

    matches = voice_result.get("matches") or []
    strong_matches = [m for m in matches if (m.get("similarity") or 0) >= VOICE_MATCH_ALERT_THRESHOLD]

    est_gender = voice_result.get("estimated_gender")
    claimed_gender_norm = _GENDER_NORM_MAP.get((claimed_gender or "").strip())
    gender_mismatch = bool(est_gender and claimed_gender_norm and est_gender != claimed_gender_norm)

    save_error = voice_result.get("save_error")

    autotune_prob = voice_result.get("autotune_probability")
    autotune_suspected = bool(autotune_prob is not None and autotune_prob >= AUTOTUNE_ALERT_THRESHOLD)

    if not strong_matches and not gender_mismatch and not save_error and not autotune_suspected:
        return

    lines = [f"🎙️ 음성 자동분석 참고 알림 — {claimed_nickname or '(닉네임 미상)'}님 (신청 성별: {claimed_gender or '미상'})"]
    lines.extend(build_voice_check_report_lines(claimed_gender=claimed_gender, voice_result=voice_result))

    if strong_matches:
        # ▼ 블랙리스트 유사 매칭이 있을 때만 경고 문구 추가 (줄바꿈 \n 포함) ▼
        lines.append("\n※ 자동 판정이 아니니 반드시 직접 음성 대조 후 최종 판단해 주세요.")

    alert_text = "\n".join(lines)

    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.push_message_with_http_info(
                PushMessageRequest(to=ADMIN_GROUP_CHAT_ID, messages=[TextMessage(text=alert_text)])
            )
    except Exception as e:
        print(f"⚠️ 운영진방 음성분석 알림 전송 실패: {e}")


# ==========================================
# [핸들러 4] 📌 음성 메시지 처리 핸들러 (마지막 입장 유저만 작동)
# ==========================================
@handler.add(MessageEvent, message=AudioMessageContent)
def handle_audio(event):
    user_id = event.source.user_id
    if not user_id:
        return

    source_id = getattr(event.source, 'group_id', getattr(event.source, 'room_id', event.source.user_id))

    if not is_last_joined_user(source_id, user_id):
        return

    is_audio_waiting = False
    claimed_nickname, claimed_gender = "", ""
    if supabase:
        res = supabase_execute(
            lambda: supabase.table('user_validations').select('status, nickname, gender').eq('user_id', user_id).execute(),
            label="음성대기 상태 조회"
        )
        if res and res.data and res.data[0].get('status') == '음성대기':
            is_audio_waiting = True
            claimed_nickname = res.data[0].get('nickname') or ""
            claimed_gender = res.data[0].get('gender') or ""

    if is_audio_waiting:
        # ✨ [변경됨/단순화] push 메시지를 전혀 쓰지 않고 "응답(reply) 메시지"만으로 처리하도록 재설계.
        # - 음성 파일 수신 시점엔 무거운 분석을 절대 실행하지 않는다 (Vercel 함수가 오래 붙잡혀 있다가
        #   죽거나 타임아웃 나는 걸 방지) → 3번 멘트만 즉시 reply하고 끝낸다.
        # - 실제 무거운 자동분석(Cloud Run 호출)은 신입이 "문제없음"이라고 답장하는 그 요청 안에서
        #   동기적으로 실행하고, 그 답장의 reply_token으로 최종 안내까지 그대로 회신한다.
        #   → push가 필요 없고, 만약 그 요청이 죽거나 타임아웃 나도 신입이 "문제없음"을 다시 보내면
        #     그대로 재시도되는 구조라 상태가 영구히 붕 뜨지 않는다.
        if supabase:
            update_res = supabase_execute(
                lambda: supabase.table('user_validations').update({"status": "음성확인중"}).eq('user_id', user_id).execute(),
                label="음성인증 음성확인중 업데이트"
            )
            if update_res is None:
                print(f"⚠️ user_id={user_id} 음성확인중 상태 업데이트 실패 — 재시도에도 실패, 시트/알림은 계속 진행")

        sync_status_to_sheet(user_id, "음성확인중")

        # ✨ [추가됨] 나중에 "문제없음" 답장 시점에 이 오디오를 다시 찾아 분석할 수 있도록
        # LINE 메시지 ID를 user_session에 저장해 둔다 (nickname/gender는 그대로 유지하며 병합).
        session_data = get_user_session(user_id) or {}
        if not isinstance(session_data, dict):
            session_data = {}
        session_data["pending_voice_message_id"] = event.message.id
        session_data.setdefault("nickname", claimed_nickname)
        session_data.setdefault("gender", claimed_gender)
        set_user_session(user_id, session_data)

        # ✨ [변경됨] 여기서 클라우드런에 분석을 "접수"만 시키고 끝난다(결과를 기다리지 않음).
        # 실제 무거운 분석(임베딩 추출 등)은 클라우드런이 백그라운드로 계속 진행하다가
        # 끝나면 Supabase에 직접 결과를 기록한다. '문제없음' 답장이 왔을 땐 그 결과를
        # 조회만 하면 되므로 그 요청이 무거워질 일이 없다.
        # (submit_voice_analysis_job은 '접수 확인(202)'까지만 기다리도록 설계되어 있어
        #  아래 reply가 크게 지연되지는 않지만, 콜드스타트 시엔 지연될 수 있다 — warmup으로 완화)
        submit_voice_analysis_job(
            supabase, configuration,
            message_id=event.message.id, user_id=user_id, nickname=claimed_nickname,
        )

        # 접수 확인 겸 '/ㅇㅈ 3' 멘트만 즉시 회신한다.
        three_text = search_keyword("3") or search_keyword("3번")
        immediate_reply_text = three_text or (
            "🎤 음성인증 파일이 정상적으로 접수되었습니다!\n\n"
            "내용 확인 후 문제가 없다면 '문제없음'이라고 답장해 주세요."
        )
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=immediate_reply_text)])
            )


def format_voice_analysis_reply_text(user_id):
    """'문제없음' 답장을 받았을 때 호출된다. 클라우드런을 다시 부르지 않고,
    오디오 도착 시점에 이미 시작된 백그라운드 분석의 현재 상태(voice_analysis_state)를
    DB에서 "조회"만 해서, 앞에 상태 이모지를 붙인 안내 문구를 만들어 돌려준다.

    🔵 = 분석 완료, 결과 조회 가능
    🟡 = 아직 분석 진행 중 (또는 접수 기록조차 아직 없는 경우 — 오디오 도착 처리와의 경합)

    ✨ [변경됨] 자동분석 에러(🔴)는 신입이 있는 방에는 절대 노출하지 않는다 — 시스템 내부 사정을
    신입에게 드러낼 이유가 없고, 괜히 불안하게 만들 수 있다. 에러 상세는 운영진이 'N번방 확인'
    명령어(format_voice_check_status)를 실행했을 때만 보여준다. 신입에게는 정상 진행 중(🟡)과
    똑같은 문구로 안내한다.

    상태와 무관하게 항상 문자열을 반환하고, 예외를 던지지 않는다 — 수동 인증 흐름을 막지 않기 위함.
    """
    PENDING_TEXT_FOR_MEMBER = (
        "🟡 자동분석이 아직 진행 중입니다. 완료되는 대로 운영진 확인 시 함께 반영됩니다.\n\n"
        f"{FINAL_APPROVAL_WAIT_TEXT}"
    )

    if not supabase or not user_id:
        # DB 연결 실패 같은 내부 사정도 신입에게는 노출하지 않는다.
        return PENDING_TEXT_FOR_MEMBER

    res = supabase_execute(
        lambda: supabase.table('user_validations')
            .select('nickname, gender, voice_analysis_state, voice_analysis_result, voice_analysis_error')
            .eq('user_id', user_id).execute(),
        label="음성분석 결과 조회(문제없음)"
    )
    row = (res.data[0] if res and res.data else {}) or {}
    claimed_nickname = row.get('nickname') or ""
    claimed_gender = row.get('gender') or ""
    state = row.get('voice_analysis_state')
    result = row.get('voice_analysis_result') or {}

    if state == "완료":
        # voice_analysis_result는 analyze_new_member_voice()가 반환하던 것과 같은 모양(estimated_gender/
        # matches/pitch_hz 등)으로 클라우드런이 채워 넣는다는 전제 → 기존 리포트/알림 함수를 그대로 재사용.
        report_text = build_voice_check_report_text(claimed_gender=claimed_gender, voice_result=result)
        notify_admin_voice_analysis(
            claimed_nickname=claimed_nickname, claimed_gender=claimed_gender,
            user_id=user_id, voice_result=result,
        )
        kst = datetime.timezone(datetime.timedelta(hours=9))
        supabase_execute(
            lambda: supabase.table('user_validations').update({
                # ✨ 기존 'N번방 확인' 명령어(format_voice_check_status)가 이 두 컬럼을 그대로 읽으므로
                # 하위 호환을 위해 함께 채워 둔다.
                "voice_checked_at": datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M"),
                "voice_check_report": report_text,
            }).eq('user_id', user_id).execute(),
            label="음성검증 확인정보 저장(비동기 파이프라인)"
        )
        return f"🔵 자동분석 결과를 조회했습니다.\n{report_text}\n\n{FINAL_APPROVAL_WAIT_TEXT}"

    # state가 "에러"이거나 "처리중"이거나, 아직 기록 자체가 없는 경우(레이스 컨디션 등) 모두
    # 신입에게는 똑같이 '진행 중'으로만 안내한다. 에러 상세는 운영진이 N번방 확인 시에만 본다.
    return PENDING_TEXT_FOR_MEMBER


if __name__ == "__main__":
    app.run(port=5000)

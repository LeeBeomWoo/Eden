import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import re
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
from voice_analysis import submit_voice_analysis_job, process_admin_blacklist_voice_upload

app = Flask(__name__)


# 인증자방(관리자 그룹방) ID 고정 설정
ADMIN_GROUP_CHAT_ID = "C1fdb3b771a6bd0686fa7dbf1b5145a70"

# ✨ [추가됨] 같은 코드를 여러 클라우드런 인스턴스에 올려서, 인증방별로 처리를 나눠 맡기기 위한 설정.
# 이 서비스 자신의 클라우드런 URL(예: https://xxxx-uc.a.run.app)을 배포 시 환경변수로 넣어준다.
# (인스턴스마다 이 값만 다르게 설정하면 됨 — 나머지 코드/환경변수는 동일해도 무방)
MY_SERVICE_URL = os.environ.get("MY_SERVICE_URL", "").strip().rstrip("/")

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

    ✨ [추가됨] 음성 자동분석과 신입의 응답은 서로 다른 요청(타이밍)으로 들어오고
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


# 음성 확인이 모두 끝났을 때 보내는 최종 안내 문구. (format_voice_analysis_reply_text에서 사용)
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

# ==========================================
# [룸스테이트 — DB(Supabase room_states) 우선, DB 조회가 실패할 때만 구글시트 '룸스테이트' 탭 사용]
#   시트 탭: A: room_id / B: data(JSON) / C: updated_at(KST)
#   쓰기는 DB와 시트 양쪽에 모두 반영해 두고(시트 = 백업), /디비업데이트 시 시트 기준으로 DB를 맞춥니다.
# ==========================================
ROOM_STATE_SHEET_NAME = "룸스테이트"
ROOM_STATE_SHEET_CACHE_TTL = 3  # 초. DB 장애로 시트 폴백을 쓸 때 시트 읽기 횟수(API 할당량) 절약용
_room_state_sheet_cache = {}    # room_id -> (만료시각, data 또는 None)


def _init_room_state_sheet():
    try:
        try:
            return spreadsheet.worksheet(ROOM_STATE_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=ROOM_STATE_SHEET_NAME, rows=200, cols=3)
            ws.update(range_name='A1:C1', values=[["room_id", "data", "updated_at"]])
            return ws
    except Exception as e:
        print(f"⚠️ 룸스테이트 시트 연결 실패: {e}")
        return None


room_state_sheet = _init_room_state_sheet()


def _room_state_sheet_read(room_id):
    """시트에서 room_id 행의 data(dict)를 반환. 없으면 None. 시트 조회 자체가 실패하면 예외를 그대로 올림."""
    rows = room_state_sheet.get_all_values()
    for row in rows[1:]:
        if row and str(row[0]).strip() == room_id:
            raw = row[1] if len(row) > 1 else ""
            return json.loads(raw) if raw else None
    return None


def _room_state_sheet_write(room_id, data):
    if not room_state_sheet:
        return
    try:
        kst = datetime.timezone(datetime.timedelta(hours=9))
        now_str = datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M:%S")
        payload = [room_id, json.dumps(data, ensure_ascii=False), now_str]
        with sheet_sync_lock():
            ids = [str(v).strip() for v in room_state_sheet.col_values(1)]
            if room_id in ids:
                r = ids.index(room_id) + 1
                room_state_sheet.update(range_name=f'A{r}:C{r}', values=[payload], value_input_option='RAW')
            else:
                room_state_sheet.append_row(payload, value_input_option='RAW')
    except Exception as e:
        print(f"room_state 시트 저장 에러: {e}")


def _room_state_sheet_delete(room_id):
    if not room_state_sheet:
        return
    try:
        with sheet_sync_lock():
            ids = [str(v).strip() for v in room_state_sheet.col_values(1)]
            if room_id in ids:
                room_state_sheet.delete_rows(ids.index(room_id) + 1)
    except Exception as e:
        print(f"room_state 시트 삭제 에러: {e}")


def set_room_state(room_id, data, ttl=3600):
    room_states_memory[room_id] = data
    _room_state_sheet_cache.pop(room_id, None)
    if supabase:
        try:
            supabase.table('room_states').upsert({"room_id": room_id, "data": data}).execute()
        except Exception as e:
            print(f"room_state DB 저장 에러: {e}")
    _room_state_sheet_write(room_id, data)


def get_room_state(room_id, strict=False):
    """DB를 먼저 읽고, DB 조회가 '실패'했을 때만 시트를 읽습니다. (DB에 행이 없는 것은 실패가 아니라 '없음'입니다)
    strict=True면 DB와 시트 모두 실패했을 때 예외를 올립니다('N번방 확인'이 '없음'과 '조회 실패'를 구분하기 위함)."""
    db_ok = False
    if supabase:
        try:
            res = supabase.table('room_states').select('data').eq('room_id', room_id).execute()
            db_ok = True
            if res.data:
                return res.data[0]['data']
            return None
        except Exception as e:
            print(f"room_state DB 조회 에러 -> 시트로 폴백: {e}")

    if not db_ok and room_state_sheet:
        cached = _room_state_sheet_cache.get(room_id)
        if cached and cached[0] > time.time():
            return cached[1]
        try:
            data = _room_state_sheet_read(room_id)
            _room_state_sheet_cache[room_id] = (time.time() + ROOM_STATE_SHEET_CACHE_TTL, data)
            return data
        except Exception as e:
            print(f"room_state 시트 조회 에러: {e}")

    if strict and (supabase or room_state_sheet):
        raise RuntimeError("DB와 시트 모두에서 룸스테이트 조회에 실패했습니다.")
    return room_states_memory.get(room_id)


def del_room_state(room_id):
    room_states_memory.pop(room_id, None)
    _room_state_sheet_cache.pop(room_id, None)
    if supabase:
        try:
            supabase.table('room_states').delete().eq('room_id', room_id).execute()
        except Exception as e:
            print(f"room_state DB 삭제 에러: {e}")
    _room_state_sheet_delete(room_id)


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
    """'#번방' 이름 -> room_id. DB(room_management)를 먼저 보고, DB 조회가 '실패'했을 때만 '방관리' 시트(A열 방이름 / B열 room_id)를 봅니다."""
    target_name = room_name.replace(" ", "")
    db_ok = False
    if supabase:
        try:
            res = supabase.table('room_management').select('room_id').eq('room_name', target_name).execute()
            db_ok = True
            if res.data:
                return res.data[0]['room_id']
            return None
        except Exception as e:
            print(f"방 DB 조회 실패 -> 방관리 시트로 폴백: {e}")

    if not db_ok and room_manage_sheet:
        try:
            for row in room_manage_sheet.get_all_values()[1:]:
                if len(row) >= 2 and str(row[0]).replace(" ", "") == target_name and str(row[1]).strip():
                    return str(row[1]).strip()
        except Exception as e:
            print(f"방관리 시트 조회 실패: {e}")
    return None


def get_forward_target_url(raw_body):
    """이 웹훅 이벤트가 어느 방(그룹)에서 왔는지 확인해서, 그 방을 담당하는 클라우드런
    URL이 '방관리' 시트(=room_management 테이블, service_url 열)에 등록돼 있고
    그게 지금 이 서비스(MY_SERVICE_URL) 자신이 아니면 그 URL을 돌려준다.

    담당 URL을 못 찾거나, 그룹 이벤트가 아니거나, 담당이 곧 나 자신이면 None을 돌려주고
    (= 지금 이 서비스에서 그대로 처리), 조회 자체가 실패해도 None을 돌려줘서
    최소한 이 서비스에서라도 처리를 시도하게 한다 (라우팅 실패가 인증 자체를 막으면 안 되므로).
    """
    if not supabase or not MY_SERVICE_URL:
        return None
    try:
        payload = json.loads(raw_body)
        events = payload.get("events") or []
        if not events:
            return None
        source = events[0].get("source") or {}
        group_id = source.get("groupId") or source.get("roomId")
        if not group_id or group_id == ADMIN_GROUP_CHAT_ID:
            # 1:1 대화이거나 관리자방(공통) 이벤트는 라우팅하지 않고 받은 서비스가 그대로 처리
            return None
    except Exception as e:
        print(f"⚠️ 라우팅용 이벤트 파싱 실패(이 서비스에서 직접 처리): {e}")
        return None

    res = supabase_execute(
        lambda: supabase.table('room_management').select('service_url').eq('room_id', group_id).limit(1).execute(),
        label="방 담당 서비스 URL 조회"
    )
    rows = getattr(res, "data", None) or []
    if not rows:
        return None

    target_url = (rows[0].get('service_url') or "").strip().rstrip("/")
    if not target_url or target_url == MY_SERVICE_URL:
        return None

    return f"{target_url}/api"


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
        # ✨ [추가됨] 새 FLIRTY 양식 항목 (T열 쓰던닉 / U열 야방 방이름)
        "prev_nick": find_header_col_index(header_row, ["쓰던닉"]),
        "yadan_room": find_header_col_index(header_row, ["방이름"]),
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
    - require_status를 지정하면 유저의 현재 status가 그 값일 때만 진행합니다.
    - require_status=None이면 현재 status와 무관하게 강제로 진행합니다. (관리자 수동 트리거용: /ㅇㅈ 음성인증, /ㅇㅅㅇㅈ, 재입장 이어가기)
    반환값: (성공여부, 안내 멘트 또는 None)

    ✨ [변경됨] 새 FLIRTY 흐름: "오늘 날짜 + 닉네임"을 라인 음성으로 녹음하도록 안내합니다.
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

    user_nickname = u_data.get('nickname', '신입')
    reply_text = build_voice_instruction(user_nickname)

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
# ✨ 닉네임 변경 플로우용 헬퍼 함수
# ==========================================
def build_all_mention_message(text_body):
    """방 전체(@전체)를 멘션하면서 안내 문구를 붙인 메시지 객체를 만듭니다.
    설치된 line-bot-sdk 버전이 텍스트 메시지 v2(멘션)를 지원하지 않으면 멘션 없이 일반 텍스트로 대체합니다."""
    if _MENTION_SUPPORTED:
        try:
            return TextMessageV2(
                text="{everyone} " + text_body,
                substitution={
                    # LINE 규칙: substitution 키는 영문/숫자/_ (1~20자)만 허용 → 한글 키("전체")는 400 에러
                    "everyone": MentionSubstitutionObject(
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


def is_nickname_changed_correctly(source_id, user_id, birth_year, nickname, gender=""):
    """LINE 표시명에 닉네임 + 생년 윗첨자(+여자는 ✿)가 반영됐는지 느슨하게 확인합니다."""
    current_name = get_current_display_name(source_id, user_id)
    short_year = _norm_year(birth_year)
    if not current_name or not short_year or not nickname:
        return False
    cur = re.sub(r"\s", "", current_name)
    if str(nickname).strip() not in cur:
        return False
    if to_superscript(short_year) not in cur:
        return False
    if normalize_gender(gender) == "여" and "✿" not in cur:
        return False
    return True


def has_blacklist_issue(user_id):
    """DB에 저장된 음성 자동분석 결과 기준으로 블랙리스트 유사 매칭(강한 의심)이 남아있는지 재확인합니다."""
    row = get_validation_row(user_id, "voice_analysis_result")
    voice_result = drop_self_matches(row.get('voice_analysis_result') or {}, user_id) or {}
    matches = voice_result.get('matches') or []
    # 블랙리스트 음성뿐 아니라 기존 신입 음성과 강하게 유사한 경우(다른 닉/계정 재입장 의심)도 운영진 검토 대상
    return any((m.get('similarity') or 0) >= VOICE_MATCH_ALERT_THRESHOLD for m in matches)


def is_yadan_none(yadan_text):
    """'야방경험(야단라경험)' 답변이 '무/없음' 계열인지 판별합니다.
    (예: 무, 없음, 없어요, 없습니다, x, no, 해당없음, -)  비어 있어도 '없음'으로 봅니다."""
    t = re.sub(r"[\s\.\,\!\~\-\_\(\)\[\]]", "", str(yadan_text or "")).lower()
    if t == "":
        return True
    if t in ("무", "x", "no", "none", "없", "해당없음", "해당사항없음", "ㅇㅇ없음", "ㄴㄴ"):
        return True
    return t.startswith("없") and len(t) <= 8


def is_clean_new_user(user_id):
    """1번 양식 제출 시점의 중복/블랙 필터링 결과가 '이상 없음(깨끗한 신규)'이었는지 확인합니다.
    기록이 없거나(이전 버전에서 진행 중이던 유저 등) 판단 불가하면 안전하게 False(운영진 확인)로 처리합니다."""
    details = get_validation_row(user_id, "details").get('details') or {}
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except Exception:
            details = {}
    return isinstance(details, dict) and details.get('dup_clean') is True


def build_nickchange_messages(user_id):
    """닉변 안내를 2개 메시지로 돌려줍니다: [1] 변경할 닉네임만 단독(복사하기 쉽게), [2] 닉변/프로필 안내 문구."""
    nick_only, guide_text = build_nickchange_parts(user_id)
    messages = []
    if nick_only:
        messages.append(TextMessage(text=nick_only))
    messages.append(TextMessage(text=guide_text))
    return messages


def build_nickchange_text(user_id):
    """(호환용) 안내 문구만 돌려줍니다."""
    return build_nickchange_parts(user_id)[1]


def build_nickchange_parts(user_id):
    """(변경할 닉네임 단독 문자열, 안내 문구) — 닉네임을 먼저, 안내는 그 다음 메시지로 보낸다.
    남자: 닉네임 + 생년2자리 윗첨자 / 여자: 닉네임 + 생년2자리 윗첨자 + ✿ 장식"""
    row = get_validation_row(user_id, "nickname, birth_year, gender")
    nickname = row.get('nickname') or ""
    nick_only = build_display_name(nickname, row.get('birth_year'), row.get('gender')) if nickname else ""
    return nick_only, NICKCHANGE_GUIDE_TEXT


# ==========================================
# ✨ 닉네임 수동 변경('/새닉 변경') + 재제출 이력/비교
#   - 신입이 다른 닉네임으로 바꾸기로 하면, 운영진이 그 방에서 '/바뀐닉 변경'을 입력해 DB 닉네임을 직접 수정합니다.
#   - user_validations.details(JSON)에 아래 값들을 쌓습니다. (컬럼/테이블 추가 불필요)
#       form_nickname : 양식에 적었던 닉네임 (중복검사/비교용, 운영진이 바꿔도 보존)
#       nick_changes  : 운영진이 닉네임을 바꾼 기록 [{at, from, to, by}]
#       history       : 재제출 때마다 이전 양식 스냅샷 (최근 5개)
# ==========================================
DIFF_DETAIL_FIELDS = [
    ("marriage", "결혼유무"), ("yadan", "야방경험"),
    ("yadan_room", "야방 방이름"), ("prev_nick", "쓰던닉"),
]

# 예전 기록(나온이유/킥이력/초대자 등)도 같이 보여주기 위한 전체 라벨 목록
DETAIL_LABELS = [
    ("yadan_room", "야방 방이름"), ("prev_nick", "쓰던닉"),
    ("leave_reason", "나온이유"), ("kick_reason", "킥이력"),
    ("yadan", "야방경험"), ("inviter", "초대자"),
    ("marriage", "결혼유무"), ("military", "군필여부"), ("age", "나이"),
]


def _load_details(raw):
    """details가 문자열(JSON)이어도 dict로 안전하게 변환"""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}
    return raw if isinstance(raw, dict) else {}


def _norm_year(y):
    """1994 / 94 / '94년생' / '94 년생' 모두 숫자만 뽑아 뒤 2자리('94')로 맞춤. 숫자가 없으면 빈 문자열."""
    return re.sub(r"\D", "", str(y or ""))[-2:]


def _year_label(y):
    """화면 표시용: 2자리로 정규화된 값을 돌려주고, 숫자가 없으면 원문(없으면 '-')을 그대로 보여줌.
    (예전에 '94년생'으로 저장된 기록도 '94년생년생'으로 겹쳐 보이지 않게 함)"""
    return _norm_year(y) or (str(y).strip() if y else "-")


# ==========================================
# ✨ FLIRTY 새 양식/표시명용 헬퍼
# ==========================================
_SUPERSCRIPT_TABLE = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")
FEMALE_NAME_SUFFIX = "͙͘͡✿"   # 여자 표시명 뒤에 붙는 장식 (복사한 문자열 그대로)


def to_superscript(s):
    return str(s or "").translate(_SUPERSCRIPT_TABLE)


def build_display_name(nickname, birth_year, gender):
    """남: 닉네임 + 생년2자리 윗첨자 / 여: 거기에 ✿ 장식 추가"""
    name = f"{str(nickname or '').strip()}{to_superscript(_norm_year(birth_year))}"
    if normalize_gender(gender) == "여":
        name += FEMALE_NAME_SUFFIX
    return name


def _pick_field(data, exact=None, contains=()):
    """양식 dict에서 값을 찾는다. exact(정확히 일치하는 키) 우선, 없으면 contains 중 하나를 포함한 키."""
    if exact and exact in data:
        return str(data[exact]).strip()
    for k, v in data.items():
        if any(c in k for c in contains):
            return str(v).strip()
    return ""


def _clean_yes_no(v):
    """'(유/무) 유' → '유', '없음' → '무'"""
    t = re.sub(r"\(\s*유\s*/\s*무\s*\)", "", str(v or "")).strip()
    if not t:
        return ""
    if t[0] == "유":
        return "유"
    if t[0] == "무" or t.startswith("없"):
        return "무"
    return t


def _is_none_word(v):
    t = re.sub(r"[\s\.\,\!\~\-\_]", "", str(v or "")).lower()
    return t in ("", "없음", "없다", "없어요", "없습니다", "무", "x", "no", "none", "해당없음")


def build_voice_instruction(nickname):
    kst = datetime.timezone(datetime.timedelta(hours=9))
    today = datetime.datetime.now(kst)
    return (
        "🎤 라인에서 음성 녹음 하셔야합니다.\n"
        "( ex : 7월 20일 모모 )\n\n"
        f"👉 \"{today.month}월 {today.day}일 {nickname}\" 이라고 녹음해 주세요.\n\n"
        "조용한 곳에서 천천히 또박또박 부탁드립니다."
    )


NICKCHANGE_GUIDE_TEXT = (
    "닉네임은 두글자에서 세글자로 해주세요\n"
    "프로필은 얼굴사진 아니여도 됩니다 무조건 프로필 설정 해주세요\n"
    "완료되시면 \"완료\"라고 입력해 주세요"
)


def _kst_now_str(fmt="%Y-%m-%d %H:%M"):
    kst = datetime.timezone(datetime.timedelta(hours=9))
    return datetime.datetime.now(kst).strftime(fmt)


def diff_with_previous(old_row, new):
    """이전 제출(old_row: user_validations 행)과 이번 제출(new: dict)을 비교합니다.
    반환: (달라진 항목 줄 리스트, 성별/년생 불일치 여부)"""
    od = _load_details(old_row.get('details'))
    lines, critical = [], False

    og = _GENDER_NORM_MAP.get(str(old_row.get('gender') or '').strip())
    ng = _GENDER_NORM_MAP.get(str(new.get('gender') or '').strip())
    if og and ng and og != ng:
        lines.append(f"성별: {og} → {ng} ⚠️")
        critical = True

    oy, ny = _norm_year(old_row.get('birth_year')), _norm_year(new.get('birth_year'))
    if oy and oy != ny:
        lines.append(f"년생: {_year_label(old_row.get('birth_year'))} → {_year_label(new.get('birth_year'))} ⚠️")
        critical = True

    # 닉네임은 '양식에 적었던 값'끼리 비교 (운영진이 닉변해 둔 값과 섞이지 않게)
    old_form_nick = str(od.get('form_nickname') or old_row.get('nickname') or '').strip()
    new_nick = str(new.get('nickname') or '').strip()
    if old_form_nick and old_form_nick != new_nick:
        lines.append(f"닉네임: {old_form_nick} → {new_nick}")

    o_reg = re.sub(r"\s", "", str(old_row.get('region') or ''))
    n_reg = re.sub(r"\s", "", str(new.get('region') or ''))
    if o_reg and o_reg != n_reg:
        lines.append(f"지역: {old_row.get('region')} → {new.get('region')}")

    for key, label in DIFF_DETAIL_FIELDS:
        a, b = str(od.get(key, '')).strip(), str(new.get(key, '')).strip()
        if a and a != b:
            # 과거엔 있었다고 했는데 이번엔 '없음'으로 바꾼 경우는 강조
            warn = " ⚠️ (이전에는 있음 → 이번엔 없음)" if key in ("yadan", "kick_reason") and is_yadan_none(b) and not is_yadan_none(a) else ""
            lines.append(f"{label}: {a} → {b}{warn}")

    changes = od.get('nick_changes') or []
    if changes:
        lines.append("(이전 운영진 닉변 이력: " + " / ".join(f"{c.get('from')}→{c.get('to')}" for c in changes[-3:]) + ")")
    return lines, critical


def sync_nickname_to_sheet(user_id, nickname):
    """검증 시트 A열(닉네임)만 동기화합니다. 실패해도 무시합니다."""
    try:
        with sheet_sync_lock():
            if validation_sheet:
                ids = [str(v).strip() for v in validation_sheet.col_values(5)]
                if user_id in ids:
                    validation_sheet.update(range_name=f'A{ids.index(user_id) + 1}', values=[[nickname]])
    except Exception as e:
        print(f"닉네임 시트 동기화 에러: {e}")


def find_nickname_conflicts(nick, exclude_uid):
    """새 닉네임과 같은 '다른 유저'의 검증기록/음성DB 기록을 줄 리스트로 돌려줍니다.
    (user_id가 NULL인 행도 놓치지 않도록 파이썬에서 필터링)"""
    out = []
    if not supabase or not nick:
        return out
    r1 = supabase_execute(
        lambda: supabase.table('user_validations').select('user_id, nickname, birth_year, gender, black_reason').eq('nickname', nick).execute(),
        label="새 닉네임 중복조회(검증)"
    )
    for r in (r1.data if r1 and r1.data else []):
        if str(r.get('user_id') or '') == str(exclude_uid):
            continue
        black = f" / 💀 블랙사유: {str(r['black_reason']).strip()}" if str(r.get('black_reason') or '').strip() else ""
        out.append(f" - 검증기록: {r.get('nickname')} / {_year_label(r.get('birth_year'))}년생 / {r.get('gender') or '-'}{black}")
    r2 = supabase_execute(
        lambda: supabase.table('voice_profiles').select('user_id, nickname, source').eq('nickname', nick).execute(),
        label="새 닉네임 중복조회(음성DB)"
    )
    for r in (r2.data if r2 and r2.data else []):
        if str(r.get('user_id') or '') == str(exclude_uid):
            continue
        kind = "신입 음성인증" if r.get('source') == 'new_member' else "블랙리스트 음성 등록"
        out.append(f" - 🎙️ 음성DB: {kind} ({r.get('nickname')})")
    return out


def apply_nickname_change(source_id, sender_user_id, new_nick):
    """'/새닉 변경' 처리. 이 방에서 인증 진행 중인 신입의 DB 닉네임을 new_nick으로 바꾸고 이력을 남깁니다.
    반환: 방에 reply할 TextMessage 리스트 (신입 본인이 입력했으면 빈 리스트 = 무시)"""
    target = get_room_target_user_id(source_id)
    if not target:
        return [TextMessage(text="❌ 현재 이 방에서 인증 진행 중인 신입 유저를 찾을 수 없습니다.")]
    if target == sender_user_id:
        return []  # 신입이 스스로 못 바꾸게 무시

    row = get_validation_row(target, "nickname, birth_year, gender, status, details")
    if not row:
        return [TextMessage(text="❌ 해당 유저의 인증 정보를 DB에서 찾을 수 없습니다.")]

    # 표시명 전체('아이언⁸⁶')를 붙여 넣어도 닉네임만 남기기
    new_nick = re.sub(r"[⁰¹²³⁴⁵⁶⁷⁸⁹\u0358\u0359\u0361✿\uFE0F]+$", "", new_nick).strip()
    if not new_nick:
        return [TextMessage(text="사용법: /바뀐닉네임 변경")]

    old_nick = row.get('nickname') or ""
    if new_nick == old_nick:
        return [TextMessage(text=f"ℹ️ 이미 '{new_nick}'(으)로 저장되어 있습니다.")]

    details = _load_details(row.get('details'))
    details.setdefault('form_nickname', old_nick)              # 양식에 적었던 닉네임 보존
    changes = list(details.get('nick_changes') or [])
    changes.append({"at": _kst_now_str(), "from": old_nick, "to": new_nick, "by": sender_user_id})
    details['nick_changes'] = changes[-5:]

    conflicts = find_nickname_conflicts(new_nick, target)
    if conflicts:
        # 새 닉네임이 기존 기록과 겹치면 마지막에 자동 '퇴장' 멘트 대신 운영진 추가확인으로 보낸다
        details['dup_clean'] = False

    res = supabase_execute(
        lambda: supabase.table('user_validations').update({"nickname": new_nick, "details": details}).eq('user_id', target).execute(),
        label="닉네임 수동 변경"
    )
    if res is None:
        return [TextMessage(text="⚠️ 닉네임 변경 저장에 실패했습니다. 잠시 후 다시 시도해 주세요.")]

    sess = get_user_session(target)
    if isinstance(sess, dict):
        sess["nickname"] = new_nick
        set_user_session(target, sess)
    sync_nickname_to_sheet(target, new_nick)

    # 충돌 상세는 신입이 있는 방에 노출하지 않고 운영진방으로만 알린다
    if conflicts:
        alert = (f"🔁 닉네임 수동 변경 알림\n- {old_nick} → {new_nick}\n- 유저ID: {target}\n\n"
                 f"⚠️ 새 닉네임과 같은 기존 기록이 있습니다:\n" + "\n".join(conflicts) +
                 "\n\n※ 자동 퇴장 멘트 대신 '운영진 추가 확인' 안내로 진행되니 직접 확인해 주세요.")
        try:
            with ApiClient(configuration) as api_client:
                MessagingApi(api_client).push_message_with_http_info(
                    PushMessageRequest(to=ADMIN_GROUP_CHAT_ID, messages=[TextMessage(text=alert[:4900])])
                )
        except Exception as e:
            print(f"⚠️ 닉네임 변경 충돌 알림 전송 실패: {e}")

    hint = {"닉변대기": "변경 후 '완료'"}.get(row.get('status'), "변경 후 안내에 따라 답장")
    return [
        TextMessage(text=f"✅ 닉네임을 '{new_nick}'(으)로 변경 처리했습니다.\n아래 닉네임으로 바꾸시고 프로필도 설정한 뒤 {hint}을(를) 입력해 주세요."),
        TextMessage(text=build_display_name(new_nick, row.get('birth_year'), row.get('gender'))),
    ]


# ==========================================
# ✨ 기존 멤버 수동 등록 ('/기존멤버등록')
#   1) 인증자방에서 '/기존멤버등록' 입력 → 봇이 양식 제출 요청
#   2) 양식 제출 → user_validations에 등록(가짜 user_id 'EM_...', status '기존멤버')
#   3) 봇이 음성 파일 요청 → 음성 제출 → 신입과 똑같은 분석/저장 루틴(submit_voice_analysis_job)
#      (voice_profiles.source는 신입과 같은 'new_member'로 저장되어, 이후 신입 대조 시 '기존 신입 음성'으로 잡힘)
#   결과 확인은 '/닉네임 확인'
# ==========================================
MEMBER_REGISTER_TTL_SEC = 1800   # 양식/음성 대기 제한(30분). 지나면 대기 상태를 버린다.
MEMBER_FORM_TEMPLATE = (
    "닉네임:\n"
    "년생:\n"
    "나이:\n"
    "성별:\n"
    "지역:\n"
    "결혼유무:\n"
    "군필여부:\n"
    "초대자:\n"
    "야단라경험유무:\n"
    "기존 다른방에서 나온이유:\n"
    "다른 방에서 킥을 당한적 있는지:"
)


def parse_signup_form(text):
    """'키: 값' 줄 형태의 양식 텍스트를 dict로 파싱 (1번 양식 제출 처리와 같은 규칙)."""
    data = {}
    for line in str(text or "").split("\n"):
        delimiter = ":" if ":" in line else ("：" if "：" in line else None)
        if not delimiter:
            continue
        k, v = line.split(delimiter, 1)
        k = k.replace("-", "").strip()
        if "(" in k:
            k = k.split("(", 1)[0].strip()
        if "/" in k:
            k = k.split("/", 1)[0].strip()
        data[k] = v.strip()
    return data


def _sheet_append_validation_row(nickname, gender, region, birth_year, user_id, entry_date, status, details_list):
    """검증 시트에 한 줄 백업(실패해도 무시). 열 배치는 1번 양식 제출 때와 같다."""
    try:
        with sheet_sync_lock():
            if validation_sheet:
                idx = len(validation_sheet.get_all_values()) + 1
                validation_sheet.update(range_name=f'A{idx}:H{idx}', values=[[nickname, gender, region, birth_year, user_id, entry_date, "", 1]])
                validation_sheet.update(range_name=f'K{idx}:L{idx}', values=[[user_id, status]])
                validation_sheet.update(range_name=f'M{idx}:S{idx}', values=[details_list])
    except Exception as e:
        print(f"기존멤버 등록 - 시트 백업 에러(무시): {e}")


def register_existing_member(admin_user_id, extracted, force_new=False):
    """양식(dict)을 user_validations에 등록하고, 음성 대기 상태로 전환한다. 반환: reply TextMessage 리스트.
    필수 항목이 비면 등록하지 않고 안내만 한다(대기 상태는 유지되어 다시 제출하면 됨)."""
    nickname = extracted.get("닉네임", "").strip()
    birth_year = _norm_year(extracted.get("년생", ""))
    gender = normalize_gender(extracted.get("성별", ""))
    region = extracted.get("지역", "").strip()

    missing = []
    if not nickname: missing.append("닉네임")
    if not birth_year: missing.append("년생(숫자로 작성, 예: 94)")
    if not gender: missing.append("성별")
    if not region: missing.append("지역")
    if missing:
        return [TextMessage(text=f"⚠️ 다음 항목이 비어 있습니다: {', '.join(missing)}\n\n채워서 양식을 다시 제출해 주세요. (취소: /기존멤버등록 취소)")]
    if not supabase:
        return [TextMessage(text="⚠️ DB 연결이 없어 등록할 수 없습니다.")]

    # ✨ 같은 닉네임의 기존 기록이 있으면 바로 등록하지 않고, 관련 기록을 보여준 뒤 '같은 사람인지' 먼저 확인한다.
    # (운영진이 번호로 연결하거나 '새로'로 새 기록을 만든다. force_new=True면 이 확인을 건너뜀)
    if not force_new:
        cands = find_member_candidates(nickname)
        if cands:
            set_user_session(admin_user_id, {"pending_member_register": {
                "stage": "confirm", "form": extracted,
                "candidate_ids": [str(c.get("user_id")) for c in cands], "at": time.time(),
            }})
            return [TextMessage(text=t) for t in split_for_line(format_member_candidates_message(nickname, cands, extracted))]

    details = {
        "age": extracted.get("나이", "").strip(),
        "marriage": extracted.get("결혼유무", "").strip(),
        "military": extracted.get("군필여부", "").strip(),
        "inviter": extracted.get("초대자", "").strip(),
        "yadan": extracted.get("야단라경험유무", "").strip(),
        "leave_reason": extracted.get("기존 다른방에서 나온이유", "").strip(),
        "kick_reason": extracted.get("다른 방에서 킥을 당한적 있는지", "").strip(),
        "form_nickname": nickname,
        "registered_by": admin_user_id,
        "source": "existing_member_register",
    }
    new_uid = f"EM_{int(time.time())}_{random.randint(1000, 9999)}"
    entry_date = _kst_now_str("%Y-%m-%d")
    conflicts = [] if force_new else find_nickname_conflicts(nickname, None)

    res = supabase_execute(
        lambda: supabase.table('user_validations').insert({
            "user_id": new_uid, "nickname": nickname, "gender": gender, "region": region,
            "birth_year": birth_year, "entry_date": entry_date, "retry_count": 1,
            "status": "기존멤버", "details": details,
        }).execute(),
        label="기존멤버 등록"
    )
    if res is None:
        return [TextMessage(text="⚠️ DB 등록에 실패했습니다. 잠시 후 양식을 다시 제출해 주세요.")]

    _sheet_append_validation_row(
        nickname, gender, region, birth_year, new_uid, entry_date, "기존멤버",
        [details["age"], details["marriage"], details["military"], details["inviter"],
         details["yadan"], details["leave_reason"], details["kick_reason"]],
    )

    set_user_session(admin_user_id, {"pending_member_register": {
        "stage": "voice", "user_id": new_uid, "nickname": nickname, "gender": gender, "at": time.time(),
    }})

    lines = [
        "✅ 기존 멤버를 DB에 등록했습니다.",
        f"- {nickname} / {birth_year}년생 / {gender} / {region}",
        f"- 등록 ID: {new_uid}",
    ]
    if conflicts:
        lines.append("\n⚠️ 같은 닉네임의 기존 기록이 있습니다 (중복 등록 여부 확인):")
        lines.extend(conflicts[:5])
    lines.append("\n🎤 이제 분석할 음성 파일을 보내주세요. (30분 안에 · 취소: /기존멤버등록 취소)")
    return [TextMessage(text="\n".join(lines))]


def find_member_candidates(nickname, limit=5):
    """같은 닉네임(또는 양식에 적었던 닉네임)의 기존 검증 기록을 찾는다."""
    cols = 'user_id, nickname, gender, region, birth_year, status, entry_date, black_reason, details'
    found = {}
    for col in ('nickname', 'details->>form_nickname'):
        res = supabase_execute(
            lambda c=col: supabase.table('user_validations').select(cols).eq(c, nickname).execute(),
            label=f"기존멤버 후보 조회({col})"
        )
        for r in (res.data if res and res.data else []):
            found.setdefault(str(r.get('user_id')), r)
    return list(found.values())[:limit]


def _form_to_compare(form):
    """양식 dict(한글 키)를 diff_with_previous가 받는 형태로 바꾼다."""
    return {
        "nickname": form.get("닉네임", "").strip(),
        "birth_year": _norm_year(form.get("년생", "")),
        "gender": normalize_gender(form.get("성별", "")),
        "region": form.get("지역", "").strip(),
        "marriage": form.get("결혼유무", "").strip(),
        "military": form.get("군필여부", "").strip(),
        "inviter": form.get("초대자", "").strip(),
        "yadan": form.get("야단라경험유무", "").strip(),
        "leave_reason": form.get("기존 다른방에서 나온이유", "").strip(),
        "kick_reason": form.get("다른 방에서 킥을 당한적 있는지", "").strip(),
        "age": form.get("나이", "").strip(),
    }


def format_member_candidates_message(nickname, cands, form):
    """후보 기록들을 번호와 함께 보여주고, 이번 양식과 달라진 점을 같이 표시한다."""
    new_form = _form_to_compare(form)
    lines = [f"🔎 '{nickname}'와(과) 같은 기록이 {len(cands)}건 있습니다. 같은 사람인지 확인해 주세요.\n"]
    for i, c in enumerate(cands, 1):
        d = _load_details(c.get('details'))
        lines.append(f"[{i}] {c.get('nickname') or '-'} / {_year_label(c.get('birth_year'))}년생 / {c.get('gender') or '-'} / {c.get('region') or '-'}")
        lines.append(f" - 상태: {c.get('status') or '-'} / 입장일: {c.get('entry_date') or '-'}")
        if str(c.get('black_reason') or '').strip():
            lines.append(f" - 💀 블랙사유: {str(c.get('black_reason')).strip()}")
        info = []
        for key, label in DETAIL_LABELS:
            val = str(d.get(key, "")).strip()
            if val:
                info.append(f"{label}: {val}")
        if info:
            lines.append(f" - 📝 {' / '.join(info)}")
        diff_lines, _ = diff_with_previous(c, new_form)
        if diff_lines:
            lines.append(" - 🔀 이번 양식과 다른 점:\n   " + "\n   ".join(diff_lines))
        else:
            lines.append(" - ✅ 이번 양식과 주요 항목이 같음")
        lines.append("")
    n = len(cands)
    lines.append(f"▶ 같은 사람이면 /1 ~ /{n} (번호) 입력 → 그 기록과 연결\n▶ 다른 사람이면 /새로 → 새 기록으로 등록\n▶ 취소: /취소")
    return "\n".join(lines)


def link_existing_member(admin_user_id, target_uid, form):
    """선택한 기존 기록에 이번 양식을 연결한다: 이전 값은 details.history에 남기고, 새 양식 값으로 갱신.
    status/black_reason/entry_date/닉네임은 그대로 둔다. 이후 음성 대기 상태로 전환."""
    row = get_validation_row(target_uid, "user_id, nickname, gender, region, birth_year, status, entry_date, black_reason, details")
    if not row:
        return [TextMessage(text="⚠️ 선택한 기록을 DB에서 찾지 못했습니다. '/기존멤버등록'부터 다시 진행해 주세요.")]

    new_form = _form_to_compare(form)
    diff_lines, critical = diff_with_previous(row, new_form)

    d = _load_details(row.get('details'))
    history = list(d.get('history') or [])
    history.append({
        "at": _kst_now_str("%Y-%m-%d"),
        "nickname": row.get('nickname'),
        "form_nickname": d.get('form_nickname') or row.get('nickname'),
        "birth_year": row.get('birth_year'), "gender": row.get('gender'), "region": row.get('region'),
        "age": d.get('age'),
        **{k: d.get(k) for k, _ in DIFF_DETAIL_FIELDS},
    })
    d['history'] = history[-5:]
    for k in ("age", "marriage", "military", "inviter", "yadan", "leave_reason", "kick_reason"):
        if new_form.get(k):
            d[k] = new_form[k]
    d.setdefault('form_nickname', row.get('nickname'))
    links = list(d.get('member_links') or [])
    links.append({"at": _kst_now_str(), "by": admin_user_id})
    d['member_links'] = links[-5:]

    res = supabase_execute(
        lambda: supabase.table('user_validations').update({
            "gender": new_form["gender"], "region": new_form["region"],
            "birth_year": new_form["birth_year"], "details": d,
        }).eq('user_id', target_uid).execute(),
        label="기존멤버 기록 연결"
    )
    if res is None:
        return [TextMessage(text="⚠️ 기록 연결(갱신)에 실패했습니다. 잠시 후 번호를 다시 입력해 주세요.")]

    nickname = row.get('nickname') or ""
    set_user_session(admin_user_id, {"pending_member_register": {
        "stage": "voice", "user_id": target_uid, "nickname": nickname, "gender": new_form["gender"], "at": time.time(),
    }})

    lines = [
        "🔗 기존 기록과 연결했습니다.",
        f"- {nickname} / {new_form['birth_year']}년생 / {new_form['gender']} / {new_form['region']}",
        f"- 연결된 ID: {target_uid}",
        f"- 상태: {row.get('status') or '-'} (그대로 유지)",
    ]
    if str(row.get('black_reason') or '').strip():
        lines.append("- 💀 블랙리스트 기록과 연결되었습니다 (사유/상태는 그대로 유지)")
    if diff_lines:
        lines.append("- 🔀 이전 값과 달라서 갱신한 내용 (이전 값은 이력에 보관):\n   " + "\n   ".join(diff_lines))
    if critical:
        lines.append("⚠️ 성별/년생이 이전 기록과 다릅니다. 같은 사람이 맞는지 한 번 더 확인해 주세요.")
    lines.append("\n🎤 이제 분석할 음성 파일을 보내주세요. (30분 안에 · 취소: /기존멤버등록 취소)")
    return [TextMessage(text=t) for t in split_for_line("\n".join(lines))]


def start_member_register(admin_user_id, rest):
    """'/기존멤버등록 [취소 | 양식]' 처리. 반환: reply TextMessage 리스트."""
    rest = (rest or "").strip()
    if rest in ("취소", "취소하기", "cancel"):
        del_user_session(admin_user_id)
        return [TextMessage(text="🗑️ 기존 멤버 등록을 취소했습니다.")]

    # 같은 메시지에 양식을 같이 붙여 보냈으면 바로 등록
    if all(k in rest for k in ("닉네임", "년생", "성별", "지역")):
        return register_existing_member(admin_user_id, parse_signup_form(rest))

    set_user_session(admin_user_id, {"pending_member_register": {"stage": "form", "at": time.time()}})
    return [
        TextMessage(text="📝 기존 멤버의 양식을 제출해 주세요.\n아래 양식을 복사해 채워서 보내면 됩니다. (닉네임/년생/성별/지역은 필수)\n취소: /기존멤버등록 취소"),
        TextMessage(text=MEMBER_FORM_TEMPLATE),
    ]


def _get_pending_member_register(admin_user_id, stage):
    """대기 중인 등록 상태를 돌려준다. 단계가 다르거나 시간이 지났으면 None(지났으면 상태도 삭제)."""
    sess = get_user_session(admin_user_id)
    pending = sess.get("pending_member_register") if isinstance(sess, dict) else None
    if not pending or pending.get("stage") != stage:
        return None
    if time.time() - float(pending.get("at") or 0) > MEMBER_REGISTER_TTL_SEC:
        del_user_session(admin_user_id)
        return None
    return pending


def handle_member_register_form(event, admin_user_id, text):
    """인증자방에서 '/기존멤버등록' 후 제출된 양식 처리. 처리했으면 True."""
    if not _get_pending_member_register(admin_user_id, "form"):
        return False
    if not all(k in text for k in ("닉네임", "년생", "성별", "지역")):
        return False   # 양식이 아닌 일반 대화는 무시
    messages = register_existing_member(admin_user_id, parse_signup_form(text))
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message_with_http_info(
            ReplyMessageRequest(reply_token=event.reply_token, messages=messages)
        )
    return True


def handle_member_register_confirm(event, admin_user_id, text):
    """동일 기록 확인 질문에 대한 답. '/'로 시작하는 메시지만 인식한다: /번호(연결) · /새로(새 기록) · /취소.
    인식해서 처리했으면 True. 인식하지 못하면 False (다른 명령어가 그대로 동작하도록 하고, 마지막까지
    아무 명령에도 걸리지 않았을 때만 send_member_confirm_guide가 안내한다)."""
    pending = _get_pending_member_register(admin_user_id, "confirm")
    if not pending:
        return False
    raw = (text or "").strip()
    if not raw.startswith("/"):
        return False
    t = re.sub(r"\s", "", raw[1:])
    cand_ids = pending.get("candidate_ids") or []
    form = pending.get("form") or {}

    if t.isdigit() and 1 <= int(t) <= len(cand_ids):
        messages = link_existing_member(admin_user_id, cand_ids[int(t) - 1], form)
    elif t in ("새로", "신규", "새로작성", "새기록", "새로등록", "아니", "아니오", "아니요", "다른사람"):
        messages = register_existing_member(admin_user_id, form, force_new=True)
    elif t in ("취소", "취소하기"):
        del_user_session(admin_user_id)
        messages = [TextMessage(text="🗑️ 기존 멤버 등록을 취소했습니다.")]
    else:
        return False

    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message_with_http_info(
            ReplyMessageRequest(reply_token=event.reply_token, messages=messages)
        )
    return True


def send_member_confirm_guide(event, admin_user_id):
    """확인 대기 중에 '/'로 입력했지만 어떤 답변·명령에도 해당하지 않을 때, 가능한 답변을 안내한다."""
    pending = _get_pending_member_register(admin_user_id, "confirm")
    if not pending:
        return False
    n = len(pending.get("candidate_ids") or [])
    guide = (
        "⚠️ 답변을 인식하지 못했습니다. 아래 중 하나로 입력해 주세요.\n\n"
        f"▶ /1 ~ /{n} : 그 번호의 기록과 연결\n"
        "▶ /새로 : 새 기록으로 등록\n"
        "▶ /취소 : 기존 멤버 등록 취소"
    )
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message_with_http_info(
            ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=guide)])
        )
    return True


def handle_member_register_voice(event, admin_user_id):
    """인증자방에서 양식 등록 직후 제출된 음성 처리: 신입과 같은 분석/저장 루틴에 접수. 처리했으면 True."""
    pending = _get_pending_member_register(admin_user_id, "voice")
    if not pending:
        return False
    del_user_session(admin_user_id)   # 1회성 (중복 처리 방지)

    target_uid = pending.get("user_id")
    nickname = pending.get("nickname") or ""
    submit_voice_analysis_job(
        supabase, configuration,
        message_id=event.message.id, user_id=target_uid, nickname=nickname,
        room_id=ADMIN_GROUP_CHAT_ID,
    )
    reply_text = (
        f"🎤 [{nickname}] 음성이 접수되었습니다. 분석과 저장은 신입과 같은 방식으로 진행됩니다.\n"
        f"잠시 후 '/{nickname} 확인'으로 분석 결과(성별 추정/기존 음성 대조)를 확인할 수 있습니다."
    )
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message_with_http_info(
            ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
        )
    return True


def send_reply_or_push(reply_token, source_id, messages):
    """reply로 먼저 보내고, 실패하면 그룹으로 push로 재시도합니다. 성공 여부 반환(실패 사유는 로그)."""
    try:
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message_with_http_info(
                ReplyMessageRequest(reply_token=reply_token, messages=messages)
            )
        return True
    except Exception as e:
        print(f"⚠️ reply 발송 실패(push로 재시도): {e}")
    try:
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message_with_http_info(
                PushMessageRequest(to=source_id, messages=messages)
            )
        return True
    except Exception as e:
        print(f"⚠️ push 발송도 실패: {e}")
        return False


# def check_voice_gate(user_id):
#     """'완료' 시점에 음성 자동분석 상태를 본다: ok(완료) / wait(진행 중) / review(에러·멈춤)"""
#     row = get_validation_row(user_id, "voice_analysis_state, voice_analysis_synced_at")
#     state = row.get('voice_analysis_state')
#     if state == "완료":
#         return "ok"
#     if state == "에러" or _voice_analysis_is_stale(row):
#         return "review"
#     return "wait"

def check_voice_gate(user_id):
    """'완료' 시점에 음성 자동분석 상태를 본다: ok(완료) / wait(진행 중) / review(에러·멈춤·대조 실패)"""
    row = get_validation_row(user_id, "voice_analysis_state, voice_analysis_synced_at, voice_analysis_result")
    state = row.get('voice_analysis_state')
    if state == "완료":
        result = row.get('voice_analysis_result') or {}
        if isinstance(result, dict) and result.get('match_error'):
            return "review"   # 블랙리스트 대조가 실패했으니 '이상 없음'으로 볼 수 없다 → 운영진 확인
        return "ok"
    if state == "에러" or _voice_analysis_is_stale(row):
        return "review"
    return "wait"
    
# def finalize_after_nickname_check(source_id, user_id, reply_token):
#     """'닉변대기' 상태에서 신입이 '완료'를 입력했을 때의 최종 판정.

#     1) 닉네임(+생년 윗첨자, 여자는 ✿) 변경 확인 — 미변경이면 재안내
#     2) 음성 자동분석 상태 확인 — 진행 중이면 잠시 후 다시 입력하게 안내 / 에러·멈춤이면 운영진 검토
#     3) 이상 없음(깨끗한 신규) → '/ㅇㅈ 퇴장' 멘트 + @전체 완료멘션 (퇴장대기)
#        이상 있음 → 운영진 확인 대기 안내 (관리자검토대기)
#     """
#     row = get_validation_row(user_id, "nickname, birth_year, gender")
#     nickname = row.get('nickname') or ""
#     birth_year = row.get('birth_year') or ""
#     gender = row.get('gender') or ""

#     def _reply(text):
#         with ApiClient(configuration) as api_client:
#             MessagingApi(api_client).reply_message_with_http_info(
#                 ReplyMessageRequest(reply_token=reply_token, messages=[TextMessage(text=text)])
#             )

#     # 1) 닉네임 변경 확인
#     if not is_nickname_changed_correctly(source_id, user_id, birth_year, nickname, gender):
#         expected = build_display_name(nickname, birth_year, gender)
#         _reply(
#             f"닉네임이 아직 '{expected}' 형식과 맞지 않아요.\n"
#             "닉네임을 변경하고 프로필도 설정한 뒤 다시 \"완료\"라고 입력해 주세요.\n"
#             "다른 닉네임으로 하고 싶으시면 운영진에게 말씀해 주세요."
#         )
#         return

#     # 2) 음성 자동분석 상태 확인
#     gate = check_voice_gate(user_id)
#     if gate == "wait":
#         _reply("음성 분석을 마무리하는 중이에요. 잠시 후 다시 \"완료\"라고 입력해 주세요.")
#         return
#     if gate == "ok":
#         try:
#             # 부수효과 목적: 운영진방 참고 알림 + voice_check_report 저장 (반환 문구는 사용 안 함)
#             format_voice_analysis_reply_text(user_id)
#         except Exception as e:
#             print(f"음성분석 결과 알림/저장 중 예외(무시): {e}")

#     # 3) 이상 여부 판정 (분석 에러/멈춤이면 무조건 운영진 검토)
#     _blacklist_issue = has_blacklist_issue(user_id) if gate == "ok" else False
#     _clean_new = is_clean_new_user(user_id)
#     print(f"[퇴장 멘트 판정] user_id={user_id} gate={gate} 음성블랙이슈={_blacklist_issue} 깨끗한신규={_clean_new}")

#     if gate == "review" or _blacklist_issue or not _clean_new:
#         if supabase:
#             supabase_execute(
#                 lambda: supabase.table('user_validations').update({"status": "관리자검토대기"}).eq('user_id', user_id).execute(),
#                 label="관리자검토대기 상태 전환"
#             )
#         sync_status_to_sheet(user_id, "관리자검토대기")
#         _reply("기본 인증절차가 완료되었습니다. 인증자의 추가 확인 후 입장 진행하겠습니다.")
#         return

#     # 4) 깨끗한 신규 → 퇴장 안내 + @전체 완료멘션을 같이 reply
#     #    (memberLeft 웹훅에는 replyToken이 없어, 실제 퇴장 후에는 reply가 불가능하므로 안내와 동시에 발송)
#     if supabase:
#         supabase_execute(
#             lambda: supabase.table('user_validations').update({"status": "퇴장대기"}).eq('user_id', user_id).execute(),
#             label="퇴장대기 상태 전환"
#         )
#     sync_status_to_sheet(user_id, "퇴장대기")

#     exit_text = search_keyword("퇴장") or search_keyword("ㅌㅈ") or "✅ 인증이 모두 완료되었습니다. 안내에 따라 방을 나가주세요."
#     completion_message = build_all_mention_message(f"[{nickname or '신입'}]님 인증이 완료되었습니다.")

#     sent_ok = send_reply_or_push(reply_token, source_id, [TextMessage(text=exit_text), completion_message])
#     if not sent_ok:
#         print(f"⚠️ user_id={user_id} 퇴장 멘트 발송 실패 -> 닉변대기로 되돌림")
#         if supabase:
#             supabase_execute(
#                 lambda: supabase.table('user_validations').update({"status": "닉변대기"}).eq('user_id', user_id).execute(),
#                 label="퇴장 멘트 발송 실패 롤백"
#             )
#         sync_status_to_sheet(user_id, "닉변대기")

def finalize_after_nickname_check(source_id, user_id, reply_token):
    """'닉변대기' 상태에서 신입이 '완료'를 입력했을 때의 최종 처리.

    1) 닉네임(+생년 윗첨자, 여자는 ✿) 변경 확인 — 미변경이면 재안내
    2) 음성 자동분석 상태 확인 — 진행 중이면 잠시 후 다시 입력하게 안내
    3) 이상 유무와 상관없이 항상 '기본 인증 완료, 인증자 확인 후 진행' 안내 (관리자검토대기)
       (퇴장 멘트/@전체 완료멘션은 더 이상 자동으로 보내지 않음)
    """
    row = get_validation_row(user_id, "nickname, birth_year, gender")
    nickname = row.get('nickname') or ""
    birth_year = row.get('birth_year') or ""
    gender = row.get('gender') or ""

    def _reply(text):
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message_with_http_info(
                ReplyMessageRequest(reply_token=reply_token, messages=[TextMessage(text=text)])
            )

    # 1) 닉네임 변경 확인
    if not is_nickname_changed_correctly(source_id, user_id, birth_year, nickname, gender):
        expected = build_display_name(nickname, birth_year, gender)
        _reply(
            f"닉네임이 아직 '{expected}' 형식과 맞지 않아요.\n"
            "닉네임을 변경하고 프로필도 설정한 뒤 다시 \"완료\"라고 입력해 주세요.\n"
            "다른 닉네임으로 하고 싶으시면 운영진에게 말씀해 주세요."
        )
        return

    # 2) 음성 자동분석 상태 확인
    gate = check_voice_gate(user_id)
    if gate == "wait":
        _reply("음성 분석을 마무리하는 중이에요. 잠시 후 다시 \"완료\"라고 입력해 주세요.")
        return
    if gate == "ok":
        try:
            # 부수효과 목적: 운영진방 참고 알림 + voice_check_report 저장 (반환 문구는 사용 안 함)
            format_voice_analysis_reply_text(user_id)
        except Exception as e:
            print(f"음성분석 결과 알림/저장 중 예외(무시): {e}")

    # 3) 문제 유무와 상관없이 항상 운영진 확인 대기로 전환
    print(f"[완료 처리] user_id={user_id} gate={gate} -> 관리자검토대기")
    if supabase:
        supabase_execute(
            lambda: supabase.table('user_validations').update({"status": "관리자검토대기"}).eq('user_id', user_id).execute(),
            label="관리자검토대기 상태 전환"
        )
    sync_status_to_sheet(user_id, "관리자검토대기")
    _reply("기본 인증절차가 완료되었습니다. 인증자의 추가 확인 후 입장 진행하겠습니다.")

@app.route("/api", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature')
    body = request.get_data(as_text=True)

    # ✨ [추가됨] 같은 코드가 올라간 다른 클라우드런 인스턴스가 이 방을 담당하고 있으면
    # 원본 요청을 그대로 그쪽으로 넘기고, 여기서는 처리하지 않는다.
    # (서명은 채널 시크릿+바디로 계산되므로, 바디/시그니처를 그대로 전달하면
    #  받는 쪽에서도 서명 검증이 정상적으로 통과한다)
    try:
        forward_url = get_forward_target_url(body)
        if forward_url:
            requests.post(
                forward_url,
                data=body.encode("utf-8"),
                headers={"Content-Type": "application/json", "X-Line-Signature": signature},
                timeout=10,
            )
            return 'OK'
    except Exception as e:
        print(f"⚠️ 다른 인스턴스로 전달 실패(이 서비스에서 직접 처리 시도): {e}")

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
    # ("." 초기화, 1번 양식 제출, "완료" 답장 처리)를 전혀 실행/기록하지 않는다.
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
    if is_admin_room and user_message == ".":
        return  # 인증자방에서는 '.'에 반응하지 않음

    if not is_admin_room and user_message == ".":
        room_state = get_room_state(source_id)
        tracked_user_id = room_state.get('user_id') if room_state else None
        # 신입(인증 진행 중인 본인)이 입력한 '.'은 무시 — 기존 멤버가 입력했을 때만 초기화
        if tracked_user_id and tracked_user_id == user_id:
            return
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

    # ✨ 기존멤버등록 '동일 기록 확인' 답변은 '/'로 시작하는 메시지만 인식 (/1, /새로, /취소)
    if is_admin_room and user_message.startswith("/"):
        if handle_member_register_confirm(event, user_id, user_message):
            return

    # 슬래시(/) 명령어 로직
    if user_message.startswith("/"):
        command = user_message[1:].strip()
        parts = command.split(maxsplit=1)
        cmd_prefix = parts[0]

        # ✨ 방(그룹) ID 확인: /방아이디, /방id  (어느 방에서든 동작)
        if cmd_prefix.lower() in ("방아이디", "방id"):
            gid = getattr(event.source, 'group_id', None) or getattr(event.source, 'room_id', None)
            id_reply = f"이 방의 ID:\n{gid}" if gid else "그룹방이 아닙니다."
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=id_reply)])
                )
            return

        # ✨ [추가됨] /바뀐닉 변경 : 신입이 다른 닉네임으로 바꾸기로 했을 때, 운영진이 그 방에서
        # 이 명령을 입력하면 DB의 닉네임 값을 직접 수정한다. (예: "/오브할게요 변경")
        # 인증자방에서는 동작하지 않고, 신입 본인이 입력한 건 무시한다.
        if (not is_admin_room) and command.endswith("변경") and cmd_prefix not in ("인증", "ㅇㅈ", "ㅇㅅㅇㅈ"):
            changed_nick = command[:-2].strip()
            if changed_nick:
                nick_messages = apply_nickname_change(source_id, user_id, changed_nick)
                if nick_messages:
                    with ApiClient(configuration) as api_client:
                        MessagingApi(api_client).reply_message_with_http_info(
                            ReplyMessageRequest(reply_token=event.reply_token, messages=nick_messages)
                        )
                return

        # 음성인증 멘트 수동 발송: /인증 음성인증, /ㅇㅈ 음성인증, /ㅇㅅㅇㅈ
        # (자동 흐름이 끊겼거나, 관리자가 직접 재발송해야 할 때 사용)
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

        # 0) /색상, /색상표, /색깔, /색표 : 중복/블랙 필터링 색상 의미표 (관리자방 전용)
        if command_body in ("색상", "색상표", "색깔", "색표"):
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=COLOR_LEGEND_TEXT)])
                )
            return

        # 1) /디비업데이트 명령어 (1000개 이상 데이터 대응 완료)
        if "디비업데이트" in command_body:
            try:
                sync_reports = []

                # A. '멘트' 시트 동기화
                if not sheet:
                    sync_reports.append("• ⚠️ 멘트: 건너뜀 — '멘트' 시트에 연결되지 않았습니다 (서버 시작 로그의 '시트 연결 중 일부 실패' 확인)")
                else:
                    ments_data = sheet.get_all_records()
                    headers = list(ments_data[0].keys()) if ments_data else []
                    # 헤더 공백 제거 후 '인증'/'출력' 열을 찾는다
                    key_col = next((h for h in headers if str(h).replace(" ", "") == "인증"), None)
                    val_col = next((h for h in headers if str(h).replace(" ", "") == "출력"), None)

                    if not ments_data:
                        sync_reports.append("• ⚠️ 멘트: 건너뜀 — 시트에 데이터 행이 없습니다")
                    elif key_col is None or val_col is None:
                        sync_reports.append(f"• ⚠️ 멘트: 건너뜀 — '인증'/'출력' 열을 못 찾았습니다. 현재 헤더: {headers}")
                    else:
                        _dedup = {}
                        for row in ments_data:
                            k_raw = str(row.get(key_col, '')).strip()
                            v_text = str(row.get(val_col, '')).strip()
                            if k_raw and v_text:
                                for k in [x.strip() for x in k_raw.split(',') if x.strip()]:
                                    _dedup[k] = {"keyword": k, "reply_text": v_text}
                        ments_records = list(_dedup.values())

                        if not ments_records:
                            sync_reports.append("• ⚠️ 멘트: 건너뜀 — 인증/출력 값이 모두 빈 칸입니다")
                        elif supabase:
                            new_keywords = {r["keyword"] for r in ments_records}
                            existing_rows = get_all_supabase_data('auth_ments', 'keyword')
                            existing_keywords = {str(r.get('keyword', '')).strip() for r in existing_rows if r.get('keyword')}
                            removed_keywords = list(existing_keywords - new_keywords)

                            if removed_keywords:
                                for i in range(0, len(removed_keywords), 200):
                                    supabase.table('auth_ments').delete().in_('keyword', removed_keywords[i:i + 200]).execute()

                            ok_n = process_in_chunks('auth_ments', ments_records, errors=sync_errors)
                            has_one = '1' in new_keywords
                            sync_reports.append(f"• 멘트: {ok_n}/{len(ments_records)}개 반영 (삭제 {len(removed_keywords)}개, 키워드 '1' {'있음' if has_one else '없음'})")
                            
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
                                record = {"room_name": r_name, "room_id": r_id}
                                # ✨ [추가됨] 3번째 열(담당 클라우드런 URL)이 있으면 같이 저장.
                                # 없는 방(빈 칸)은 담당 지정 안 함 = 받은 서비스가 그대로 처리.
                                if len(keys) >= 3:
                                    r_service_url = str(row[keys[2]]).strip().rstrip("/")
                                    if r_service_url:
                                        record["service_url"] = r_service_url
                                room_records.append(record)
                    if room_records and supabase:
                        supabase.table('room_management').delete().neq('room_name', '_DELETE_ALL_KEY_').execute()
                        process_in_chunks('room_management', room_records, is_insert=True)
                        sync_reports.append(f"• 방관리: {len(room_records)}개 방 목록")

                # C. '녹음' 시트 동기화
                try:
                    if client:
                        rec_sheet = client.open("인증봇").worksheet("녹음")
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

                                # ✨ [추가됨] 쓰던닉(T열) / 야방 방이름(U열): 헤더를 못 찾으면 필드를 아예 빼서 DB 값이 덮어써지지 않게 한다.
                                if col_map.get("prev_nick") is not None:
                                    record["prev_nick"] = get_cell(row, col_map["prev_nick"])
                                if col_map.get("yadan_room") is not None:
                                    record["yadan_room"] = get_cell(row, col_map["yadan_room"])

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

                # E. '룸스테이트' 시트 동기화 (시트 기준으로 DB room_states를 맞춤: 시트에 있는 방은 upsert, 시트에 없는 방은 DB에서 삭제)
                if room_state_sheet and supabase:
                    try:
                        rs_rows = room_state_sheet.get_all_values()[1:]
                        rs_records = {}
                        for row in rs_rows:
                            rid = str(row[0]).strip() if row else ""
                            raw = row[1] if len(row) > 1 else ""
                            if not rid or not raw:
                                continue
                            try:
                                rs_records[rid] = {"room_id": rid, "data": json.loads(raw)}
                            except Exception:
                                print(f"룸스테이트 시트 JSON 파싱 실패(건너뜀): {rid}")

                        existing_rs = get_all_supabase_data('room_states', 'room_id')
                        existing_rids = {str(r.get('room_id', '')).strip() for r in existing_rs if r.get('room_id')}
                        removed_rids = list(existing_rids - set(rs_records.keys()))
                        for i in range(0, len(removed_rids), 200):
                            supabase.table('room_states').delete().in_('room_id', removed_rids[i:i + 200]).execute()
                        process_in_chunks('room_states', list(rs_records.values()), is_insert=False)
                        _room_state_sheet_cache.clear()
                        sync_reports.append(f"• 룸스테이트: {len(rs_records)}개 방 (DB에서 삭제 {len(removed_rids)}개)")
                    except Exception as e:
                        sync_reports.append(f"• ⚠️ 룸스테이트: 동기화 실패 — {e}")

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

        # ✨ [추가됨] /음성업로드 — 운영진이 임의로 음성을 제출해 블랙리스트에 수동 등록
        elif command_body.startswith("음성업로드"):
            rest = command_body[len("음성업로드"):].strip()

            if rest in ("취소", "취소하기", "cancel"):
                del_user_session(event.source.user_id)
                reply_text = "🗑️ 음성 업로드 요청이 취소되었습니다."
            elif not rest:
                reply_text = (
                    "사용법: /음성업로드 닉네임 성별 [사유]\n"
                    "예) /음성업로드 홍길동 남 도촬유포\n\n"
                    "입력 후 이어서 음성 파일을 보내주시면 자동 분석 후 블랙리스트에 등록됩니다.\n"
                    "취소하려면 /음성업로드 취소"
                )
            else:
                upload_args = rest.split(maxsplit=2)
                if len(upload_args) < 2:
                    reply_text = "닉네임과 성별은 필수입니다.\n사용법: /음성업로드 닉네임 성별 [사유]"
                else:
                    up_nickname, up_gender = upload_args[0], upload_args[1]
                    up_reason = upload_args[2] if len(upload_args) >= 3 else ""

                    set_user_session(event.source.user_id, {
                        "pending_blacklist_voice_upload": {
                            "nickname": up_nickname, "gender": up_gender, "reason": up_reason,
                        }
                    }, ttl=600)

                    reply_text = (
                        f"📤 블랙리스트 음성 업로드 준비 완료\n\n"
                        f"- 닉네임: {up_nickname}\n- 성별: {up_gender}\n- 사유: {up_reason or '(미입력)'}\n\n"
                        f"🎤 이제 음성 파일을 보내주세요."
                    )

            if reply_text:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
                return

        # ✨ [추가됨] /기존멤버등록 — 기존 멤버를 양식 + 음성으로 DB에 등록
        elif command_body.startswith("기존멤버등록"):
            member_msgs = start_member_register(user_id, command_body[len("기존멤버등록"):])
            with ApiClient(configuration) as api_client:
                MessagingApi(api_client).reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=member_msgs)
                )
            return

        # 2) /O번방 확인 명령어
        elif "확인" in command_body:
            room_name_input = command_body.replace("확인", "").strip()
            target_room_id = get_room_id_by_name(room_name_input)
            if target_room_id:
                try:
                    room_state = get_room_state(target_room_id, strict=True)
                except Exception as rs_err:
                    print(f"N번방 확인 - 룸스테이트 시트 조회 실패: {rs_err}")
                    room_state = None
                    reply_text = f"⚠️ [{room_name_input}] 룸스테이트 시트 조회에 실패했습니다. 잠시 후 다시 시도해 주세요."
                if reply_text:
                    pass
                elif not room_state:
                    reply_text = f"📭 [{room_name_input}] 현재 대기 중인 신규 인증 멤버가 없습니다."
                else:
                    status = room_state.get('status')
                    is_known = room_state.get('is_known', False)
                    tracked_user_id = room_state.get('user_id')
                    if status == 'joined':
                        if is_known:
                            reply_text = f"⚠️ [{room_name_input}]\n기존 방문/블랙리스트 이력이 있는 유저가 방금 입장했습니다!\n(현재 상대방이 양식을 입력 중입니다)"
                        else:
                            reply_text = f"⏳ [{room_name_input}]\n완전한 신규 유저가 현재 양식을 입력 중입니다.\n✅ 현재까지 특이사항(기존 방문/블랙 이력) 없음"
                    elif status == 'form_submitted':
                        alert_report = room_state.get('report', f"[{room_name_input}] 양식이 접수되었습니다.")
                        reply_text = alert_report
                    elif status == 'pending_friend':
                        reply_text = f"⏳ [{room_name_input}]\n신입이 친구추가 전 단계입니다. (친구추가 후 '시작' 입력 대기 중)"
                    else:
                        reply_text = f"ℹ️ [{room_name_input}] 현재 진행 상태: {status}"

                    # ✨ [추가됨] 음성검증까지 확인되었는지 여부 + 확인된 시점이라면 그 확인 내용을 함께 출력
                    if reply_text and tracked_user_id:
                        reply_text = f"{reply_text}\n\n{format_voice_check_status(tracked_user_id)}"
            else:
                # ✨ [추가됨] 방 이름이 아니면 '닉네임'으로 보고 과거 검증 기록 + 음성DB 기록을 조회 (/닉네임 확인)
                nick_text = format_nickname_history(room_name_input) if room_name_input else None
                if nick_text:
                    reply_text = nick_text
                else:
                    reply_text = (
                        f"❌ '{room_name_input}' 정보를 방관리(DB)에서도, 닉네임 기록(검증/음성DB)에서도 찾을 수 없습니다. "
                        "방관리 시트에 방금 추가했다면 /디비업데이트 후 다시 시도해 주세요."
                    )

            if reply_text:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[TextMessage(text=t) for t in split_for_line(reply_text)]
                    ))
            return

    # ✨ [추가됨] 인증자방: '/기존멤버등록' 이후 제출된 양식 처리 (그 외 일반 대화는 건드리지 않음)
    if is_admin_room and not user_message.startswith("/"):
        if handle_member_register_form(event, user_id, user_message):
            return

    # 📌 [핵심 검증 1] 1번 양식(FLIRTY) 제출 처리 (마지막 입장 유저만 작동)
    if not is_admin_room and all(k in user_message for k in ["닉네임", "년생", "성별", "지역"]):
        if not is_last_joined_user(source_id, user_id):
            return 

        extracted_data = parse_signup_form(user_message)

        nickname = extracted_data.get("닉네임", "").strip()
        birth_year = _norm_year(extracted_data.get("년생", ""))     # `1986 / 86 / 86년생 → 86
        gender = normalize_gender(extracted_data.get("성별", ""))
        region = extracted_data.get("지역", "").strip()
        marriage = _pick_field(extracted_data, contains=("기혼", "결혼"))
        yadan = _clean_yes_no(extracted_data.get("야방경험", ""))
        yadan_room = _pick_field(extracted_data, contains=("방이름",))
        prev_nick = _pick_field(extracted_data, contains=("쓰던닉",))

        # 새 양식에 없는 기존 항목은 공란으로 저장
        age = ""
        military = ""
        inviter = ""
        leave_reason = ""
        kick_reason = ""

        missing_fields = []
        if not nickname:
            missing_fields.append("닉네임")
        elif not (2 <= len(nickname) <= 3):
            missing_fields.append("닉네임(2~3글자로 작성)")
        if not birth_year:
            missing_fields.append("년생(숫자로 작성, 예: 1986)")
        if gender not in ("남", "여"):
            missing_fields.append("성별(남/여)")
        if not region:
            missing_fields.append("지역")
        if not marriage:
            missing_fields.append("기혼,미혼,돌싱")
        if not yadan:
            missing_fields.append("야방경험(유/무)")
        elif yadan == "유" and not yadan_room:
            missing_fields.append("야방경험 있을시 방이름")
        if not prev_nick:
            missing_fields.append("FLIRTY방에 온경험 있음 쓰던닉 (없으면 '없음')")

        if missing_fields:
            reply_text = f"⚠️ 양식 작성 내용 중 다음 항목이 누락되었거나 수정이 필요합니다:\n- {', '.join(missing_fields)}\n\n해당 항목을 빠짐없이 작성 후 다시 제출해 주세요!"
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
            return

        # 한국 시간(KST) 기준 어제 날짜 계산
        kst = datetime.timezone(datetime.timedelta(hours=9))
        current_date = (datetime.datetime.now(kst) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        save_success = False
        alert_text = None
        dup_check_ok = False  # ✨ 중복/블랙 조회가 실제로 끝까지 성공했는지 (실패 시 '깨끗함'으로 간주하지 않음)

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

        # ✨ [추가됨] 동일 유저ID 이력은 수정 재제출을 해도 사라지지 않게 room_state로 이어받는다.
        # (재제출 때는 '방금 전 내가 저장한 기록'을 검증에서 제외하는데, user_id가 PK라 과거 기록이
        #  이미 그 행으로 덮어써져 있어서, 그대로 두면 재입장/중복 이력이 통째로 사라져 '신규'로 판정됨)
        id_match_lines = []   # 이번 검사에서 '유저ID 일치'로 잡힌 기록 상세
        id_match_level = 0    # 그중 가장 높은 경고 수준
        prior_id_lines = list(current_room_state.get('prior_id_lines') or []) if is_edit_resubmission else []
        # ✨ [변경됨] 3.5(🟠) 같은 소수 단계를 쓰므로 int → float
        prior_id_level = float(current_room_state.get('prior_id_level') or 0) if is_edit_resubmission else 0

        if supabase:
            try:
                # 1000개 이상 제약 해결: 페이지네이션 기반 전체 데이터 호출
                all_val_data = get_all_supabase_data('user_validations')

                # 음성DB(voice_profiles)도 함께 대조: 유저ID 일치=동일인, 닉네임 일치=동일인 의심
                # (임베딩 등 무거운 컬럼은 가져오지 않는다)
                voice_profile_rows = get_all_supabase_data('voice_profiles', 'user_id, nickname, source')
                vp_by_uid = {}
                for _vp in voice_profile_rows:
                    _vp_uid = str(_vp.get('user_id') or '').strip()
                    if _vp_uid and _vp_uid not in vp_by_uid:
                        vp_by_uid[_vp_uid] = _vp
                seen_validation_uids = set()

                for row in all_val_data:
                    rec_id = str(row.get('user_id', '')).strip()

                    # 본인이 이번 세션에서 양식을 수정 재제출하는 경우, 방금 전 자신이 남긴
                    # 기록은 '중복 유저'가 아니므로 검증 대상에서 제외한다.
                    if is_edit_resubmission and rec_id == str(user_id).strip() and rec_id != "":
                        continue

                    rec_name = str(row.get('nickname', '')).strip()
                    rec_year = _year_label(row.get('birth_year', ''))
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

                    # ✨ [변경됨] 운영진이 닉네임을 수동 변경('/새닉 변경')한 유저도,
                    # 처음 양식에 적었던 닉네임(form_nickname)으로 닉네임 일치를 잡을 수 있게 함께 비교한다.
                    rec_form_nick = str(rec_details.get('form_nickname', '')).strip() if isinstance(rec_details, dict) else ""
                    is_id_matched = (rec_id == str(user_id).strip() and rec_id != "")
                    is_name_matched = (
                        (rec_name == nickname and rec_name != "")
                        or (rec_form_nick == nickname and rec_form_nick != "")
                    )

                    if is_id_matched or is_name_matched:
                        match_reasons = []
                        if is_id_matched: match_reasons.append("고유ID 일치 → 동일인")
                        if is_name_matched: match_reasons.append("닉네임 일치 → 동일인 의심")

                        match_score = 0
                        if rec_year == birth_year: match_score += 1
                        if rec_gender == gender: match_score += 1
                        if rec_region == region: match_score += 1

                        row_info = f"📍 [기존 DB 기록] ({', '.join(match_reasons)})\n - 기존정보: {rec_name} / {rec_year}년생 / {rec_gender} / {rec_region}"
                        if rec_black:
                            row_info += f"\n - 💀 블랙사유: {rec_black}"

                        # 블랙사유 유무와 상관없이, 기존에 입력되어 있던 내용을 개수 제한 없이 함께 보여준다.
                        # (값이 있는 항목만 표시됨. 옛 기록의 나온이유/킥이력/초대자와 새 기록의 야방 방이름/쓰던닉 모두 포함)
                        detail_priority = DETAIL_LABELS
                        detail_lines = []
                        for key, label in detail_priority:
                            val = str(rec_details.get(key, "")).strip()
                            if val:
                                detail_lines.append(f"{label}: {val}")
                        if detail_lines:
                            row_info += f"\n - 📝 기존 입력 내용: {' / '.join(detail_lines)}"

                        # ✨ [추가됨] 동일 유저ID라면, 이전에 제출한 양식과 이번 양식에서 달라진 항목을 비교해서 보여준다.
                        # 성별/년생이 이전과 다르면 diff_critical=True → 아래에서 🟠 경고 단계로 올린다.
                        diff_critical = False
                        if is_id_matched:
                            diff_lines, diff_critical = diff_with_previous(row, {
                                "nickname": nickname, "birth_year": birth_year, "gender": gender, "region": region,
                                "marriage": marriage, "yadan": yadan, "yadan_room": yadan_room, "prev_nick": prev_nick,
                            })
                            if diff_lines:
                                row_info += "\n - 🔀 이전 제출과 달라진 내용:\n   " + "\n   ".join(diff_lines)

                        # 같은 유저ID의 음성DB 프로필이 있으면 함께 표시
                        linked_vp = vp_by_uid.get(rec_id) if rec_id else None
                        if linked_vp:
                            vp_kind = "신입 음성인증" if linked_vp.get('source') == 'new_member' else "블랙리스트 음성 등록"
                            row_info += f"\n - 🎙️ 음성DB: 같은 유저ID의 음성 프로필 있음 ({vp_kind})"
                        if rec_id:
                            seen_validation_uids.add(rec_id)
                        found_duplicates.append(row_info)

                        current_level = 0
                        if is_name_matched:
                            if match_score == 3: current_level = 4
                            elif match_score > 0: current_level = 2
                            else: current_level = 1
                        if is_id_matched:
                            if current_level < 3: current_level = 3
                        # ✨ [추가됨] 동일 ID인데 성별/년생이 이전 제출과 다르면 🟠 경고(3.5). 블랙(5)은 아래에서 그대로 우선한다.
                        if is_id_matched and diff_critical and current_level < 3.5:
                            current_level = 3.5
                        if rec_black:
                            current_level = 5

                        if is_id_matched:
                            id_match_lines.append(row_info)
                            id_match_level = max(id_match_level, current_level)

                        if current_level > highest_alert_level:
                            highest_alert_level = current_level
                            if current_level == 5: alert_status_text, color_emoji = "💀 [위험] 블랙리스트 유저 감지", "⚫"
                            elif current_level == 4: alert_status_text, color_emoji = "🚨 [적색 경고] 닉네임 및 모든 정보 일치", "🔴"
                            elif current_level == 3.5: alert_status_text, color_emoji = "🟠 [경고] 동일 ID인데 성별/년생이 이전과 다름", "🟠"
                            elif current_level == 3: alert_status_text, color_emoji = "🔄 [주의] 재입장 유저 (동일 ID 확인)", "🟪"
                            elif current_level == 2: alert_status_text, color_emoji = "⚠️ [황색 경고] 닉네임 및 정보 일부 일치", "🟡"
                            elif current_level == 1: alert_status_text, color_emoji = "🔵 [주의] 닉네임 일치 유저", "🟦"

                # 위에서 이미 user_validations 기록으로 표시된 유저ID를 제외하고, 음성DB에만 있는 기록을 추가 대조
                for vp in voice_profile_rows:
                    vp_uid = str(vp.get('user_id') or '').strip()
                    vp_name = str(vp.get('nickname') or '').strip()
                    if vp_uid and vp_uid in seen_validation_uids:
                        continue
                    if is_edit_resubmission and vp_uid and vp_uid == str(user_id).strip():
                        continue

                    vp_id_matched = (vp_uid != "" and vp_uid == str(user_id).strip())
                    vp_name_matched = (vp_name != "" and vp_name == nickname)
                    if not (vp_id_matched or vp_name_matched):
                        continue

                    # source가 new_member(신입 음성인증)가 아니면 블랙 음성 (admin_blacklist_upload, blacklist_backfill 등)
                    vp_is_black = (vp.get('source') != 'new_member')
                    vp_reasons = []
                    if vp_id_matched: vp_reasons.append("고유ID 일치 → 동일인")
                    if vp_name_matched: vp_reasons.append("닉네임 일치 → 동일인 의심")
                    vp_kind = "블랙리스트 음성 등록" if vp_is_black else "신입 음성인증"
                    vp_line = f"🎙️ [음성DB 기록] ({', '.join(vp_reasons)})\n - 음성DB정보: {vp_name or '(닉네임 없음)'} / {vp_kind}"
                    found_duplicates.append(vp_line)

                    if vp_is_black: vp_level = 5
                    elif vp_id_matched: vp_level = 3
                    else: vp_level = 1
                    if vp_id_matched:
                        id_match_lines.append(vp_line)
                        id_match_level = max(id_match_level, vp_level)
                    if vp_level > highest_alert_level:
                        highest_alert_level = vp_level
                        alert_status_text, color_emoji = {
                            5: ("💀 [위험] 블랙리스트 유저 감지", "⚫"),
                            3: ("🔄 [주의] 재입장 유저 (동일 ID 확인)", "🟪"),
                            1: ("🔵 [주의] 닉네임 일치 유저", "🟦"),
                        }[vp_level]

                # ✨ [추가됨] 쓰던닉(FLIRTY방에 온 경험 있음)이 적혀 있으면 그 닉네임으로도 기존 기록을 조회
                if prev_nick and not _is_none_word(prev_nick):
                    for _line in find_nickname_conflicts(prev_nick, user_id):
                        found_duplicates.append(f"📍 [쓰던닉 '{prev_nick}' 조회]\n{_line}")
                        if highest_alert_level < 2:
                            highest_alert_level = 2
                            alert_status_text, color_emoji = "⚠️ [황색 경고] 쓰던닉과 일치하는 기존 기록", "🟡"

                if is_edit_resubmission and (prior_id_lines or prior_id_level > 0):
                    carried_level = max(prior_id_level, 3)
                    found_duplicates = prior_id_lines + found_duplicates
                    if carried_level > highest_alert_level:
                        highest_alert_level = carried_level
                        # ✨ [변경됨] 🟠(3.5) 단계도 수정 재제출 때 사라지지 않도록 이어받는다.
                        carried_map = {
                            5: ("💀 [위험] 블랙리스트 유저 감지", "⚫"),
                            4: ("🚨 [적색 경고] 닉네임 및 모든 정보 일치", "🔴"),
                            3.5: ("🟠 [경고] 동일 ID인데 성별/년생이 이전과 다름", "🟠"),
                            3: ("🔄 [주의] 재입장 유저 (동일 ID 확인)", "🟪"),
                        }
                        alert_status_text, color_emoji = carried_map.get(carried_level, carried_map[3])

                if highest_alert_level > 0:
                    dup_details_str = "\n\n".join(found_duplicates)
                    _prev_line = f"🕘 쓰던닉: {prev_nick}\n" if prev_nick and not _is_none_word(prev_nick) else ""
                    alert_text = (
                        f"{color_emoji} 신입 양식 작성 중복/블랙 필터링 결과\n\n"
                        f"📌 상태: {alert_status_text}\n"
                        f"👤 신규입력: {nickname} ({birth_year}년생) / {region} / {gender}\n"
                        f"{_prev_line}\n"
                        f"📑 [중복/기존 내역 상세]\n{dup_details_str}\n\n"
                        f"💡 관리자분들께서는 위 상세 내역을 기반으로 승인 여부를 검토하시기 바랍니다."
                    )
                dup_check_ok = True
            except Exception as db_err:
                print(f"DB 검증 에러: {db_err}")

        # B. DB 업서트(Upsert) 저장
        update_data_details = {
            "marriage": marriage, "yadan": yadan, "yadan_room": yadan_room, "prev_nick": prev_nick,
            # ✨ 마지막 '퇴장' 멘트 직전에 확인하는 값: 중복/블랙 이력이 없는 깨끗한 신규 유저인지
            "dup_clean": bool(dup_check_ok and not alert_text and not is_yadan_none(yadan)),
            # ✨ 양식에 적은 닉네임 (운영진이 '/새닉 변경'으로 DB 닉네임을 바꿔도 이 값은 보존됨)
            "form_nickname": nickname,
        }
        
        target_status = "음성대기"     # ✨ [변경됨] 양식 제출 직후 바로 음성 안내 단계로 (기존: "입장대기" → "확인" 답장)
        nickname_to_save = nickname   # ✨ 수정 재제출 시 운영진이 바꿔둔 닉네임을 덮어쓰지 않기 위한 저장용 값
        if supabase:
            try:
                # ✨ 이력 비교/보존을 위해 기존 닉네임/성별/지역/년생/details도 함께 조회
                user_res = supabase.table('user_validations').select(
                    'retry_count, status, entry_date, nickname, gender, region, birth_year, details'
                ).eq('user_id', user_id).execute()
                retry_cnt = 1
                existing_entry_date = None
                if user_res.data:
                    retry_cnt = user_res.data[0].get('retry_count', 1) + 1
                    existing_entry_date = user_res.data[0].get('entry_date')
                    # ✨ 인증 진행 중 1번 양식을 '수정 재제출'한 경우, 이미 진행된 단계(음성대기/닉변대기 등)를
                    # '음성대기'로 되돌리지 않고 유지한다.
                    _existing_status = user_res.data[0].get('status')
                    if is_edit_resubmission and _existing_status and _existing_status not in ("입장대기", "완료"):
                        target_status = _existing_status
                        retry_cnt = user_res.data[0].get('retry_count', 1)  # 수정 재제출은 재시도 횟수에 포함하지 않음

                    # ✨ 이력 누적: details를 통째로 덮어쓰는 구조라, 이전 history/nick_changes를 반드시 이어받는다.
                    prev = user_res.data[0]
                    prev_d = _load_details(prev.get('details'))
                    history = list(prev_d.get('history') or [])
                    if not is_edit_resubmission:   # 같은 세션의 '수정 재제출'은 이력에 넣지 않음
                        history.append({
                            "at": current_date,
                            "nickname": prev.get('nickname'),
                            "form_nickname": prev_d.get('form_nickname') or prev.get('nickname'),
                            "birth_year": prev.get('birth_year'), "gender": prev.get('gender'), "region": prev.get('region'),
                            "age": prev_d.get('age'),
                            **{k: prev_d.get(k) for k, _ in DIFF_DETAIL_FIELDS},
                        })
                    update_data_details["history"] = history[-5:]
                    if prev_d.get('nick_changes'):
                        update_data_details["nick_changes"] = prev_d['nick_changes']
                        # 같은 세션에서 양식 내용만 고쳐 재제출했고 닉네임 양식 값이 그대로라면,
                        # 운영진이 바꿔둔 닉네임/중복 플래그를 그대로 유지한다.
                        if is_edit_resubmission and str(prev_d.get('form_nickname') or '').strip() == nickname:
                            nickname_to_save = prev.get('nickname') or nickname
                            if prev_d.get('dup_clean') is False:
                                update_data_details["dup_clean"] = False

                # 기존 입장일 기록에 이번 날짜를 콤마로 이어붙임 (덮어쓰지 않고 누적)
                entry_date_to_save = f"{existing_entry_date},{current_date}" if existing_entry_date else current_date

                supabase.table('user_validations').upsert({
                    "user_id": user_id,
                    "nickname": nickname_to_save,
                    "gender": gender,
                    "region": region,
                    "birth_year": birth_year,
                    "entry_date": entry_date_to_save,
                    "retry_count": retry_cnt,
                    "status": target_status,
                    "details": update_data_details,
                    "prev_nick": prev_nick,        # ✨ 쓰던닉 (DB에 prev_nick 컬럼 필요)
                    "yadan_room": yadan_room,      # ✨ 야방 방이름 (DB에 yadan_room 컬럼 필요)
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
                    # M:S = 나이, 결혼, 군필, 초대자, 야방경험, 나온이유, 킥이력 (새 양식에 없는 항목은 공란)
                    details_list = [age, marriage, military, inviter, yadan, leave_reason, kick_reason]

                    if user_id in clean_user_ids:
                        found_row_index = clean_user_ids.index(user_id) + 1
                        row_data = all_data[found_row_index - 1]
                        count_val = row_data[7] if len(row_data) >= 8 else "0"
                        current_retry_count = int(count_val) if count_val.isdigit() else 0

                        # F열(입장일)도 덮어쓰지 않고 콤마로 이어붙여 누적
                        existing_entry_date_sheet = row_data[5].strip() if len(row_data) > 5 and row_data[5].strip() else ""
                        entry_date_to_save_sheet = f"{existing_entry_date_sheet},{current_date}" if existing_entry_date_sheet else current_date

                        update_data_basic = [nickname_to_save, gender, region, birth_year, user_id, entry_date_to_save_sheet, "", current_retry_count + 1]
                        validation_sheet.update(range_name=f'A{found_row_index}:H{found_row_index}', values=[update_data_basic])
                        validation_sheet.update(range_name=f'K{found_row_index}:L{found_row_index}', values=[[user_id, target_status]])
                        validation_sheet.update(range_name=f'M{found_row_index}:S{found_row_index}', values=[details_list])
                        # ✨ T열 = 쓰던닉, U열 = 야방 방이름
                        validation_sheet.update(range_name=f'T{found_row_index}:U{found_row_index}', values=[[prev_nick, yadan_room]])
                    else:
                        found_row_index = len(all_data) + 1
                        row_to_insert_basic = [nickname_to_save, gender, region, birth_year, user_id, current_date, "", 1]
                        validation_sheet.update(range_name=f'A{found_row_index}:H{found_row_index}', values=[row_to_insert_basic])
                        validation_sheet.update(range_name=f'K{found_row_index}:L{found_row_index}', values=[[user_id, target_status]])
                        validation_sheet.update(range_name=f'M{found_row_index}:S{found_row_index}', values=[details_list])
                        validation_sheet.update(range_name=f'T{found_row_index}:U{found_row_index}', values=[[prev_nick, yadan_room]])
                    save_success = True
            except Exception as sheet_err:
                print(f"구글 시트 백업 에러: {sheet_err}")

        if save_success:
            if alert_text:
                state_data = {"user_id": user_id, "status": "form_submitted", "report": alert_text}
            else:
                color_emoji = "🟢"
                clean_report = (
                    f"{color_emoji} 신입 양식 작성 중복/블랙 필터링 결과\n\n"
                    "📌 상태: 이상 없음 (중복/블랙 이력이 없는 깨끗한 신규 회원)\n"
                    "양식이 정상 접수되었습니다.\n\n"
                    + format_form_basic_info(
                        nickname=nickname, birth_year=birth_year, gender=gender, region=region,
                        marriage=marriage, yadan=yadan, yadan_room=yadan_room, prev_nick=prev_nick,
                    )
                )
                state_data = {"user_id": user_id, "status": "form_submitted", "report": clean_report}

            state_data["prior_id_lines"] = (prior_id_lines + id_match_lines)[:5]
            state_data["prior_id_level"] = max(prior_id_level, id_match_level)

            set_room_state(source_id, state_data, ttl=7200)
            set_user_session(user_id, {"nickname": nickname_to_save, "gender": gender})

            if is_edit_resubmission:
                # 이미 1번 양식을 제출한 상태에서 내용만 고쳐서 재제출한 경우: DB만 갱신하고 음성 안내는 다시 보내지 않음
                reply_text = f"✏️ [{nickname}]님, 수정하신 내용이 반영되었습니다."
            else:
                # ✨ [변경됨] 양식 접수 직후 바로 "오늘 날짜 + 닉네임" 음성 녹음 안내
                reply_text = build_voice_instruction(nickname)
            # 관리자방 리포트와 같은 기준(블랙수준별 색)의 동그라미만 신입방 응답 맨 앞에 붙인다.
            # 일치 내역/블랙사유 같은 상세 내용은 신입에게 노출하지 않는다.
            if color_emoji:
                reply_text = f"{color_emoji} {reply_text}"
        else:
            reply_text = "⚠️ 서버 통신 문제로 저장에 실패했습니다. 점(.)을 입력하여 처음부터 다시 시도해 주세요!"

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
        return

    # 📌 [핵심 검증 2] 신입이 "완료" 입력 시 ('닉변대기' 상태, 마지막 입장 유저만 작동)
    # (기존의 "확인" / "문제없음" / "변경완료" / 헤르페스 "없다" 단계는 새 흐름에서 삭제됨)
    if (not is_admin_room and not user_message.startswith("/")
            and re.sub(r"[\s\.\!~]", "", user_message) in ("완료", "완료했습니다", "완료했어요", "완료요")):
        if not is_last_joined_user(source_id, user_id):
            return
        if get_validation_row(user_id, "status").get('status') == "닉변대기":
            finalize_after_nickname_check(source_id, user_id, event.reply_token)
        return

    # 3. 일반 DB 키워드 검색 — 봇 명령어는 반드시 '/'로 시작해야 하므로, 슬래시 없는 채팅에는 반응하지 않음
    if user_message.startswith("/"):
        matched_reply = search_keyword(user_message)
        if matched_reply:
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=matched_reply)]))
            return
        # ✨ 기존멤버등록 확인 대기 중인데 답변/명령어 어디에도 해당하지 않는 '/' 메시지 → 가능한 답변 안내
        if is_admin_room and send_member_confirm_guide(event, user_id):
            return


# ==========================================
# [핸들러 2] 방 입장 이벤트 처리 핸들러
# ==========================================
def send_friend_add_guide(source_id, user_id, reply_token=None):
    """친구추가 안내(/ㅇㅈ 1ㅂㅊㄱ, /ㅇㅈ 2ㅂㅊㄱ 멘트)를 보내고, 신입이 '시작'을 입력하면 처음부터 다시
    진행되도록 room_state를 pending_friend로 세팅합니다.
    - 먼저 reply로 시도하고, 실패하면 그룹으로 push(마지막 수단)로 보냅니다.
    반환값: 안내 발송 성공 여부"""
    set_room_state(source_id, {
        "user_id": user_id,
        "status": "pending_friend"
    }, ttl=3600)

    ment1 = search_keyword("1ㅂㅊㄱ")
    ment2 = search_keyword("2ㅂㅊㄱ")
    guide_messages = [TextMessage(text=t) for t in (ment1, ment2) if t]
    if not guide_messages:
        guide_messages = [TextMessage(text=(
            "저를 추가해주셔야 인증진행이 가능해요.\n"
            "친구추가 해주시고 채팅창에 '시작'이라고 입력해 주세요."
        ))]

    if reply_token:
        try:
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=reply_token, messages=guide_messages)
                )
            return True
        except Exception as e:
            print(f"친구추가 안내 reply 실패(push로 재시도): {e}")

    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.push_message_with_http_info(
                PushMessageRequest(to=source_id, messages=guide_messages)
            )
        return True
    except Exception as e:
        print(f"친구추가 안내 push도 실패: {e}")
        return False


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
            welcome_message = f"🔄 {name_prefix}이전 대화가 [음성 녹음] 단계에서 끊겼어요. 이어서 진행할게요.\n\n{v_reply_text}"
    elif last_status in ("닉변대기", "음성확인중", "분석중", "승인대기"):
        # 음성 접수 이후 [닉네임 변경] 단계에서 끊긴 경우 (예전 버전 상태값도 같이 처리)
        _nick_only, _guide = build_nickchange_parts(user_id)
        welcome_message = (
            f"🔄 {name_prefix}이전 대화가 [닉네임 변경] 단계에서 끊겼어요.\n\n"
            f"변경할 닉네임:\n{_nick_only}\n\n{_guide}"
        )
        # 상태가 닉변대기가 아니면 맞춰준다
        if last_status != "닉변대기":
            cas_update_status(user_id, last_status, "닉변대기", label="재입장 닉변대기 전환")
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
        print(f"신입 안내 메시지 전송 실패 -> 친구추가 안내로 전환: {e}")
        # ✨ [추가됨] 입장 이벤트는 발생했는데 1번 멘트가 나가지 못한 경우:
        # 친구추가 안내(/ㅇㅈ 1ㅂㅊㄱ, 2ㅂㅊㄱ)를 보내고, '시작' 입력 시 처음부터 다시 진행되게 한다.
        send_friend_add_guide(source_id, user_id, reply_token)


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

            send_friend_add_guide(source_id, user_id, event.reply_token)
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
        # ✨ '완료' 입력 → 최종 판정까지 통과해서 자동으로 '/ㅇㅈ 퇴장' 멘트가 나간 뒤
        # 신입이 실제로 나간 경우인지 확인
        leave_row = get_validation_row(tracked_user_id, "status")
        was_auto_completed = leave_row.get('status') == "퇴장대기"

        # 인증 진행 중이던 신입이 실제로 나간 경우 -> room_state/세션 초기화
        reset_verification_state(source_id, tracked_user_id)

        if was_auto_completed:
            # ✨ [변경됨] 완료멘션(@전체)은 이제 push가 아니라, 퇴장 안내를 보내는 시점
            # (finalize_after_nickname_check)에 reply로 이미 함께 발송되었으므로 여기서는
            # DB 상태만 '완료'로 반영한다. (memberLeft 이벤트에는 replyToken이 없어
            # 이 시점에는 애초에 reply를 보낼 수 없음)
            if supabase:
                supabase_execute(
                    lambda: supabase.table('user_validations').update({"status": "완료"}).eq('user_id', tracked_user_id).execute(),
                    label="자동 인증완료 상태 반영"
                )
            sync_status_to_sheet(tracked_user_id, "완료")
    elif not left_user_ids or tracked_user_id is None:
        # 누가 나갔는지 알 수 없거나, 추적 중인 인증 대상이 없는 경우에는 기존처럼 room_state만 정리
        del_room_state(source_id)
    # else: 인증과 무관한 다른 멤버가 나간 경우 -> 진행 중인 신입의 room_state를 건드리지 않음


# ==========================================
# ✨ [추가됨] 음성 자동분석 결과를 운영진방에 참고용으로 알리는 헬퍼
# ==========================================
# 블랙리스트 유사도가 이 값 이상이면 "동일인 의심"으로 강조 표시한다.
VOICE_MATCH_ALERT_THRESHOLD = 0.90


def _match_is_member(m):
    """대조 결과 한 건이 '기존 신입 음성'인지 (source가 new_member). source가 없는 예전 결과는 블랙으로 간주."""
    return (m or {}).get("source") == "new_member"


def drop_self_matches(voice_result, user_id):
    """대조 결과에서 본인(같은 user_id)의 이전 제출 음성을 뺀 사본을 돌려준다."""
    if not voice_result or not user_id:
        return voice_result
    matches = voice_result.get("matches") or []
    filtered = [m for m in matches if str(m.get("user_id") or "") != str(user_id)]
    if len(filtered) == len(matches):
        return voice_result
    new_result = dict(voice_result)
    new_result["matches"] = filtered
    return new_result

# ✨ [추가됨] 오토튠/피치보정 의심도(%)가 이 값 이상이면 운영진방 알림 조건에 포함시킨다.
AUTOTUNE_ALERT_THRESHOLD = 50.0

# 신청서 성별 표기("남"/"남자"/"여"/"여자")를 "남"/"여"로 정규화하기 위한 매핑
_GENDER_NORM_MAP = {
    "남": "남", "남자": "남", "남성": "남", "male": "남", "m": "남",
    "여": "여", "여자": "여", "여성": "여", "female": "여", "f": "여",
}

def normalize_gender(g):
    s = str(g or "").strip()
    return _GENDER_NORM_MAP.get(s.lower(), s)
# ✨ [추가됨] 대조 결과로 보여줄 최대 후보 수 (너무 많으면 메시지가 길어지므로 상위 N개만)
VOICE_MATCH_DISPLAY_LIMIT = 5


def format_voice_match_lines(matches, limit=VOICE_MATCH_DISPLAY_LIMIT, claimed_nickname=None):
    """블랙리스트 대조 결과(matches)를 유사도 내림차순으로 정렬해, 상위 `limit`건의
    대조 대상 정보(닉네임/성별/상태)와 일치율을 사람이 읽기 좋은 줄 리스트로 만듭니다.

    ✨ [추가됨] 예전에는 일치율이 VOICE_MATCH_ALERT_THRESHOLD(90%) 이상인 것만, 그마저
    없으면 가장 유사한 1건만 보여줬습니다. 운영진이 "어떤 자료와 얼마나 일치했는지"를
    임계치와 무관하게 항상 확인할 수 있도록, 매칭이 하나라도 있으면 상위 여러 건을
    함께 보여주도록 변경했습니다. 임계치 이상인 건은 ⚠️로 강조 표시합니다."""
    if not matches:
        return []
    sorted_matches = sorted(matches, key=lambda m: (m.get("similarity") or 0), reverse=True)
    shown = sorted_matches[:limit]
    lines = []
    for m in shown:
        similarity = (m.get("similarity") or 0) * 100
        nickname = m.get("nickname", "알 수 없음")
        # RPC가 gender/status를 NULL로 돌려주는 경우가 있어 값이 있을 때만 표시한다 ("None" 방지)
        match_gender = m.get("gender")
        match_status = m.get("status")
        mark = "⚠️ " if similarity >= VOICE_MATCH_ALERT_THRESHOLD * 100 else "└ "
        same_name_note = ""
        if claimed_nickname and str(nickname).strip() == str(claimed_nickname).strip():
            same_name_note = " ← 신청 닉네임과 동일 (동일인 의심)"
        is_member = _match_is_member(m)
        kind = "기존 신입 음성" if is_member else "블랙리스트 음성"
        info_parts = []
        if match_gender: info_parts.append(f"과거 성별: {match_gender}")
        info_parts.append(f"일치율: {similarity:.1f}%")
        if (is_member and claimed_nickname and not same_name_note
                and similarity >= VOICE_MATCH_ALERT_THRESHOLD * 100):
            same_name_note = " ← ⚠️ 다른 닉네임으로 재입장 의심"
        lines.append(f"  {mark}[{kind}] 닉네임: {nickname} ({' / '.join(info_parts)}){same_name_note}")
    remaining = len(sorted_matches) - len(shown)
    if remaining > 0:
        lines.append(f"  (그 외 {remaining}건 더 있음 — 유사도 낮음, 생략)")
    return lines


def _format_gender_signals(voice_result):
    """성별 추정에 쓰인 신호(피치/성도 길이/임베딩)를 ' (피치 120Hz / 성도길이 17.5cm / 임베딩 여성확률 3%)' 형태로."""
    sig = voice_result.get("gender_signals") or {}
    parts = []
    pitch = sig.get("pitch_hz") or voice_result.get("pitch_hz")
    if pitch:
        parts.append(f"피치 {pitch}Hz")
    if sig.get("vtl_cm") is not None:
        parts.append(f"성도길이 {sig['vtl_cm']}cm")
    if sig.get("embedding_female_pct") is not None:
        parts.append(f"임베딩 여성확률 {sig['embedding_female_pct']}%")
    return f" ({' / '.join(parts)})" if parts else ""


# def build_voice_check_report_lines(*, claimed_gender, voice_result, claimed_nickname=None):
#     """음성 자동분석 결과에서 '참고할 만한 내용'만 사람이 읽기 좋은 줄 리스트로 뽑아냅니다.
#     (운영진방 알림, 'N번방 확인' 저장용 리포트에서 공통으로 사용)
#     특이사항이 없으면 빈 리스트를 반환합니다.
#     """
#     lines = []
#     if not voice_result or voice_result.get("error"):
#         return lines

#     est_gender = voice_result.get("estimated_gender")
#     claimed_gender_norm = _GENDER_NORM_MAP.get((claimed_gender or "").strip())
#     gender_mismatch = bool(est_gender and claimed_gender_norm and est_gender != claimed_gender_norm)

#     signal_note = _format_gender_signals(voice_result)
#     if est_gender:
#         mismatch_note = " ⚠️ 신청서 성별과 다름" if gender_mismatch else ""
#         lines.append(f"- 음성 기반 추정 성별: {est_gender}{signal_note}{mismatch_note}")
#     elif voice_result.get("gender_uncertain"):
#         lines.append(f"- 음성 기반 추정 성별: 경계/판단 불확실{signal_note} — 참고만 해주세요")
#     if voice_result.get("gender_note"):
#         lines.append(f"- ⚠️ {voice_result['gender_note']} (참고용 정황, 확정 판정 아님)")

#     # ✨ [추가됨] 오토튠/피치보정 의심도. 확정 판정이 아니라 정황 지표이므로 항상 그 취지를 함께 남긴다.
#     autotune_prob = voice_result.get("autotune_probability")
#     if autotune_prob is not None:
#         if autotune_prob >= AUTOTUNE_ALERT_THRESHOLD:
#             level = "⚠️ 높음(의심)"
#         elif autotune_prob >= 20:
#             level = "중간"
#         else:
#             level = "낮음"
#         lines.append(f"- 오토튠/피치보정 의심도: {autotune_prob:.1f}% ({level}) — 참고용 정황 지표, 확정 판정 아님")

#     matches = voice_result.get("matches") or []
#     if matches:
#         strong = [m for m in matches if (m.get("similarity") or 0) >= VOICE_MATCH_ALERT_THRESHOLD]
#         if any(not _match_is_member(m) for m in strong):
#             header = "- ⚠️⚠️ 블랙리스트 음성과 매우 유사한 대조 결과 (동일인 의심):"
#         elif strong:
#             header = "- ⚠️ 기존 신입 음성과 매우 유사 (재입장 의심 — 닉네임/계정 변경 가능성):"
#         else:
#             header = "- 기존 음성 대조 결과 (임계치 미만 — 참고용):"
#         lines.append(header)
#         lines.extend(format_voice_match_lines(matches, claimed_nickname=claimed_nickname))

#     # ✨ [추가됨] Cloud Run(app.py)이 90% 이상 일치로 판단해 신규 저장을 생략한 경우 — 에러는 아니지만
#     # voice_profiles에 새 레코드가 없다는 뜻이므로 운영진이 참고할 수 있게 남긴다.
#     if voice_result.get("is_strong_match"):
#         _thr = voice_result.get("save_threshold")
#         _thr_txt = f"{_thr * 100:.0f}%" if isinstance(_thr, (int, float)) else "저장 제외 기준"
#         lines.append(f"- ℹ️ 블랙리스트 일치율이 {_thr_txt} 이상으로 판단되어 신규 음성 프로필 저장은 생략됨")

#     # ✨ [추가됨] 분석 자체는 성공했지만 Storage 업로드/voice_profiles insert가 실패한 경우.
#     # 신입에게는 노출되지 않고(🔵 완료로만 보임) 운영진만 이 리포트로 확인할 수 있다.
#     save_error = voice_result.get("save_error")
#     if save_error:
#         lines.append(f"- ⚠️ 음성 원본/프로필 저장 실패: {save_error} (voice_profiles 미등록 — 재확인 필요)")

#     return lines

def build_voice_check_report_lines(*, claimed_gender, voice_result, claimed_nickname=None):
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

    signal_note = _format_gender_signals(voice_result)
    if est_gender:
        mismatch_note = " ⚠️ 신청서 성별과 다름" if gender_mismatch else ""
        lines.append(f"- 음성 기반 추정 성별: {est_gender}{signal_note}{mismatch_note}")
    elif voice_result.get("gender_uncertain"):
        lines.append(f"- 음성 기반 추정 성별: 경계/판단 불확실{signal_note} — 참고만 해주세요")
    if voice_result.get("gender_note"):
        lines.append(f"- ⚠️ {voice_result['gender_note']} (참고용 정황, 확정 판정 아님)")

    # 오토튠/피치보정 의심도. 확정 판정이 아니라 정황 지표이므로 항상 그 취지를 함께 남긴다.
    autotune_prob = voice_result.get("autotune_probability")
    if autotune_prob is not None:
        if autotune_prob >= AUTOTUNE_ALERT_THRESHOLD:
            level = "⚠️ 높음(의심)"
        elif autotune_prob >= 20:
            level = "중간"
        else:
            level = "낮음"
        lines.append(f"- 오토튠/피치보정 의심도: {autotune_prob:.1f}% ({level}) — 참고용 정황 지표, 확정 판정 아님")

    # ✨ 블랙리스트 대조 자체가 실패한 경우 (매칭이 없는 것과 다름)
    if voice_result.get("match_error"):
        lines.append(f"- ⚠️⚠️ 블랙리스트 대조 실패: {voice_result['match_error']} (대조가 안 된 상태 — 직접 확인 필요)")

    matches = voice_result.get("matches") or []
    if matches:
        strong = [m for m in matches if (m.get("similarity") or 0) >= VOICE_MATCH_ALERT_THRESHOLD]
        if any(not _match_is_member(m) for m in strong):
            header = "- ⚠️⚠️ 블랙리스트 음성과 매우 유사한 대조 결과 (동일인 의심):"
        elif strong:
            header = "- ⚠️ 기존 신입 음성과 매우 유사 (재입장 의심 — 닉네임/계정 변경 가능성):"
        else:
            header = "- 기존 음성 대조 결과 (임계치 미만 — 참고용):"
        lines.append(header)
        lines.extend(format_voice_match_lines(matches, claimed_nickname=claimed_nickname))

    # 90% 이상 일치로 판단해 신규 저장을 생략한 경우
    if voice_result.get("is_strong_match"):
        _thr = voice_result.get("save_threshold")
        _thr_txt = f"{_thr * 100:.0f}%" if isinstance(_thr, (int, float)) else "저장 제외 기준"
        lines.append(f"- ℹ️ 블랙리스트 일치율이 {_thr_txt} 이상으로 판단되어 신규 음성 프로필 저장은 생략됨")

    # 분석은 성공했지만 Storage 업로드/voice_profiles insert가 실패한 경우
    save_error = voice_result.get("save_error")
    if save_error:
        lines.append(f"- ⚠️ 음성 원본/프로필 저장 실패: {save_error} (voice_profiles 미등록 — 재확인 필요)")

    return lines

def build_voice_check_report_text(*, claimed_gender, voice_result, claimed_nickname=None):
    """'N번방 확인' 명령어 응답 및 DB(voice_check_report) 저장에 쓰는 한 덩어리 요약 텍스트."""
    if not voice_result or voice_result.get("error"):
        reason = (voice_result or {}).get("error", "결과 없음")
        return f"자동분석 실패({reason}) — 운영진이 음성을 직접 듣고 판단해 주세요."

    lines = build_voice_check_report_lines(claimed_gender=claimed_gender, voice_result=voice_result, claimed_nickname=claimed_nickname)
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


COLOR_LEGEND_TEXT = (
    "🎨 중복/블랙 필터링 색상 안내\n\n"
    "⚫ 블랙리스트 유저 감지\n"
    "🔴 닉네임 및 모든 정보 일치\n"
    "🟠 동일 ID인데 성별/년생이 이전과 다름\n"
    "🟪 재입장 유저 (동일 ID 확인)\n"
    "🟡 닉네임 및 정보 일부 일치\n"
    "🟦 닉네임만 일치\n"
    "🟢 이상 없음 (중복/블랙 이력 없음)\n\n"
    "※ 대조 대상: 시트/DB 블랙리스트 + 음성DB\n"
    "※ 고유ID 일치 = 동일인 / 닉네임 일치 = 동일인 의심"
)


def format_form_basic_info(*, nickname, birth_year, gender, region, marriage,
                           yadan, yadan_room, prev_nick):
    """'N번방 확인' 리포트에 붙이는 1번 양식 기본 정보 블록.
    값이 입력된 항목만 한 줄씩 보여준다 (빈 항목은 줄 자체를 생략)."""
    rows = [
        ("닉네임", nickname),
        ("년생", f"{birth_year}년생" if str(birth_year or "").strip() else ""),
        ("성별", gender),
        ("지역", region),
        ("결혼유무", marriage),
        ("야방경험", yadan),
        ("야방 방이름", yadan_room),
        ("쓰던닉", prev_nick),
    ]
    lines = ["👤 [기본 정보]"]
    for label, val in rows:
        val = str(val or "").strip()
        if val:
            lines.append(f"- {label}: {val}")
    return "\n".join(lines)


def split_for_line(text, limit=4800, max_parts=5):
    """LINE 텍스트 메시지(최대 5000자) 제한에 맞춰 줄 단위로 나눕니다. (한 번의 reply에 최대 5개)"""
    parts, cur = [], ""
    for line in str(text).split("\n"):
        while len(line) > limit:  # 한 줄이 너무 길면 강제로 자름
            if cur:
                parts.append(cur); cur = ""
            parts.append(line[:limit]); line = line[limit:]
        if len(cur) + len(line) + 1 > limit and cur:
            parts.append(cur); cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        parts.append(cur)
    if len(parts) > max_parts:
        parts = parts[:max_parts]
        parts[-1] = parts[-1][:limit - 20] + "\n…(이하 생략)"
    return parts


def format_nickname_history(nickname):
    """관리자방 '/닉네임 확인'용: 해당 닉네임의 과거 검증 기록 + 음성DB 기록을 모아 문자열로 돌려줍니다.
    기록이 전혀 없으면 None. (닉네임 완전일치를 우선 보고, 없으면 부분일치로 한 번 더 찾습니다.)"""
    name = str(nickname or "").strip()
    if not supabase or not name:
        return None

    val_cols = ('user_id, nickname, gender, region, birth_year, status, entry_date, retry_count, '
                'black_reason, details, voice_check_report, '
                'voice_analysis_state, voice_analysis_result, voice_analysis_error, '
                'prev_nick, yadan_room')
    rows, partial = [], False
    try:
        res = supabase.table('user_validations').select(val_cols).eq('nickname', name).execute()
        rows = res.data or []
        if not rows and "%" not in name and "_" not in name:
            res = supabase.table('user_validations').select(val_cols).ilike('nickname', f"%{name}%").limit(10).execute()
            rows = res.data or []
            partial = bool(rows)
    except Exception as e:
        print(f"닉네임 확인 - user_validations 조회 실패: {e}")
        return "⚠️ 닉네임 기록 조회에 실패했습니다. 잠시 후 다시 시도해 주세요."

    # ✨ [추가됨] 운영진이 닉네임을 바꿔둔('/새닉 변경') 유저도, 처음 양식에 적었던 닉네임(details.form_nickname)으로 찾을 수 있게 함.
    # (details가 jsonb 컬럼이 아니면 이 조회만 실패하고, 위의 기본 조회 결과는 그대로 사용됨)
    try:
        res2 = supabase.table('user_validations').select(val_cols).eq('details->>form_nickname', name).execute()
        have = {str(r.get('user_id')) for r in rows}
        rows += [r for r in (res2.data or []) if str(r.get('user_id')) not in have]
    except Exception as e:
        print(f"닉네임 확인 - form_nickname 조회 실패(무시): {e}")

    # 음성DB: 닉네임 일치 + 위에서 찾은 유저ID 일치 (임베딩 등 무거운 컬럼은 가져오지 않음)
    uids = [str(r.get('user_id')).strip() for r in rows if r.get('user_id')]
    vp_rows = {}
    def _fetch_vp(cols):
        found = []
        q = supabase.table('voice_profiles').select(cols).eq('nickname', name).execute()
        found += q.data or []
        if uids:
            q2 = supabase.table('voice_profiles').select(cols).in_('user_id', uids).execute()
            found += q2.data or []
        return found
    try:
        try:
            vp_list = _fetch_vp('user_id, nickname, source, black_reason')
        except Exception:
            vp_list = _fetch_vp('user_id, nickname, source')
        for vp in vp_list:
            key = (str(vp.get('user_id') or ''), str(vp.get('nickname') or ''), str(vp.get('source') or ''))
            vp_rows[key] = vp
    except Exception as e:
        print(f"닉네임 확인 - voice_profiles 조회 실패: {e}")

    if not rows and not vp_rows:
        return None

    def _vp_kind(vp):
        return "신입 음성인증" if vp.get('source') == 'new_member' else "블랙리스트 음성 등록"

    vp_by_uid = {}
    for vp in vp_rows.values():
        vp_by_uid.setdefault(str(vp.get('user_id') or ''), []).append(vp)

    out = [f"🔎 [{name}] 과거 기록" + (" (닉네임 부분일치)" if partial else "")]
    shown_vp_keys = set()

    if rows:
        out.append(f"\n📚 검증 기록 {len(rows)}건")
    for i, r in enumerate(rows, 1):
        uid = str(r.get('user_id') or '').strip()
        details = r.get('details') or {}
        if isinstance(details, str):
            try: details = json.loads(details)
            except Exception: details = {}
        lines = [
            f"\n[{i}] {r.get('nickname') or '-'} / {_year_label(r.get('birth_year'))}년생 / {r.get('gender') or '-'} / {r.get('region') or '-'}",
            f" - 유저ID: {uid or '-'}",
            f" - 상태: {r.get('status') or '-'} / 재시도 {r.get('retry_count') or 1}회 / 입장일: {r.get('entry_date') or '-'}",
        ]
        if str(r.get('black_reason') or '').strip():
            lines.append(f" - 💀 블랙사유: {str(r.get('black_reason')).strip()}")
        detail_lines = []
        for key, label in DETAIL_LABELS:
            val = str(details.get(key, "")).strip() if isinstance(details, dict) else ""
            if val:
                detail_lines.append(f"{label}: {val}")
        # ✨ [추가됨] DB 컬럼에만 있는 값(시트 /디비업데이트로 들어온 값 등)도 함께 표시
        if str(r.get('prev_nick') or '').strip() and not any('쓰던닉' in d for d in detail_lines):
            detail_lines.append(f"쓰던닉: {r.get('prev_nick')}")
        if str(r.get('yadan_room') or '').strip() and not any('야방 방이름' in d for d in detail_lines):
            detail_lines.append(f"야방 방이름: {r.get('yadan_room')}")
        if detail_lines:
            lines.append(f" - 📝 입력내용: {' / '.join(detail_lines)}")

        # ✨ [추가됨] 운영진 닉변 기록 + 이전 제출 이력(최근 3건)
        _d = _load_details(details)
        for c in (_d.get('nick_changes') or [])[-3:]:
            lines.append(f" - 🔁 닉변: {c.get('from')} → {c.get('to')} ({c.get('at')})")
        _hist = _d.get('history') or []
        if _hist:
            lines.append(f" - 🕘 이전 제출 {len(_hist)}건:")
            for h in _hist[-3:]:
                lines.append(f"   · {h.get('at')} {h.get('form_nickname') or h.get('nickname')} / {_year_label(h.get('birth_year'))}년생 / {h.get('gender')} / {h.get('region')}")

        for vp in vp_by_uid.get(uid, []):
            shown_vp_keys.add((str(vp.get('user_id') or ''), str(vp.get('nickname') or ''), str(vp.get('source') or '')))
            extra = f" / 사유: {vp.get('black_reason')}" if vp.get('black_reason') else ""
            lines.append(f" - 🎙️ 음성DB: {_vp_kind(vp)}{extra}")
        vr = str(r.get('voice_check_report') or '').strip()
        # ✨ [추가됨] voice_check_report가 비어 있는 기록(기존 멤버 등록 등)은
        # 저장된 자동분석 결과로 요약을 만들어 보여준다.
        _vstate = r.get('voice_analysis_state')
        if not vr and _vstate == "완료" and r.get('voice_analysis_result'):
            vr = build_voice_check_report_text(
                claimed_gender=r.get('gender'),
                voice_result=drop_self_matches(r.get('voice_analysis_result'), uid),
                claimed_nickname=r.get('nickname'),
            )
        elif not vr and _vstate == "에러":
            vr = f"🔴 자동분석 오류: {r.get('voice_analysis_error') or '(사유 미상)'}"
        elif not vr and _vstate == "처리중":
            vr = "⏳ 자동분석 진행 중 (잠시 후 다시 확인해 주세요)"
        if vr:
            lines.append(f" - 🎙️ 음성확인 요약:\n{vr[:700]}{'…' if len(vr) > 700 else ''}")
        out.append("\n".join(lines))

    extra_vps = [vp for k, vp in vp_rows.items() if k not in shown_vp_keys]
    if extra_vps:
        out.append(f"\n🎙️ 음성DB 기록 {len(extra_vps)}건 (검증 기록과 연결되지 않은 것)")
        for vp in extra_vps:
            extra = f" / 사유: {vp.get('black_reason')}" if vp.get('black_reason') else ""
            out.append(f" - {vp.get('nickname') or '(닉네임 없음)'} / {_vp_kind(vp)} / 유저ID: {vp.get('user_id') or '-'}{extra}")

    return "\n".join(out)


def format_voice_check_status(user_id):
    """'N번방 확인' 명령어에서 특정 유저의 음성검증 진행 상태를 사람이 읽기 좋은 문장으로 만들어 돌려줍니다.
    - 음성검증까지 확인이 끝났는지 여부
    - 확인이 된 시점이라면 그때 저장해 둔 확인 내용(자동분석 요약)
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

    # ✅ [수정] voice_check_report가 아직 DB에 저장되지 못한 경우를 구제한다.
    # voice_analysis_result(음성분석 서버가 저장한 원본 결과)가 있으면 그 자리에서 읽기 전용으로만
    # 리포트 문자열을 재구성해서 보여준다 — DB에는 아무것도 쓰지 않는다.
    if not report and row.get('voice_analysis_state') == "완료" and row.get('voice_analysis_result'):
        report = build_voice_check_report_text(
            claimed_gender=row.get('gender'),
            voice_result=drop_self_matches(row.get('voice_analysis_result'), user_id),
            claimed_nickname=row.get('nickname'),
        )

    # ✨ [추가됨] 자동분석 에러는 신입에게는 절대 보여주지 않고, 운영진이 'N번방 확인'을 했을 때만
    # 노출한다. voice_analysis_state는 신입의 답장 여부(status)와 무관하게 오디오 도착 시점부터
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
        note = "자동분석 진행 중" if status == "분석중" else "음성 확인 대기 중"
        return f"🎙️ 음성검증: ⏳ 음성 파일 제출 완료, {note}{analysis_error_note}"
    # ✨ 음성 제출 이후 단계들(닉변/퇴장/운영진 추가확인)은 자동분석 요약을 같은 방식으로 보여준다.
    _after_voice_headers = {
        "승인대기": "🎙️ 음성검증: ✅ 음성 파일 제출 완료 (운영진 최종 승인 대기 중)",
        "완료": "🎙️ 음성검증: ✅ 음성 파일 제출 + 운영진 최종 승인까지 완료",
        "닉변대기": "🎙️ 음성검증: ✅ 음성 파일 제출 완료 (신입 닉네임 변경 대기 중)",
        "헤르페스확인대기": "🎙️ 음성검증: ✅ 음성 확인 완료 (헤르페스 질문 답변 대기 중)",
        "퇴장대기": "🎙️ 음성검증: ✅ 음성 확인 완료 (퇴장 안내 발송됨, 신입 퇴장 대기 중)",
        "관리자검토대기": "🎙️ 음성검증: ✅ 음성 확인 완료 (운영진 추가 확인 대기 중)",
    }
    if status in _after_voice_headers:
        header = _after_voice_headers[status]
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

    voice_result = drop_self_matches(voice_result, user_id)
    matches = voice_result.get("matches") or []
    strong_matches = [m for m in matches if (m.get("similarity") or 0) >= VOICE_MATCH_ALERT_THRESHOLD]

    est_gender = voice_result.get("estimated_gender")
    claimed_gender_norm = _GENDER_NORM_MAP.get((claimed_gender or "").strip())
    gender_mismatch = bool(est_gender and claimed_gender_norm and est_gender != claimed_gender_norm)

    save_error = voice_result.get("save_error")

    autotune_prob = voice_result.get("autotune_probability")
    autotune_suspected = bool(autotune_prob is not None and autotune_prob >= AUTOTUNE_ALERT_THRESHOLD)

    gender_suspect = bool(voice_result.get("gender_note"))  # 피치와 성도길이/음색이 강하게 충돌 (톤 조작 의심)

    if not strong_matches and not gender_mismatch and not save_error and not autotune_suspected and not gender_suspect:
        return

    lines = [f"🎙️ 음성 자동분석 참고 알림 — {claimed_nickname or '(닉네임 미상)'}님 (신청 성별: {claimed_gender or '미상'})"]
    lines.extend(build_voice_check_report_lines(claimed_gender=claimed_gender, voice_result=voice_result, claimed_nickname=claimed_nickname))

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
# ✨ [추가됨] 관리자방 '/음성업로드' — 운영진이 직접 제출한 음성을 블랙리스트에 수동 등록
# ==========================================
def handle_admin_blacklist_voice_upload(event, admin_user_id):
    """관리자방에서 '/음성업로드' 명령 직후 도착한 음성 메시지를 처리합니다.
    대기 중인 업로드 요청(pending_blacklist_voice_upload)이 없으면 조용히 무시합니다
    (관리자방에는 인증과 무관한 음성이 오갈 수 있으므로)."""
    session_data = get_user_session(admin_user_id) or {}
    pending = session_data.get("pending_blacklist_voice_upload") if isinstance(session_data, dict) else None
    if not pending:
        return

    del_user_session(admin_user_id)  # 1회성 소모 (중복 처리 방지)

    nickname = pending.get("nickname", "")
    gender = pending.get("gender", "")
    reason = pending.get("reason", "")

    result = process_admin_blacklist_voice_upload(
        supabase, configuration,
        message_id=event.message.id, nickname=nickname, gender=gender,
        black_reason=reason, registered_by=admin_user_id,
        room_id=ADMIN_GROUP_CHAT_ID,   # ✨ 추가
    )

    if result.get("error"):
        reply_text = f"❌ 음성 분석/업로드 중 오류가 발생했습니다: {result['error']}"
    else:
        lines = [f"✅ 블랙리스트 음성 등록 완료 — {nickname} ({gender})"]
        if reason:
            lines.append(f"- 사유: {reason}")
        if result.get("blacklist_user_id"):
            lines.append(f"- 등록 ID: {result['blacklist_user_id']}")
        if result.get("pitch_hz"):
            lines.append(f"- 분석 피치: {result['pitch_hz']}Hz (추정 성별: {result.get('estimated_gender') or ('경계/불확실' if result.get('gender_uncertain') else '알수없음')})")
            # ✨ [추가됨] 성도길이/임베딩 신호, 톤 조작 의심 메모, 입력 성별과 추정 성별 불일치 표시
            _sig = _format_gender_signals(result)
            if result.get("gender_signals") and _sig:
                lines.append(f"- 성별 판정 신호:{_sig}")
            if result.get("gender_note"):
                lines.append(f"- ⚠️ {result['gender_note']} (참고용 정황, 확정 판정 아님)")
            _est = result.get("estimated_gender")
            if gender in ("남", "여") and _est in ("남", "여") and gender != _est:
                lines.append(f"- ⚠️ 입력한 성별({gender})과 음성 추정 성별({_est})이 다릅니다 — 톤 조작 가능성")

        matches = result.get("matches") or []
        if matches:
            has_strong = any((m.get("similarity") or 0) >= VOICE_MATCH_ALERT_THRESHOLD for m in matches)
            header = (
                "\n⚠️ 기존 등록 음성(블랙리스트/기존 신입)과 매우 유사한 음성이 있습니다 (중복 인물 가능성):"
                if has_strong else
                "\n(참고) 기존 등록 음성 대조 결과:"
            )
            lines.append(header)
            lines.extend(format_voice_match_lines(matches, claimed_nickname=nickname))

        reply_text = "\n".join(lines)

    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
        )


# ==========================================
# [핸들러 4] 📌 음성 메시지 처리 핸들러 (마지막 입장 유저만 작동)
# ==========================================
@handler.add(MessageEvent, message=AudioMessageContent)
def handle_audio(event):
    user_id = event.source.user_id
    if not user_id:
        return

    source_id = getattr(event.source, 'group_id', getattr(event.source, 'room_id', event.source.user_id))

    # ✨ [추가됨] 관리자방에서 '/음성업로드' 명령 직후 온 음성 -> 블랙리스트 수동 등록 플로우로 분기.
    # (신입 검증용 last-joined 체크는 적용하지 않는다 — 운영진 본인이 보내는 음성이므로.)
    if source_id == ADMIN_GROUP_CHAT_ID:
        # ✨ '/기존멤버등록' 양식 제출 직후의 음성이면 기존 멤버 음성 등록, 아니면 기존 블랙리스트 업로드 플로우
        if handle_member_register_voice(event, user_id):
            return
        handle_admin_blacklist_voice_upload(event, user_id)
        return

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
        # 중복 웹훅 방지: 음성대기 → 닉변대기 선점에 성공한 요청만 처리
        if not cas_update_status(user_id, "음성대기", "닉변대기", label="음성 접수(닉변대기 전환)"):
            return
        sync_status_to_sheet(user_id, "닉변대기")

        # 오라클 VM(음성분석 서버)에 분석을 "접수"만 시키고 끝난다 (결과를 기다리지 않음).
        # 실제 분석(임베딩 추출, 성별 추정, 블랙리스트 대조)과 DB 저장은 서버가 백그라운드로 진행하다가
        # 끝나면 Supabase에 직접 결과를 기록한다. '완료' 입력 시점에 그 결과를 조회만 한다.
        try:
            submit_voice_analysis_job(
                supabase, configuration,
                message_id=event.message.id, user_id=user_id, nickname=claimed_nickname,
                room_id=source_id,
            )
        except Exception as e:
            print(f"⚠️ 음성분석 접수 실패 -> 음성대기로 되돌림: {e}")
            cas_update_status(user_id, "닉변대기", "음성대기", label="음성 접수 실패 롤백")
            sync_status_to_sheet(user_id, "음성대기")
            return

        # ① 변경할 닉네임(성별별) → ② 닉변/프로필 안내
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(reply_token=event.reply_token, messages=build_nickchange_messages(user_id))
            )


def format_voice_analysis_reply_text(user_id):
    """신입이 '완료'를 입력했을 때(finalize_after_nickname_check) 호출된다.
    음성분석 서버를 다시 부르지 않고, 오디오 도착 시점에 이미 시작된 백그라운드 분석의 현재 상태
    (voice_analysis_state)를 DB에서 "조회"만 한다.

    ※ 새 흐름에서는 반환 문구를 신입에게 보내지 않는다. 이 함수의 실제 목적은 부수효과
      (운영진방 참고 알림 notify_admin_voice_analysis + voice_check_report/voice_checked_at 저장)다.

    🔵 = 분석 완료, 결과 조회 가능
    🟡 = 아직 분석 진행 중 (또는 접수 기록조차 아직 없는 경우)
    🔴 = 에러/멈춤 (신입에게는 상세 사유를 노출하지 않음)

    상태와 무관하게 항상 (문자열, 신청 시 적은 성별) 튜플을 반환하고, 예외를 던지지 않는다.
    """
    PENDING_TEXT_FOR_MEMBER = (
        "🟡 자동분석이 아직 진행 중입니다. 완료되는 대로 운영진 확인 시 함께 반영됩니다.\n\n"
        f"{FINAL_APPROVAL_WAIT_TEXT}"
    )

    if not supabase or not user_id:
        # DB 연결 실패 같은 내부 사정도 신입에게는 노출하지 않는다.
        return PENDING_TEXT_FOR_MEMBER, ""

    res = supabase_execute(
        lambda: supabase.table('user_validations')
            .select('nickname, gender, voice_analysis_state, voice_analysis_result, voice_analysis_error, voice_analysis_synced_at')
            .eq('user_id', user_id).execute(),
        label="음성분석 결과 조회(완료)"
    )
    row = (res.data[0] if res and res.data else {}) or {}
    claimed_nickname = row.get('nickname') or ""
    claimed_gender = row.get('gender') or ""
    state = row.get('voice_analysis_state')
    result = row.get('voice_analysis_result') or {}

    if state == "완료":
        result = drop_self_matches(result, user_id) or result
        # voice_analysis_result는 음성분석 서버가 채워 넣는 결과(estimated_gender/matches/pitch_hz 등)
        # → 기존 리포트/알림 함수를 그대로 재사용.
        report_text = build_voice_check_report_text(claimed_gender=claimed_gender, voice_result=result, claimed_nickname=claimed_nickname)
        notify_admin_voice_analysis(
            claimed_nickname=claimed_nickname, claimed_gender=claimed_gender,
            user_id=user_id, voice_result=result,
        )
        kst = datetime.timezone(datetime.timedelta(hours=9))
        supabase_execute(
            lambda: supabase.table('user_validations').update({
                # ✨ 'N번방 확인' 명령어(format_voice_check_status)가 이 두 컬럼을 그대로 읽으므로 함께 채워 둔다.
                "voice_checked_at": datetime.datetime.now(kst).strftime("%Y-%m-%d %H:%M"),
                "voice_check_report": report_text,
            }).eq('user_id', user_id).execute(),
            label="음성검증 확인정보 저장(비동기 파이프라인)"
        )
        # report_text(블랙리스트 대조 대상 닉네임/일치율 등 상세 내용)는 절대 신입 본인 채팅방에 보내지 않는다.
        return f"🔵 자동분석이 완료되었습니다.\n\n{FINAL_APPROVAL_WAIT_TEXT}", claimed_gender

    # 🔴 관리자방('N번방 확인')과 같은 기준: 에러이거나, '처리중'인 채로 일정 시간 이상 멈춘 경우.
    if state == "에러" or _voice_analysis_is_stale(row):
        return (
            "🔴 자동분석 중 문제가 발생했습니다. 운영진이 직접 확인해 드릴 예정입니다.\n\n"
            f"{FINAL_APPROVAL_WAIT_TEXT}"
        ), claimed_gender

    # 🟡 처리중이거나 아직 기록 자체가 없는 경우(레이스 컨디션 등)
    return PENDING_TEXT_FOR_MEMBER, claimed_gender


if __name__ == "__main__":
    app.run(port=5000)

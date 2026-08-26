import os
import time
import random
import json
import gspread
import datetime
import threading
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
from linebot.v3.webhooks import (
    MessageEvent, TextMessageContent, MemberJoinedEvent, MemberLeftEvent, AudioMessageContent
)

app = Flask(__name__)

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
if client:
    try:
        sheet = client.open("인증멘트").worksheet("멘트")
        validation_sheet = client.open("인증멘트").worksheet("검증")
        room_manage_sheet = client.open("인증멘트").worksheet("방관리")
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
    """해당 방의 인증 상태를 초기화합니다.
    - '.' 명령어와 신입 유저 퇴장(MemberLeftEvent) 양쪽에서 공용으로 사용합니다.
    - target_user_id를 명시하지 않으면 현재 room_state에 기록된 유저를 대상으로 합니다.
    - 반환값: 실제로 초기화된 user_id (없으면 None)
    """
    room_state = get_room_state(source_id)
    if target_user_id is None:
        target_user_id = room_state.get('user_id') if room_state else None

    if target_user_id:
        del_user_session(target_user_id)
        try:
            if supabase:
                supabase.table('user_validations').update({"status": "입장대기"}).eq('user_id', target_user_id).execute()

            with sheet_sync_lock():
                if validation_sheet:
                    raw_user_ids = validation_sheet.col_values(5)
                    clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                    if target_user_id in clean_user_ids:
                        row_index = clean_user_ids.index(target_user_id) + 1
                        validation_sheet.update(range_name=f'L{row_index}', values=[["입장대기"]])
        except Exception as e:
            print(f"유저 상태 초기화 처리 중 에러: {e}")

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


@app.route("/api", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature')
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
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
    
    # 관리자 명령어('.', '/')가 아닌 일반 채팅에 한해 검증을 진행합니다.
    if not (user_message == "." or user_message.startswith("/")):
        if not is_last_joined_user(source_id, user_id):
            return

    # 0. 점(.) 입력 시 해당 방의 인증 상태 초기화 (관리자 또는 누구나 실행 가능)
    if user_message == ".":
        reset_verification_state(source_id)

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

                # A. '멘트' 시트 동기화
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
                        supabase.table('auth_ments').delete().neq('keyword', '_DELETE_ALL_KEY_').execute()
                        process_in_chunks('auth_ments', ments_records, is_insert=True)
                        sync_reports.append(f"• 멘트: {len(ments_records)}개 키워드")

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
                        val_records = []
                        for row in all_val_data[1:]:
                            u_id = str(row[4]).strip() if len(row) > 4 else ""
                            if u_id:
                                val_records.append({
                                    "user_id": u_id,
                                    "nickname": str(row[0]).strip() if len(row) > 0 else "",
                                    "gender": str(row[1]).strip() if len(row) > 1 else "",
                                    "region": str(row[2]).strip() if len(row) > 2 else "",
                                    "birth_year": str(row[3]).strip() if len(row) > 3 else "",
                                    "entry_date": str(row[5]).strip() if len(row) > 5 else "",
                                    "black_reason": str(row[6]).strip() if len(row) > 6 else "",
                                    "retry_count": int(row[7]) if len(row) > 7 and row[7].isdigit() else 1,
                                    "status": str(row[11]).strip() if len(row) > 11 else "입장대기"
                                })
                        if val_records and supabase:
                            # 데이터가 많을 경우를 대비해 청크(분할) 업데이트 적용
                            process_in_chunks('user_validations', val_records, is_insert=False)
                            sync_reports.append(f"• 검증이력: {len(val_records)}명 유저 데이터")

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
                    if status == 'joined':
                        if is_known:
                            reply_text = f"⚠️ [{room_name_input}]\n기존 방문/블랙리스트 이력이 있는 유저가 방금 입장했습니다!\n(현재 상대방이 양식을 입력 중입니다)"
                        else:
                            reply_text = f"⏳ [{room_name_input}]\n완전한 신규 유저가 현재 양식을 입력 중입니다."
                    elif status == 'form_submitted':
                        alert_report = room_state.get('report', f"[{room_name_input}] 양식이 접수되었습니다.")
                        reply_text = alert_report
            else:
                reply_text = f"❌ '{room_name_input}' 정보를 DB/방관리 시트에서 찾을 수 없습니다."

            if reply_text:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
            return

    # 📌 [핵심 검증 1] 1번 양식 제출 처리 (마지막 입장 유저만 작동)
    if all(k in user_message for k in ["닉네임", "년생", "성별", "지역"]):
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

        if supabase:
            try:
                # 1000개 이상 제약 해결: 페이지네이션 기반 전체 데이터 호출
                all_val_data = get_all_supabase_data('user_validations')

                for row in all_val_data:
                    rec_id = str(row.get('user_id', '')).strip()
                    rec_name = str(row.get('nickname', '')).strip()
                    rec_year = str(row.get('birth_year', '')).strip()
                    rec_gender = str(row.get('gender', '')).strip()
                    rec_region = str(row.get('region', '')).strip()
                    rec_black = str(row.get('black_reason', '')).strip()

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
                user_res = supabase.table('user_validations').select('retry_count, status').eq('user_id', user_id).execute()
                retry_cnt = 1
                if user_res.data:
                    retry_cnt = user_res.data[0].get('retry_count', 1) + 1

                supabase.table('user_validations').upsert({
                    "user_id": user_id,
                    "nickname": nickname,
                    "gender": gender,
                    "region": region,
                    "birth_year": birth_year,
                    "entry_date": current_date,
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

                        update_data_basic = [nickname, gender, region, birth_year, user_id, current_date, "", current_retry_count + 1]
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
    if not user_message.startswith("/") and any(word in user_message for word in ["확인", "확인했습니다", "확인완료"]):
        if not is_last_joined_user(source_id, user_id):
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
@handler.add(MemberJoinedEvent)
def handle_member_joined(event):
    source_id = getattr(event.source, 'group_id', getattr(event.source, 'room_id', None))
    if not source_id:
        return

    joined_members = event.joined.members
    for member in joined_members:
        user_id = member.user_id
        if not user_id:
            continue

        is_known = False
        if supabase:
            res = supabase.table('user_validations').select('user_id').eq('user_id', user_id).execute()
            if res.data:
                is_known = True

        set_room_state(source_id, {
            "user_id": user_id,
            "status": "joined",
            "is_known": is_known
        }, ttl=3600)

    welcome_message = search_keyword("1") or search_keyword("1번")
    if not welcome_message:
        welcome_message = "👋 환영합니다! (DB에 '1'번 키워드 멘트가 없으니 등록해 주세요.)"

    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text=welcome_message)]
                )
            )
    except Exception as e:
        print(f"신입 안내 메시지 전송 실패: {e}")


# ==========================================
# [핸들러 3] 방 퇴장 이벤트 처리 핸들러
# ==========================================
@handler.add(MemberLeftEvent)
def handle_member_left(event):
    source_id = getattr(event.source, 'group_id', getattr(event.source, 'room_id', None))
    if not source_id:
        return

    left_user_ids = {m.user_id for m in event.left.members if getattr(m, 'user_id', None)}

    room_state = get_room_state(source_id)
    tracked_user_id = room_state.get('user_id') if room_state else None

    if tracked_user_id and tracked_user_id in left_user_ids:
        # 인증 진행 중이던 신입이 실제로 나간 경우 -> user_validations 상태까지 함께 초기화
        reset_verification_state(source_id, tracked_user_id)
    elif not left_user_ids or tracked_user_id is None:
        # 누가 나갔는지 알 수 없거나, 추적 중인 인증 대상이 없는 경우에는 기존처럼 room_state만 정리
        del_room_state(source_id)
    # else: 인증과 무관한 다른 멤버가 나간 경우 -> 진행 중인 신입의 room_state를 건드리지 않음


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
    if supabase:
        res = supabase.table('user_validations').select('status').eq('user_id', user_id).execute()
        if res.data and res.data[0].get('status') == '음성대기':
            is_audio_waiting = True

    if is_audio_waiting:
        if supabase:
            supabase.table('user_validations').update({"status": "승인대기"}).eq('user_id', user_id).execute()

        with sheet_sync_lock():
            try:
                if validation_sheet:
                    raw_user_ids = validation_sheet.col_values(5)
                    clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                    if user_id in clean_user_ids:
                        row_index = clean_user_ids.index(user_id) + 1
                        validation_sheet.update(range_name=f'L{row_index}', values=[["승인대기"]])
            except Exception as e:
                print(f"음성 제출 시트 업데이트 에러: {e}")

        reply_text = "🎤 음성인증 파일이 정상적으로 접수되었습니다!\n\n운영진이 확인 후 최종 승인 처리해 드릴 예정이니 잠시만 기다려 주세요."
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))


if __name__ == "__main__":
    app.run(port=5000)

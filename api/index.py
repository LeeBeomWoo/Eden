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
json_key_dict = json.loads(json_key_str)
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key_dict, scope)
client = gspread.authorize(creds)

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

# 스레드 락 (동일 인스턴스 내 동시 접근 제어)
local_sheet_lock = threading.Lock()

# 구글 스프레드시트 연결 (관리자 대시보드 및 동기화용)
sheet = client.open("인증멘트").worksheet("멘트")
validation_sheet = client.open("인증멘트").worksheet("검증")
try:
    room_manage_sheet = client.open("인증멘트").worksheet("방관리")
except Exception as e:
    print(f"⚠️ '방관리' 시트를 찾을 수 없습니다: {e}")
    room_manage_sheet = None


# ==========================================
# [동시성 제어 - Supabase 분산 락 컨텍스트 매니저]
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
                except:
                    pass


# ==========================================
# [Supabase 유저/방 세션 관리 함수]
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


# ==========================================
# [DB 전용 조회 함수 (구글 시트 Direct 조회 대신 DB 사용)]
# ==========================================
def get_room_id_by_name(room_name):
    """DB에서 방 이름으로 방 ID를 조회합니다."""
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
    """DB에서 녹음 안내 멘트 목록을 가져옵니다."""
    if supabase:
        try:
            males = supabase.table('recording_ments').select('ment').eq('gender', 'male').execute()
            females = supabase.table('recording_ments').select('ment').eq('gender', 'female').execute()
            col_male = [row['ment'] for row in males.data] if males.data else []
            col_female = [row['ment'] for row in females.data] if females.data else []
            return col_male, col_female
        except Exception as e:
            print(f"녹음 멘트 DB 조회 실패: {e}")
    return [], []

def search_keyword(keyword):
    """DB에서 인증 키워드에 해당하는 출력 멘트를 가져옵니다."""
    if supabase:
        try:
            res = supabase.table('auth_ments').select('reply_text').eq('keyword', keyword).execute()
            if res.data:
                return res.data[0]['reply_text']
        except Exception as e:
            print(f"키워드 DB 검색 오류: {e}")
    return None

def get_all_keywords():
    """DB에 등록된 모든 인증 키워드 목록을 가져옵니다."""
    if supabase:
        try:
            res = supabase.table('auth_ments').select('keyword').execute()
            if res.data:
                return [row['keyword'] for row in res.data]
        except Exception as e:
            print(f"전체 키워드 DB 조회 오류: {e}")
    return []


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

    # 0. 점(.) 입력 시 임시 저장 데이터 및 상태 초기화
    if user_message == ".":
        del_user_session(user_id)
        del_room_state(source_id)
        try:
            if supabase:
                supabase.table('user_validations').update({"status": "입장대기"}).eq('user_id', user_id).execute()
            with sheet_sync_lock():
                raw_user_ids = validation_sheet.col_values(5)
                clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                if user_id in clean_user_ids:
                    row_index = clean_user_ids.index(user_id) + 1
                    validation_sheet.update(range_name=f'K{row_index}:L{row_index}', values=[["", ""]])
        except Exception as e:
            print(f"초기화 처리 중 에러: {e}")
            
        reply_text = "🔄 임시 저장된 데이터와 인증 진행 상태가 초기화되었습니다.\n신입 인증 양식을 처음부터 다시 작성해 주세요!"
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
        return

    # 3. 슬래시(/) 명령어 로직 (유저용 인증 멘트 조회)
    if user_message.startswith("/"):
        command = user_message[1:].strip()
        parts = command.split(maxsplit=1)
        cmd_prefix = parts[0]

        if cmd_prefix in ("인증", "ㅇㅈ"):
            if len(parts) < 2 or not parts[1].strip():
                reply_text = "사용법: /인증 [키워드] 형태로 입력해 주세요."
            else:
                keyword = parts[1].strip()
                try:
                    res = supabase.table('auth_ments').select('reply_text').eq('keyword', keyword).execute()
                    if res.data and len(res.data) > 0:
                        reply_text = res.data[0]['reply_text']
                    else:
                        reply_text = f"'{keyword}'에 해당하는 인증 멘트를 찾을 수 없습니다."
                except Exception as e:
                    reply_text = f"DB 조회 오류: {e}"

            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(
                        reply_token=event.reply_token,
                        messages=[TextMessage(text=reply_text)]
                    )
                )
            return
    # [관리자 전용 명령어 (반드시 '/'로 시작해야 함)]
    if user_message.startswith("/") and source_id == ADMIN_GROUP_CHAT_ID:
        command_body = user_message[1:].strip()

        # 1) /디비업데이트 명령어 (4개 구글 시트를 Supabase DB로 일괄 동기화)
        if "디비업데이트" in command_body:
            try:
                sync_reports = []

                # A. '멘트' 시트 동기화
                ments_data = sheet.get_all_records()
                ments_records = []
                for row in ments_data:
                    k_raw = str(row.get('인증', '')).strip()
                    v_text = str(row.get('출력', '')).strip()
                    if k_raw and v_text:
                        for k in [x.strip() for x in k_raw.split(',') if x.strip()]:
                            ments_records.append({"keyword": k, "reply_text": v_text})
                
                # A. '멘트' 시트 동기화 부분
                if ments_records and supabase:
                    supabase.table('auth_ments').delete().neq('keyword', '_DELETE_ALL_KEY_').execute()
                    supabase.table('auth_ments').insert(ments_records).execute()
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
                        supabase.table('room_management').insert(room_records).execute()
                        sync_reports.append(f"• 방관리: {len(room_records)}개 방 목록")
                                    # C. '녹음' 시트 동기화
                try:
                    rec_sheet = client.open("인증멘트").worksheet("녹음")
                    males = [c.strip() for c in rec_sheet.col_values(1)[1:] if c and c.strip()]
                    females = [c.strip() for c in rec_sheet.col_values(2)[1:] if c and c.strip()]
                    rec_records = [{"gender": "male", "ment": m} for m in males] + [{"gender": "female", "ment": f} for f in females]
                    
                    if rec_records and supabase:
                        supabase.table('recording_ments').delete().gte('id', 0).execute()
                        supabase.table('recording_ments').insert(rec_records).execute()
                        sync_reports.append(f"• 녹음멘트: 남성({len(males)}) / 여성({len(females)})")
                except Exception as e:
                    print(f"녹음 시트 동기화 패스: {e}")

                # D. '검증' 시트 동기화
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
                        supabase.table('user_validations').upsert(val_records).execute()
                        sync_reports.append(f"• 검증이력: {len(val_records)}명 유저 데이터")

                report_str = "\n".join(sync_reports)
                reply_text = f"✅ 모든 구글 시트 데이터가 DB에 동기화되었습니다!\n\n{report_str}"
            except Exception as e:
                reply_text = f"❌ DB 업데이트 중 오류 발생: {e}"

            if reply_text:
                # LINE 메시지 5,000자 초과 오류 방지
                safe_reply_text = reply_text[:4900] if len(reply_text) > 4900 else reply_text
                
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(
                        ReplyMessageRequest(
                            reply_token=event.reply_token, 
                            messages=[TextMessage(text=safe_reply_text)]
                        )
                    )
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

        # 3) /방목록갱신 명령어 (안내용)
        elif command_body == "방목록갱신":
            reply_text = "💡 이제 방 목록을 포함한 모든 정보는 '/디비업데이트' 명령어를 통해 DB로 즉시 동기화됩니다."
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
            return

    # 1. 신입 인증 양식 제출 처리 (일반 텍스트 입력)
    if all(k in user_message for k in ["닉네임", "년생", "성별", "지역"]):
        extracted_data = {}
        for line in user_message.split("\n"):
            delimiter = ":" if ":" in line else ("：" if "：" in line else None)
            if delimiter:
                parts = line.split(delimiter, 1)
                key_name = parts[0].replace("-", "").strip()
                if "(" in key_name:
                    key_name = key_name.split("(", 1)[0].strip()
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
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")

        save_success = False
        alert_text = None
        current_status = "입장대기"

        # A. DB에서 중복 및 블랙리스트 조회
        found_duplicates = []
        highest_alert_level = 0
        alert_status_text = ""
        color_emoji = ""

        if supabase:
            try:
                val_res = supabase.table('user_validations').select('*').execute()
                all_val_data = val_res.data if val_res.data else []

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
                        if is_id_matched:
                            match_reasons.append("고유ID 일치")
                        if is_name_matched:
                            match_reasons.append("닉네임 일치")

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
        
        if supabase:
            try:
                user_res = supabase.table('user_validations').select('retry_count, status').eq('user_id', user_id).execute()
                retry_cnt = 1
                if user_res.data:
                    retry_cnt = user_res.data[0].get('retry_count', 1) + 1
                    current_status = user_res.data[0].get('status', '입장대기')

                supabase.table('user_validations').upsert({
                    "user_id": user_id,
                    "nickname": nickname,
                    "gender": gender,
                    "region": region,
                    "birth_year": birth_year,
                    "entry_date": current_date,
                    "retry_count": retry_cnt,
                    "status": current_status,
                    "details": update_data_details
                }).execute()
                save_success = True
            except Exception as e:
                print(f"Supabase 유저 저장 에러: {e}")

        # C. 구글 시트 백업 및 관리자 대시보드 업데이트
        with sheet_sync_lock():
            try:
                all_data = validation_sheet.get_all_values()
                clean_user_ids = [str(row[4]).strip() if len(row) > 4 else "" for row in all_data]
                details_list = [age, marriage, military, inviter, yadan, leave_reason, kick_reason]

                if user_id in clean_user_ids:
                    found_row_index = clean_user_ids.index(user_id) + 1
                    row_data = all_data[found_row_index - 1]
                    count_val = row_data[7] if len(row_data) >= 8 else "0"
                    current_retry_count = int(count_val) if count_val.isdigit() else 0
                    if len(row_data) >= 12 and row_data[11].strip():
                        current_status = row_data[11].strip()

                    update_data_basic = [nickname, gender, region, birth_year, user_id, current_date, "", current_retry_count + 1]
                    validation_sheet.update(range_name=f'A{found_row_index}:H{found_row_index}', values=[update_data_basic])
                    validation_sheet.update(range_name=f'K{found_row_index}:L{found_row_index}', values=[[user_id, current_status]])
                    validation_sheet.update(range_name=f'M{found_row_index}:S{found_row_index}', values=[details_list])
                else:
                    found_row_index = len(all_data) + 1
                    row_to_insert_basic = [nickname, gender, region, birth_year, user_id, current_date, "", 1]
                    validation_sheet.update(range_name=f'A{found_row_index}:H{found_row_index}', values=[row_to_insert_basic])
                    validation_sheet.update(range_name=f'K{found_row_index}:L{found_row_index}', values=[[user_id, "입장대기"]])
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
            set_user_session(user_id, {"nickname": nickname})

            if current_status == "입장대기":
                reply_text = "저희 커뮤니티 내부규정상 내부자료(앨범을 비롯 노트내용들이나 대화내용에 대해 내부인원들의 동의없이 무단 유출은 개인정보보호법에 의거하여 추후 처벌대상이 될수도 있으니 꼭 유의하여 주세요\n\n방에 불편한분이 계시면 예고없이 강퇴당할수있으니 참고바랍니다\n\n읽고 확인이라고 입력해 주세요"
            elif current_status == "음성대기":
                reply_text = "✅ 양식 수정이 반영되었습니다.\n\n이어서 진행 중이던 [음성 인증]을 마저 완료해 주세요!"
            elif current_status == "승인대기":
                reply_text = "✅ 양식 수정이 반영되었습니다.\n\n현재 관리자 승인을 대기 중이므로 잠시만 기다려주세요."
            else:
                reply_text = "✅ 양식 수정이 정상적으로 반영되었습니다."
        else:
            reply_text = "⚠️ 서버 통신 문제로 저장에 실패했습니다. 점(.)을 입력하여 처음부터 다시 시도해 주세요!"

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
        return

    # 2. 안내 확인 답변 처리 ("확인", "확인했습니다" 등)
    if not user_message.startswith("/") and any(word in user_message for word in ["확인", "확인했습니다", "확인완료"]):
        try:
            should_respond = False
            user_gender = ""
            user_nickname = ""

            # DB에서 유저 상태 확인
            if supabase:
                u_res = supabase.table('user_validations').select('*').eq('user_id', user_id).execute()
                if u_res.data:
                    u_data = u_res.data[0]
                    if u_data.get('status') == '입장대기':
                        should_respond = True
                        user_gender = u_data.get('gender', '')
                        user_nickname = u_data.get('nickname', '')

            if should_respond:
                session_info = get_user_session(user_id) or {}
                final_nickname = session_info.get("nickname") or user_nickname

                col_male, col_female = get_recording_ments()
                if user_gender in ["남", "남자"]:
                    selected_ment = random.choice(col_male) if col_male else "남성 인증 문구를 불러올 수 없습니다."
                elif user_gender in ["여", "여자"]:
                    selected_ment = random.choice(col_female) if col_female else "여성 인증 문구를 불러올 수 없습니다."
                else:
                    selected_ment = "성별 정보가 올바르지 않습니다. 양식을 다시 작성해주세요."

                reply_text = f"⭕️ 작성이 완료되었다면 음성인증을 진행합니다.\n\n키보드 상단 음성메시지를 활용해서 진행합니다.\n\n아래 문구를 정확하게 읽어주세요.\n\n\"제 닉네임은 {final_nickname}입니다. 오늘은 OO월 OO일, 초대자 ■■입니다. {selected_ment}\"\n\n조용한 곳에서 천천히 또박또박 부탁드립니다."

                # DB & 구글 시트 상태 업데이트 ('음성대기')
                if supabase:
                    supabase.table('user_validations').update({"status": "음성대기"}).eq('user_id', user_id).execute()

                with sheet_sync_lock():
                    raw_user_ids = validation_sheet.col_values(5)
                    clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                    if user_id in clean_user_ids:
                        row_index = clean_user_ids.index(user_id) + 1
                        validation_sheet.update(range_name=f'L{row_index}', values=[["음성대기"]])

                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
                return
        except Exception as e:
            print(f"확인 답변 처리 에러: {e}")

    # 3. DB 인증 키워드 자동 응답 (인증멘트 검색)
    matched_reply = search_keyword(user_message)
    if matched_reply:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=matched_reply)]))
        return

# ==========================================
# [핸들러 2] 방 입장 이벤트 처리 핸들러 (수정본)
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

    # 📌 신입 입장 시 자동으로 전송될 안내 및 양식 멘트
    welcome_message = (
        "👋 환영합니다! 아래 신입 인증 양식을 작성하여 전송해 주세요.\n\n"
        "닉네임 : \n"
        "년생 : \n"
        "나이 : \n"
        "성별 : \n"
        "지역 : \n"
        "결혼유무 : \n"
        "군필여부 : \n"
        "초대자 : \n"
        "야단라경험유무 : \n"
        "기존 다른방에서 나온이유 : \n"
        "다른 방에서 킥을 당한적 있는지 : \n\n"
        "⚠️ 위 양식을 복사하여 빠짐없이 입력 후 전송 부탁드립니다!"
    )

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
    if source_id:
        del_room_state(source_id)


# ==========================================
# [핸들러 4] 음성 메시지 처리 핸들러 (녹음 인증)
# ==========================================
@handler.add(MessageEvent, message=AudioMessageContent)
def handle_audio(event):
    user_id = event.source.user_id
    if not user_id:
        return

    # DB 상태 확인
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
                raw_user_ids = validation_sheet.col_values(5)
                clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                if user_id in clean_user_ids:
                    row_index = clean_user_ids.index(user_id) + 1
                    validation_sheet.update(range_name=f'L{row_index}', values=[["승인대기"]])
            except Exception as e:
                print(f"음성 제출 시트 업데이트 에러: {e}")

        reply_text = "🎤 음성인증 파일이 접수되었습니다!\n\n운영진이 확인 후 최종 승인 처리해 드릴 예정이니 잠시만 기다려 주세요."
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))


if __name__ == "__main__":
    app.run(port=5000)

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

# Upstash Redis 라이브러리
from upstash_redis import Redis

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

# [Upstash Redis 클라이언트 초기화]
redis_url = os.environ.get("UPSTASH_REDIS_REST_URL")
redis_token = os.environ.get("UPSTASH_REDIS_REST_TOKEN")

if redis_url and redis_token:
    redis = Redis(url=redis_url, token=redis_token)
else:
    redis = None
    print("⚠️ Upstash Redis 환경변수가 설정되지 않아 캐시 및 분산 락 없이 동작합니다.")

# 메모리 폴백(Fallback) 변수
notified_users = {}
room_states_memory = {} # 방 상태 메모리 폴백

# 동일 프로세스 내 동시 접근 방지용 스레드 락
local_sheet_lock = threading.Lock()

# 구글 스프레드시트 연결
sheet = client.open("인증멘트").worksheet("멘트")
validation_sheet = client.open("인증멘트").worksheet("검증")
try:
    room_manage_sheet = client.open("인증멘트").worksheet("방관리")
except Exception as e:
    print(f"⚠️ '방관리' 시트를 찾을 수 없습니다: {e}")
    room_manage_sheet = None


# ==========================================
# [동시성 제어 - 분산 락 컨텍스트 매니저]
# ==========================================
@contextmanager
def sheet_sync_lock(timeout=10, wait_time=7):
    acquired = False
    with local_sheet_lock:
        if redis:
            end_time = time.time() + wait_time
            while time.time() < end_time:
                try:
                    res = redis.set("lock:sheet_write", "LOCKED", nx=True, ex=timeout)
                    if res:
                        acquired = True
                        break
                except Exception as e:
                    break
                time.sleep(0.25)
        try:
            yield
        finally:
            if acquired and redis:
                try:
                    redis.delete("lock:sheet_write")
                except:
                    pass

# ==========================================
# [Redis 유저/방 세션 관리 함수]
# ==========================================
def set_user_session(user_id, data, ttl=3600):
    if redis:
        try:
            redis.set(f"session:user:{user_id}", json.dumps(data), ex=ttl)
            return
        except: pass
    notified_users[user_id] = data

def get_user_session(user_id):
    if redis:
        try:
            val = redis.get(f"session:user:{user_id}")
            if val: return json.loads(val)
        except: pass
    return notified_users.get(user_id)

def del_user_session(user_id):
    if redis:
        try: redis.delete(f"session:user:{user_id}")
        except: pass
    notified_users.pop(user_id, None)

def set_room_state(room_id, data, ttl=3600):
    """특정 방(인증방)에 입장한 유저의 상태와 리포트를 캐시(Redis)에 저장"""
    if redis:
        try:
            redis.set(f"room_state:{room_id}", json.dumps(data), ex=ttl)
            return
        except: pass
    room_states_memory[room_id] = data

def get_room_state(room_id):
    if redis:
        try:
            val = redis.get(f"room_state:{room_id}")
            if val: return json.loads(val)
        except: pass
    return room_states_memory.get(room_id)

def del_room_state(room_id):
    if redis:
        try: redis.delete(f"room_state:{room_id}")
        except: pass
    room_states_memory.pop(room_id, None)

def get_room_id_by_name(room_name):
    """'방관리' 시트에서 '1번방' 등의 이름을 검색해 해당 방의 ID를 반환"""
    if not room_manage_sheet: return None
    cache_key = "cache:room_management"
    data = None
    if redis:
        try:
            val = redis.get(cache_key)
            if val: data = json.loads(val)
        except: pass
    
    if not data:
        try:
            data = room_manage_sheet.get_all_records()
            if redis and data:
                redis.set(cache_key, json.dumps(data), ex=3600) # 1시간 캐시
        except Exception as e:
            print(f"방관리 시트 로드 실패: {e}")
            return None
            
    # 시트의 A열(키) 기반 검색
    target_name = room_name.replace(" ", "")
    for row in data:
        keys = list(row.keys())
        if len(keys) >= 2:
            sheet_room_name = str(row[keys[0]]).replace(" ", "")
            if sheet_room_name == target_name:
                return str(row[keys[1]]).strip()
    return None

# ==========================================
# [기존 캐시 함수들]
# ==========================================
def get_recording_ments():
    cache_key = "cache:recording_ments"
    if redis:
        try:
            cached_val = redis.get(cache_key)
            if cached_val:
                data = json.loads(cached_val)
                return data.get("male", []), data.get("female", [])
        except: pass
    try:
        recording_sheet = client.open("인증멘트").worksheet("녹음")
        col_male = [cell for cell in recording_sheet.col_values(1)[1:] if cell and cell.strip()]
        col_female = [cell for cell in recording_sheet.col_values(2)[1:] if cell and cell.strip()]
        if redis:
            try: redis.set(cache_key, json.dumps({"male": col_male, "female": col_female}), ex=3600)
            except: pass
        return col_male, col_female
    except: return [], []

def search_keyword(keyword):
    cache_key = "cache:sheet_ments_records"
    data = None
    if redis:
        try:
            cached_val = redis.get(cache_key)
            if cached_val: data = json.loads(cached_val)
        except: pass
    if not data:
        try:
            data = sheet.get_all_records()
            if redis and data: redis.set(cache_key, json.dumps(data), ex=600)
        except: return None
    for row in data:
        cell_value = str(row.get('인증', '')).strip()
        keywords_in_cell = [k.strip() for k in cell_value.split(',') if k.strip()]
        if keyword in keywords_in_cell:
            return row.get('출력')
    return None

def get_all_keywords():
    cache_key = "cache:all_keywords"
    if redis:
        try:
            cached_val = redis.get(cache_key)
            if cached_val: return json.loads(cached_val)
        except: pass
    try:
        values = sheet.col_values(1)
        keywords = [str(val).strip() for val in values[1:] if str(val).strip()]
        if redis and keywords: redis.set(cache_key, json.dumps(keywords), ex=600)
        return keywords
    except: return []

@app.route("/api", methods=['POST'])
def callback():
    signature = request.headers.get('X-Line-Signature')
    body = request.get_data(as_text=True)
    try: handler.handle(body, signature)
    except InvalidSignatureError: abort(400)
    return 'OK'

# ==========================================
# [핸들러 1] 일반 유저 메시지 처리 핸들러
# ==========================================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id  
    if not user_id: return 
    
    # 메시지가 작성된 방/그룹의 ID 추출
    source_id = getattr(event.source, 'group_id', getattr(event.source, 'room_id', event.source.user_id))
    user_message = event.message.text.strip()
    reply_text = ""

    # 0. 점(.)만 입력된 경우 임시 저장 데이터 및 인증 상태 초기화
    if user_message == ".":
        del_user_session(user_id)
        try:
            with sheet_sync_lock():
                raw_user_ids = validation_sheet.col_values(5)
                clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                if user_id in clean_user_ids:
                    row_index = clean_user_ids.index(user_id) + 1
                    validation_sheet.update(range_name=f'K{row_index}:L{row_index}', values=[["", ""]])
        except: pass
        
        reply_text = "🔄 임시 저장된 데이터와 인증 진행 상태가 초기화되었습니다.\n신입 인증 양식을 처음부터 다시 작성해 주세요!"
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
        return

    # [NEW] 0-1. 관리자방 명령어: O번방 확인
    if "확인" in user_message and source_id == ADMIN_GROUP_CHAT_ID:
        # 예: "1번방 확인", "1번 방 확인" 등을 파싱
        room_name_input = user_message.replace("확인", "").strip()
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
                    # 양식 제출 시 생성해둔 중복/블랙 알림 리포트를 그대로 답장
                    alert_report = room_state.get('report', f"[{room_name_input}] 양식이 접수되었습니다.")
                    reply_text = alert_report
        else:
            if room_manage_sheet:
                reply_text = f"❌ '{room_name_input}' 정보를 '방관리' 시트에서 찾을 수 없습니다."
            
        if reply_text:
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
        return


    # 1. 그룹/룸 고유 아이디 확인 명령어
    if user_message == "/여긴어디?":
        if hasattr(event.source, 'group_id'):
            reply_text = f"📍 현재 계신 곳은 [그룹방]입니다.\n🆔 Group ID:\n{event.source.group_id}"
        elif hasattr(event.source, 'room_id'):
            reply_text = f"📍 현재 계신 곳은 [멀티 대화방]입니다.\n🆔 Room ID:\n{event.source.room_id}"
        else:
            reply_text = f"👤 현재 계신 곳은 [1:1 채팅방]입니다.\n🆔 User ID:\n{event.source.user_id}"
            
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
        return

    # 2. 신입 인증 양식 제출 처리
    if all(k in user_message for k in ["닉네임", "년생", "성별", "지역"]):
        extracted_data = {}
        for line in user_message.split("\n"):
            delimiter = ":" if ":" in line else ("：" if "：" in line else None)
            if delimiter:
                parts = line.split(delimiter, 1)
                key_name = parts[0].replace("-", "").strip()
                if "(" in key_name: key_name = key_name.split("(", 1)[0].strip()
                extracted_data[key_name] = parts[1].strip()

        required_fields = ["닉네임", "년생", "나이", "성별", "지역", "결혼유무", "군필여부", "초대자", "야단라경험유무", "기존 다른방에서 나온이유", "다른 방에서 킥을 당한적 있는지"]
        missing_fields = []
        user_gender = extracted_data.get("성별", "").strip()
        
        for req_field in required_fields:
            val = extracted_data.get(req_field, "").strip()
            if not val:
                if req_field == "군필여부" and user_gender in ["여", "여자"]: continue
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

        with sheet_sync_lock():
            try:
                all_data = validation_sheet.get_all_values()
                found_duplicates = []
                highest_alert_level = 0
                alert_status_text = ""
                color_emoji = ""

                if all_data and len(all_data) > 1:
                    headers = all_data[0]
                    idx_id = headers.index("아이디") if "아이디" in headers else 4
                    idx_name = headers.index("닉네임") if "닉네임" in headers else 0
                    idx_year = headers.index("년생") if "년생" in headers else 3
                    idx_gender = headers.index("성별") if "성별" in headers else 1
                    idx_region = headers.index("사는지역") if "사는지역" in headers else 2
                    idx_black = headers.index("블랙사유") if "블랙사유" in headers else 6

                    for idx, row in enumerate(all_data[1:]):
                        sheet_row_num = idx + 2 
                        rec_id = str(row[idx_id]).strip() if idx_id < len(row) else ""
                        rec_name = str(row[idx_name]).strip() if idx_name < len(row) else ""
                        rec_year = str(row[idx_year]).strip() if idx_year < len(row) else ""
                        rec_gender = str(row[idx_gender]).strip() if idx_gender < len(row) else ""
                        rec_region = str(row[idx_region]).strip() if idx_region < len(row) else ""
                        rec_black = str(row[idx_black]).strip() if idx_black < len(row) else ""
                        
                        is_id_matched = (rec_id == str(user_id).strip() and rec_id != "")
                        is_name_matched = (rec_name == nickname)

                        if is_id_matched or is_name_matched:
                            match_reasons = []
                            if is_id_matched: match_reasons.append("고유ID 일치")
                            if is_name_matched: match_reasons.append("닉네임 일치")

                            match_score = 0
                            if rec_year == birth_year: match_score += 1
                            if rec_gender == gender: match_score += 1
                            if rec_region == region: match_score += 1

                            row_info = f"📍 [시트 {sheet_row_num}행] ({', '.join(match_reasons)})\n - 기존정보: {rec_name} / {rec_year}년생 / {rec_gender} / {rec_region}"
                            if rec_black: row_info += f"\n - 💀 블랙사유: {rec_black}"
                            found_duplicates.append(row_info)

                            current_level = 0
                            if is_name_matched:
                                if match_score == 3: current_level = 4
                                elif match_score > 0: current_level = 2
                                else: current_level = 1
                            if is_id_matched:
                                if current_level < 3: current_level = 3
                            if rec_black: current_level = 5

                            if current_level > highest_alert_level:
                                highest_alert_level = current_level
                                if current_level == 5: alert_status_text, color_emoji = "💀 [위험] 블랙리스트 유저 감지", "⚫"
                                elif current_level == 4: alert_status_text, color_emoji = "🚨 [적색 경고] 닉네임 및 모든 정보 일치", "🔴"
                                elif current_level == 3: alert_status_text, color_emoji = "🔄 [주의] 재입장 유저 (동일 ID 확인)", "🟪"
                                elif current_level == 2: alert_status_text, color_emoji = "⚠️ [황색 경고] 닉네임 및 정보 일부 일치", "🟡"
                                elif current_level == 1: alert_status_text, color_emoji = "🔵 [주의] 닉네임 일치 유저", "🟦"

                if highest_alert_level > 0:
                    dup_details_str = "\n\n".join(found_duplicates)
                    # [NEW] Push 메시지용이 아닌, Redis 저장용 리포트 생성
                    alert_text = (
                        f"{color_emoji} 신입 양식 작성 중복/블랙 필터링 결과\n\n"
                        f"📌 상태: {alert_status_text}\n"
                        f"👤 신규입력: {nickname} ({birth_year}년생) / {region} / {gender}\n\n"
                        f"📑 [중복/기존 내역 상세]\n{dup_details_str}\n\n"
                        f"💡 관리자분들께서는 위 상세 내역을 기반으로 승인 여부를 검토하시기 바랍니다."
                    )

                clean_user_ids = [str(row[4]).strip() if len(row) > 4 else "" for row in all_data]
                update_data_details = [age, marriage, military, inviter, yadan, leave_reason, kick_reason]

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
                    validation_sheet.update(range_name=f'M{found_row_index}:S{found_row_index}', values=[update_data_details])
                else:
                    found_row_index = len(all_data) + 1 
                    row_to_insert_basic = [nickname, gender, region, birth_year, user_id, current_date, "", 1]
                    validation_sheet.update(range_name=f'A{found_row_index}:H{found_row_index}', values=[row_to_insert_basic])
                    validation_sheet.update(range_name=f'K{found_row_index}:L{found_row_index}', values=[[user_id, "입장대기"]])
                    validation_sheet.update(range_name=f'M{found_row_index}:S{found_row_index}', values=[update_data_details])

                save_success = True
            except Exception as sheet_err:
                save_success = False
                print(f"🚨 구글 시트 저장 처리 에러:\n{sheet_err}")

        # [NEW] 푸시 발송 대신 캐시에 유저의 상태(양식 제출 및 리포트)를 저장해둡니다.
        if save_success:
            if alert_text:
                state_data = {
                    "user_id": user_id,
                    "status": "form_submitted",
                    "report": alert_text
                }
            else:
                state_data = {
                    "user_id": user_id,
                    "status": "form_submitted",
                    "report": "✅ 해당 유저는 중복/블랙 이력이 없는 깨끗한 신규 회원입니다.\n양식이 정상 접수되었습니다."
                }
            # 현재 양식을 제출한 방의 ID 기준으로 상태 저장 (관리자가 조회할 수 있도록)
            set_room_state(source_id, state_data, ttl=7200) # 2시간 보관
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

        # [NEW] 방 목록 캐시 즉시 갱신 명령어
    if user_message in ["방목록갱신", "/방목록갱신"] and source_id == ADMIN_GROUP_CHAT_ID:
        if redis:
            try:
                redis.delete("cache:room_management")
            except: pass
        reply_text = "🔄 방 관리 목록 캐시가 갱신되었습니다. 이제 새로 추가된 방을 즉시 인식할 수 있습니다!"
        
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
        return

    # 3. 안내 확인 답변 처리
    if not user_message.startswith("/") and any(word in user_message for word in ["확인", "확인했습니다", "확인완료"]):
        try:
            should_respond = False
            reply_text = ""
            with sheet_sync_lock():
                raw_user_ids = validation_sheet.col_values(5)
                clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                if user_id in clean_user_ids:
                    row_index = clean_user_ids.index(user_id) + 1
                    row_data = validation_sheet.row_values(row_index)
                    current_status = row_data[11].strip() if len(row_data) > 11 else ""
                    
                    if current_status == "입장대기":
                        should_respond = True
                        sheet_nickname = row_data[0].strip() if len(row_data) > 0 else ""
                        user_gender = row_data[1].strip() if len(row_data) > 1 else ""
                        session_info = get_user_session(user_id) or {}
                        user_nickname = session_info.get("nickname", sheet_nickname)
                        
                        col_male, col_female = get_recording_ments()
                        if user_gender in ["남", "남자"]: selected_ment = random.choice(col_male) if col_male else "남성 인증 문구를 불러올 수 없습니다."
                        elif user_gender in ["여", "여자"]: selected_ment = random.choice(col_female) if col_female else "여성 인증 문구를 불러올 수 없습니다."
                        else: selected_ment = "성별 정보가 올바르지 않습니다. 양식을 다시 작성해주세요."

                        reply_text = f"⭕️ 작성이 완료되었다면 음성인증을 진행합니다.\n\n키보드 상단 음성메시지를 활용해서 진행합니다.\n\n아래 문구를 정확하게 읽어주세요.\n\n\"제 닉네임은 {user_nickname}입니다. 오늘은 OO월 OO일, 초대자 ■■입니다. {selected_ment}\"\n\n조용한 곳에서 천천히 또박또박 부탁드립니다."
                        validation_sheet.update(range_name=f'L{row_index}', values=[['음성대기']])

            if should_respond and reply_text:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
            return
        except Exception as e:
            pass
        return


    # 4. 슬래시(/) 명령어 로직 (기존과 동일)
    if not user_message.startswith("/"): return
    command = user_message[1:].strip()
    if command.startswith(("인증 ", "ㅇㅈ ")):
        search_query = command[3:].strip() if command.startswith("인증 ") else command[2:].strip()
        if search_query in ["1단계", "양식", "양식안내"]:
            reply_text = "안녕하세요\n𝔼·𝔻 ꕤ 𝔼·ℕ 신입 인증방에\n오신것을 환영합니다\n\n⭕️아래의 본문을 복사해서 빠.짐.없.이. 작성해주세요.\n\n - 닉네임(두글자):\n - 년생:\n - 나이: (만나이 ❌️):\n - 성별(남/여/남자/여자 중 하나만 입력):\n - 지역(시까지, 단 서울 및 광역시는 구까지):\n - 결혼유무(기/미/돌):\n - 군필여부(남자만):\n - 초대자:\n - 야단라경험유무(양식 - 방 이름 및 임티, 기존에 썻던 닉, 에덴 입장 경험):\n - 기존 다른방에서 나온이유(없다면 무) :\n - 다른 방에서 킥을 당한적 있는지(있다면 사유도) :"
        elif search_query in ["2단계", "주의사항", "규정"]:
            reply_text = "저희 커뮤니티 내부규정상 내부자료(앨범을 비롯 노트내용들이나 대화내용에 대해 내부인원들의 동의없이 무단 유출은 개인정보보호법에 의거하여 추후 처벌대상이 될수도 있으니 꼭 유의하여 주세요\n\n방에 불편한분이 계시면 예고없이 강퇴당할수있으니 참고바랍니다\n\n읽고 확인이라고 입력해 주세요"
        elif search_query in ["3단계", "음성", "음성안내"]:
            session_info = get_user_session(user_id) or {}
            user_nickname = session_info.get("nickname", "[본인닉네임]")
            col_male, col_female = get_recording_ments()
            all_ments = col_male + col_female
            random_ment = random.choice(all_ments) if all_ments else "인증 문구를 불러올 수 없습니다."
            reply_text = f"⭕️ 작성이 완료되었다면 음성인증을 진행합니다.\n\n키보드 상단 음성메시지를 활용해서 진행합니다.\n\n아래 문구를 정확하게 읽어주세요.\n\n\"제 닉네임은 {user_nickname}입니다. 오늘은 OO월 OO일, 초대자 ■■입니다. {random_ment}\"\n\n조용한 곳에서 천천히 또박또박 부탁드립니다."
        else:
            result = search_keyword(search_query)
            reply_text = result if result else f"😢 '{search_query}' 미 입력된 인증멘트. 오타에 주의해주세요!"
    elif command == "목록":
        keywords = get_all_keywords()
        list_text = "📍 [단계별 고정 명령어]\n- /ㅇㅈ 1단계 (또는 양식)\n- /ㅇㅈ 2단계 (또는 주의사항)\n- /ㅇㅈ 3단계 (또는 음성)\n\n📋 [시트 등록 인증멘트]\n"
        reply_text = list_text + "\n".join(f"- {k}" for k in keywords) if keywords else list_text + "📭 현재 등록된 인증 멘트가 없습니다."
    elif command in ["id", "내정보", "아이디"]:
        reply_text = f"👤 당신의 LINE User ID:\n{user_id}\n\n위 ID를 복사하여 관리자에게 전달해 주세요!"
    else:
        reply_text = f"명령어 확인. '{command}'이런 명령어는 없습니다. 😢"

    if reply_text:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))

# ==========================================
# [핸들러 2] 유저 입장 시 처리 핸들러 (블랙리스트 즉시 캐싱)
# ==========================================
@handler.add(MemberJoinedEvent)
def handle_member_joined(event):
    source_id = getattr(event.source, 'group_id', getattr(event.source, 'room_id', None))
    
    # [NEW] 유저가 입장하자마자 구글 시트에서 방문 이력이 있는지 빠르게 검사합니다.
    try:
        joined_member_id = event.joined.members[0].user_id
        is_known_user = False
        with sheet_sync_lock():
            raw_user_ids = validation_sheet.col_values(5)
            clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
            if joined_member_id in clean_user_ids:
                is_known_user = True
                
        if source_id:
            state_data = {
                "user_id": joined_member_id,
                "status": "joined",
                "is_known": is_known_user
            }
            set_room_state(source_id, state_data, ttl=7200)
    except Exception as e:
        print(f"입장 유저 캐싱 에러: {e}")

    welcome_text = "안녕하세요\n𝔼·ℕ ꕤ 𝔼·ℕ 신입 인증방에\n오신것을 환영합니다\n\n⭕️아래의 본문을 복사해서 빠.짐.없.이. 작성해주세요.\n\n - 닉네임(두글자):\n - 년생:\n - 나이: (만나이 ❌️):\n - 성별(남/여/남자/여자 중 하나만 입력):\n - 지역(시까지, 단 서울 및 광역시는 구까지):\n - 결혼유무(기/미/돌):\n - 군필여부(남자만):\n - 초대자:\n - 야단라경험유무(방 이름 및 임티, 기존에 썻던 닉):\n - 기존 다른방에서 나온이유(없다면 무) :\n - 다른 방에서 킥을 당한적 있는지(있다면 사유도) :"
    
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=welcome_text)]))
    except: pass


# ==========================================
# [핸들러 3] 유저 퇴장 시 처리 핸들러
# ==========================================
@handler.add(MemberLeftEvent)
def handle_member_left(event):
    source_id = getattr(event.source, 'group_id', getattr(event.source, 'room_id', None))
    try:
        left_members = event.left.members
        for member in left_members:
            user_id = member.user_id
            if not user_id: continue

            # 1. 임시 세션 및 방 상태 삭제
            del_user_session(user_id)
            if source_id: del_room_state(source_id) # 방 캐시 초기화

            # 2. 구글 시트 초기화
            with sheet_sync_lock():
                raw_user_ids = validation_sheet.col_values(5)
                clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                if user_id in clean_user_ids:
                    row_index = clean_user_ids.index(user_id) + 1
                    validation_sheet.update(range_name=f'K{row_index}:L{row_index}', values=[["", ""]])
    except: pass


# ==========================================
# [핸들러 4] 음성 메시지(음성 인증) 처리 핸들러
# ==========================================
@handler.add(MessageEvent, message=AudioMessageContent)
def handle_audio_message(event):
    user_id = event.source.user_id
    if not user_id: return

    # [주의] 음성 인증 완료 알람은 여전히 Push를 사용하도록 남겨두었습니다. 
    # (빈도가 낮을 것으로 예상되어 남겼으나, 이것도 요금 문제를 일으킨다면 나중에 명령어 방식으로 바꿀 수 있습니다.)
    try:
        should_alert = False
        nickname = "알수없음"
        with sheet_sync_lock():
            raw_user_ids = validation_sheet.col_values(5)
            clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
            if user_id in clean_user_ids:
                row_index = clean_user_ids.index(user_id) + 1
                row_data = validation_sheet.row_values(row_index)
                current_status = row_data[11].strip() if len(row_data) >= 12 else ""
                nickname = row_data[0].strip() if len(row_data) > 0 else "알수없음"

                if current_status == "음성대기":
                    validation_sheet.update(range_name=f'L{row_index}', values=[['승인대기']])
                    should_alert = True

        if should_alert:
            reply_text = "🎙️ 음성 인증 메시지가 성공적으로 제출되었습니다!\n\n인증자 확인 후 이후절차 진행 예정이니 잠시만 기다려주세요. 감사합니다! 😊"
            admin_alert_text = f"🔔 [음성 인증 제출 완료]\n\n👤 닉네임: {nickname}\n해당 신입 유저가 음성 인증을 제출하여 상태가 [승인대기]로 변경되었습니다."
            
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)]))
                
                # Push 발송 에러가 나더라도(한도 초과) 봇이 죽지 않도록 예외처리 강화
                try:
                    line_bot_api.push_message(PushMessageRequest(to=ADMIN_GROUP_CHAT_ID, messages=[TextMessage(text=admin_alert_text)]))
                except Exception as e:
                    print(f"음성 알림 Push 실패(한도 초과 예상): {e}")

    except Exception as e:
        pass

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

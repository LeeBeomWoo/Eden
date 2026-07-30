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
from linebot.v3.webhooks import MessageEvent, TextMessageContent, MemberJoinedEvent, AudioMessageContent


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

# 동일 프로세스 내 동시 접근 방지용 스레드 락
local_sheet_lock = threading.Lock()

# 구글 스프레드시트 연결
sheet = client.open("인증멘트").worksheet("멘트")
validation_sheet = client.open("인증멘트").worksheet("검증")


# ==========================================
# [동시성 제어 - 분산 락 컨텍스트 매니저]
# ==========================================
@contextmanager
def sheet_sync_lock(timeout=10, wait_time=7):
    """
    4명 이상 동시 접속 시 구글 시트 덮어쓰기 및 API 충돌을 방지하는 이중 락(Thread + Redis Lock)
    """
    acquired = False
    with local_sheet_lock:
        if redis:
            end_time = time.time() + wait_time
            while time.time() < end_time:
                try:
                    # set nx=True, ex=timeout 으로 분산 락 획득 시도
                    res = redis.set("lock:sheet_write", "LOCKED", nx=True, ex=timeout)
                    if res:
                        acquired = True
                        break
                except Exception as e:
                    print(f"Redis Lock 획득 시도 중 에러: {e}")
                    break
                time.sleep(0.25)
        
        try:
            yield
        finally:
            if acquired and redis:
                try:
                    redis.delete("lock:sheet_write")
                except Exception as e:
                    print(f"Redis Lock 해제 에러: {e}")


# ==========================================
# [Redis 유저 세션 관리 함수]
# ==========================================
def set_user_session(user_id, data, ttl=3600):
    if redis:
        try:
            redis.set(f"session:user:{user_id}", json.dumps(data), ex=ttl)
            return
        except Exception as e:
            print(f"Redis 세션 저장 실패: {e}")
    notified_users[user_id] = data


def get_user_session(user_id):
    if redis:
        try:
            val = redis.get(f"session:user:{user_id}")
            if val:
                return json.loads(val)
        except Exception as e:
            print(f"Redis 세션 읽기 실패: {e}")
    return notified_users.get(user_id)


def del_user_session(user_id):
    if redis:
        try:
            redis.delete(f"session:user:{user_id}")
        except Exception as e:
            print(f"Redis 세션 삭제 실패: {e}")
    notified_users.pop(user_id, None)


# ==========================================
# [Redis 캐시 함수 정의]
# ==========================================
def get_recording_ments():
    cache_key = "cache:recording_ments"
    if redis:
        try:
            cached_val = redis.get(cache_key)
            if cached_val:
                data = json.loads(cached_val)
                return data.get("male", []), data.get("female", [])
        except Exception as e:
            print(f"Redis 읽기 에러 (녹음): {e}")

    try:
        recording_sheet = client.open("인증멘트").worksheet("녹음")
        col_male = [cell for cell in recording_sheet.col_values(1)[1:] if cell and cell.strip()]
        col_female = [cell for cell in recording_sheet.col_values(2)[1:] if cell and cell.strip()]
        
        if redis:
            try:
                payload = json.dumps({"male": col_male, "female": col_female})
                redis.set(cache_key, payload, ex=3600)
            except Exception as e:
                print(f"Redis 저장 에러 (녹음): {e}")
                
        return col_male, col_female
    except Exception as e:
        print(f"녹음 시트 로드 에러: {e}")
        return [], []


def search_keyword(keyword):
    cache_key = "cache:sheet_ments_records"
    data = None
    if redis:
        try:
            cached_val = redis.get(cache_key)
            if cached_val:
                data = json.loads(cached_val)
        except Exception as e:
            print(f"Redis 읽기 에러 (키워드 검색): {e}")

    if not data:
        try:
            data = sheet.get_all_records()
            if redis and data:
                redis.set(cache_key, json.dumps(data), ex=600)
        except Exception as e:
            print(f"시트 로드 실패: {e}")
            return None

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
            if cached_val:
                return json.loads(cached_val)
        except Exception as e:
            print(f"Redis 읽기 에러 (목록): {e}")

    try:
        values = sheet.col_values(1)
        keywords = [str(val).strip() for val in values[1:] if str(val).strip()]
        if redis and keywords:
            redis.set(cache_key, json.dumps(keywords), ex=600)
        return keywords
    except Exception as e:
        print(f"키워드 목록 불러오기 실패: {e}")
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
# [핸들러 1] 일반 유저 메시지 처리 핸들러
# ==========================================
@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id  
    if not user_id: return 

    user_message = event.message.text.strip()
    reply_text = ""

    # 0. 점(.)만 입력된 경우 임시 저장 데이터 및 인증 상태 초기화
    if user_message == ".":
        del_user_session(user_id)
            
        try:
            with sheet_sync_lock():
                raw_user_ids = validation_sheet.col_values(5)
                clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                current_user_id = str(user_id).strip()
                
                if current_user_id in clean_user_ids:
                    row_index = clean_user_ids.index(current_user_id) + 1
                    # 배치 업데이트로 K, L열 한번에 초기화
                    validation_sheet.update(range_name=f'K{row_index}:L{row_index}', values=[["", ""]])
        except Exception as e:
            print(f"초기화 시트 에러: {e}")
            
        reply_text = (
            "🔄 임시 저장된 데이터와 인증 진행 상태가 초기화되었습니다.\n"
            "신입 인증 양식을 처음부터 다시 작성해 주세요!"
        )

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
            )
        return

    # 1. 그룹/룸 고유 아이디 확인 명령어
    if user_message == "/여긴어디?":
        source_type = event.source.type
        if source_type == "group":
            current_id = event.source.group_id
            reply_text = f"📍 현재 계신 곳은 [그룹방]입니다.\n🆔 Group ID:\n{current_id}"
        elif source_type == "room":
            current_id = event.source.room_id
            reply_text = f"📍 현재 계신 곳은 [멀티 대화방]입니다.\n🆔 Room ID:\n{current_id}"
        else:
            current_id = event.source.user_id
            reply_text = f"👤 현재 계신 곳은 [1:1 채팅방]입니다.\n🆔 User ID:\n{current_id}"
            
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
            )
        return

    # 2. 신입 인증 양식 제출 처리
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

        # 전체 필수 항목 리스트 정의 (어느 하나라도 빠지면 안 됨)
        required_fields = [
            "닉네임", "년생", "나이", "성별", "지역", 
            "결혼유무", "군필여부", "초대자", "야단라경험유무", 
            "기존 다른방에서 나온이유", "다른 방에서 킥을 당한적 있는지"
        ]

        missing_fields = []
        user_gender = extracted_data.get("성별", "").strip()
        
        for req_field in required_fields:
            val = extracted_data.get(req_field, "").strip()
            if not val:
                # [예외 처리] 여성의 경우 군필여부는 빈칸이어도 통과
                if req_field == "군필여부" and user_gender in ["여", "여자"]:
                    continue
                missing_fields.append(req_field)

        # 누락된 항목이 존재할 경우 되돌려 보냄
        if missing_fields:
            reply_text = (
                f"⚠️ 양식 작성 내용 중 다음 항목이 누락되었습니다:\n"
                f"- {', '.join(missing_fields)}\n\n"
                "해당 항목을 빠짐없이 작성 후 다시 제출해 주세요!\n\n"
                "기존양식은 건들지 말고, : ← 표시 뒤에 입력하여주세요."
            )
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
                )
            return

        # 모든 데이터 추출 및 변수화 (추가된 세부 항목 포함)
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

        # [단계 A & B] 동시성 방지 락 내부에서 읽기/검증/저장 일괄 처리
        save_success = False
        alert_text = None
        current_status = "입장대기" # 신규 유저 기본 상태

        with sheet_sync_lock():
            try:
                all_data = validation_sheet.get_all_values()
                
                # 중복 및 블랙 내역을 담을 리스트와 경고 레벨 변수
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

                        # ID가 일치하거나 닉네임이 일치하는 경우 (중복 감지)
                        if is_id_matched or is_name_matched:
                            match_reasons = []
                            if is_id_matched: match_reasons.append("고유ID 일치")
                            if is_name_matched: match_reasons.append("닉네임 일치")

                            match_score = 0
                            if rec_year == birth_year: match_score += 1
                            if rec_gender == gender: match_score += 1
                            if rec_region == region: match_score += 1

                            row_info = (
                                f"📍 [시트 {sheet_row_num}행] ({', '.join(match_reasons)})\n"
                                f" - 기존정보: {rec_name} / {rec_year}년생 / {rec_gender} / {rec_region}"
                            )
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
                                if current_level == 5:
                                    alert_status_text = "💀 [위험] 블랙리스트 유저 감지"
                                    color_emoji = "⚫"
                                elif current_level == 4:
                                    alert_status_text = "🚨 [적색 경고] 닉네임 및 모든 정보 일치"
                                    color_emoji = "🔴"
                                elif current_level == 3:
                                    alert_status_text = "🔄 [주의] 재입장 유저 (동일 ID 확인)"
                                    color_emoji = "🟪"
                                elif current_level == 2:
                                    alert_status_text = "⚠️ [황색 경고] 닉네임 및 정보 일부 일치"
                                    color_emoji = "🟡"
                                elif current_level == 1:
                                    alert_status_text = "🔵 [주의] 닉네임 일치 유저"
                                    color_emoji = "🟦"

                if highest_alert_level > 0:
                    dup_details_str = "\n\n".join(found_duplicates)
                    alert_text = (
                        f"{color_emoji} 신입 양식 작성 중복 필터링\n\n"
                        f"📌 상태: {alert_status_text}\n"
                        f"👤 신규입력: {nickname} ({birth_year}년생) / {region} / {gender}\n\n"
                        f"📑 [중복/기존 내역 상세]\n"
                        f"{dup_details_str}\n\n"
                        f"💡 관리자분들께서는 위 '시트 행 번호' 및 상세 내역을 기반으로 승인 여부를 검토하시기 바랍니다."
                    )

                clean_user_ids = [str(row[4]).strip() if len(row) > 4 else "" for row in all_data]
                current_user_id = str(user_id).strip()

                # 시트에 입력할 데이터를 그룹화
                # 1. A~H 열 (기본정보)
                # 2. M~S 열 (상세정보: 나이, 결혼, 군필, 초대자, 야단라, 나온이유, 킥사유)
                update_data_details = [age, marriage, military, inviter, yadan, leave_reason, kick_reason]

                if current_user_id in clean_user_ids:
                    # 🔄 기존 유저 덮어쓰기 (수정 제출 시)
                    found_row_index = clean_user_ids.index(current_user_id) + 1
                    row_data = all_data[found_row_index - 1]
                    count_val = row_data[7] if len(row_data) >= 8 else "0"
                    current_retry_count = int(count_val) if count_val.isdigit() else 0
                    
                    # L열(12번째) 기존 상태 보존
                    if len(row_data) >= 12:
                        existing_status = row_data[11].strip()
                        if existing_status:
                            current_status = existing_status
                            
                    update_data_basic = [nickname, gender, region, birth_year, user_id, current_date, "", current_retry_count + 1]
                    
                    # A~H, K~L, M~S 각각 덮어쓰기 진행
                    validation_sheet.update(range_name=f'A{found_row_index}:H{found_row_index}', values=[update_data_basic])
                    validation_sheet.update(range_name=f'K{found_row_index}:L{found_row_index}', values=[[user_id, current_status]])
                    validation_sheet.update(range_name=f'M{found_row_index}:S{found_row_index}', values=[update_data_details])
                else:
                    # ➕ 신규 유저 추가
                    found_row_index = len(all_data) + 1 
                    
                    row_to_insert_basic = [nickname, gender, region, birth_year, user_id, current_date, "", 1]
                    
                    validation_sheet.update(range_name=f'A{found_row_index}:H{found_row_index}', values=[row_to_insert_basic])
                    validation_sheet.update(range_name=f'K{found_row_index}:L{found_row_index}', values=[[user_id, "입장대기"]])
                    validation_sheet.update(range_name=f'M{found_row_index}:S{found_row_index}', values=[update_data_details])

                save_success = True

            except Exception as sheet_err:
                save_success = False
                error_msg = f"🚨 구글 시트 저장 처리 에러:\n{sheet_err}"
                print(error_msg)
                try:
                    with ApiClient(configuration) as api_client:
                        line_bot_api = MessagingApi(api_client)
                        line_bot_api.push_message(
                            PushMessageRequest(to=ADMIN_GROUP_CHAT_ID, messages=[TextMessage(text=error_msg)])
                       )
                except Exception as push_err:
                    print(f"에러 메시지 푸시 전송 실패: {push_err}")

        
        # 관리자 알림 전송
        if alert_text:
            try:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.push_message(
                        PushMessageRequest(to=ADMIN_GROUP_CHAT_ID, messages=[TextMessage(text=alert_text)])
                    )
            except Exception as push_err:
                print(f"관리자 푸시 알림 에러: {push_err}")

        # [단계 C] 사용자 응답
        if save_success:
            set_user_session(user_id, {"nickname": nickname})
            
            # 현재 진행 상태에 따라 안내 문구를 다르게 출력
            if current_status == "입장대기":
                reply_text = (
                    "저희 커뮤니티 내부규정상 내부자료(앨범을 비롯 노트내용들이나 대화내용에 대해 " 
                    "내부인원들의 동의없이 무단 유출은 개인정보보호법에 의거하여 추후 처벌대상이 될수도 있으니 꼭 유의하여 주세요\n\n" 
                    "방에 불편한분이 계시면 예고없이 강퇴당할수있으니 참고바랍니다\n\n" 
                    "읽고 확인이라고 입력해 주세요"
                )
            elif current_status == "음성대기":
                reply_text = (
                    "✅ 양식 수정이 반영되었습니다.\n\n"
                    "이어서 진행 중이던 [음성 인증]을 마저 완료해 주세요!"
                )
            elif current_status == "승인대기":
                reply_text = (
                    "✅ 양식 수정이 반영되었습니다.\n\n"
                    "현재 관리자 승인을 대기 중이므로 잠시만 기다려주세요."
                )
            else:
                reply_text = "✅ 양식 수정이 정상적으로 반영되었습니다."
        else:
            reply_text = (
                "⚠️ 양식은 정상적으로 작성되었으나, 서버/시트 통신 문제로 저장에 실패했습니다.\n\n"
                "잠시 후 양식을 다시 제출해 주시거나 점(.)을 입력하여 처음부터 다시 시도해 주세요!"
            )

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
            )
        return

    
    # 3. 안내 확인 답변 처리
    if not user_message.startswith("/") and any(word in user_message for word in ["확인", "확인했습니다", "확인완료"]):
        try:
            should_respond = False
            reply_text = ""

            with sheet_sync_lock():
                raw_user_ids = validation_sheet.col_values(5)
                clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
                current_user_id = str(user_id).strip()
                
                if current_user_id in clean_user_ids:
                    row_index = clean_user_ids.index(current_user_id) + 1
                    row_data = validation_sheet.row_values(row_index)
                    
                    # L열(12번째, 인덱스 11) 상태 확인
                    current_status = row_data[11].strip() if len(row_data) > 11 else ""
                    
                    # 상태가 "입장대기"일 때만 반응!
                    if current_status == "입장대기":
                        should_respond = True
                        
                        sheet_nickname = row_data[0].strip() if len(row_data) > 0 else ""
                        user_gender = row_data[1].strip() if len(row_data) > 1 else ""
                        
                        session_info = get_user_session(user_id) or {}
                        user_nickname = session_info.get("nickname", sheet_nickname)
                        
                        col_male, col_female = get_recording_ments()

                        if user_gender in ["남", "남자"]:
                            selected_ment = random.choice(col_male) if col_male else "남성 인증 문구를 불러올 수 없습니다."
                        elif user_gender in ["여", "여자"]:
                            selected_ment = random.choice(col_female) if col_female else "여성 인증 문구를 불러올 수 없습니다."
                        else:
                            selected_ment = "성별 정보가 올바르지 않습니다. 양식을 다시 작성해주세요."

                        reply_text = (
                            "⭕️ 작성이 완료되었다면 음성인증을 진행합니다.\n\n"
                            "키보드 상단 음성메시지를 활용해서 진행합니다.\n\n"
                            "아래 문구를 정확하게 읽어주세요.\n\n"
                            f"\"제 닉네임은 {user_nickname}입니다. 오늘은 OO월 OO일, 초대자 ■■입니다. {selected_ment}\"\n\n"
                            "조용한 곳에서 천천히 또박또박 부탁드립니다."
                        )
                        
                        # 상태를 '음성대기'로 업데이트
                        validation_sheet.update(range_name=f'L{row_index}', values=[['음성대기']])

            # 신입 회원(입장대기 상태)인 경우에만 메시지 전송
            if should_respond and reply_text:
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(
                        ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
                    )
            return
                    
        except Exception as e:
            print(f"인증멘트 로직 에러: {e}")
            error_text = f"⚠️ 서버 처리 중 오류가 발생했습니다. 잠시 후 다시 '확인'을 입력해 주세요.\n(시스템 메시지: {e})"
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=error_text)])
                )
        return



    # 4. 슬래시(/) 명령어 로직
    if not user_message.startswith("/"):
        return

    command = user_message[1:].strip()
    if command.startswith(("인증 ", "ㅇㅈ ")):
        if command.startswith("인증 "):
            search_query = command[3:].strip()   # "인증 " 제거
        else:
            search_query = command[2:].strip()   # "ㅇㅈ " 제거

        # 고정 단계별 안내문 수동 출력 로직
        if search_query in ["1단계", "양식", "양식안내"]:
            reply_text = (
                "안녕하세요\n"
                "𝔼·𝔻 ꕤ 𝔼·ℕ 신입 인증방에\n"
                "오신것을 환영합니다\n\n"
                "⭕️아래의 본문을 복사해서 빠.짐.없.이. 작성해주세요.\n\n"
                " - 닉네임(두글자):\n"
                " - 년생:\n"
                " - 나이: (만나이 ❌️):\n"
                " - 성별(남/여/남자/여자 중 하나만 입력):\n"
                " - 지역(시까지, 단 서울 및 광역시는 구까지):\n"
                " - 결혼유무(기/미/돌):\n"
                " - 군필여부(남자만):\n"
                " - 초대자:\n"
                " - 야단라경험유무(방 이름 및 임티, 기존에 썻던 닉):\n"
                " - 기존 다른방에서 나온이유(없다면 무) :\n"
                " - 다른 방에서 킥을 당한적 있는지(있다면 사유도) :"
            )
        elif search_query in ["2단계", "주의사항", "규정"]:
            reply_text = (
                "저희 커뮤니티 내부규정상 내부자료(앨범을 비롯 노트내용들이나 대화내용에 대해 " 
                "내부인원들의 동의없이 무단 유출은 개인정보보호법에 의거하여 추후 처벌대상이 될수도 있으니 꼭 유의하여 주세요\n\n" 
                "방에 불편한분이 계시면 예고없이 강퇴당할수있으니 참고바랍니다\n\n" 
                "읽고 확인이라고 입력해 주세요"
            )
        elif search_query in ["3단계", "음성", "음성안내"]:
            session_info = get_user_session(user_id) or {}
            user_nickname = session_info.get("nickname", "[본인닉네임]")
            
            col_male, col_female = get_recording_ments()
            all_ments = col_male + col_female
            
            random_ment = random.choice(all_ments) if all_ments else "인증 문구를 불러올 수 없습니다."

            reply_text = (
                "⭕️ 작성이 완료되었다면 음성인증을 진행합니다.\n\n"
                "키보드 상단 음성메시지를 활용해서 진행합니다.\n\n"
                "아래 문구를 정확하게 읽어주세요.\n\n"
                f"\"제 닉네임은 {user_nickname}입니다. 오늘은 OO월 OO일, 초대자 ■■입니다. {random_ment}\"\n\n"
                "조용한 곳에서 천천히 또박또박 부탁드립니다."
            )
        else:
            result = search_keyword(search_query)
            if result:
                reply_text = result
            else:
                reply_text = f"😢 '{search_query}' 미 입력된 인증멘트. 오타에 주의해주세요!"
            
    elif command == "목록":
        keywords = get_all_keywords()
        list_text = "📍 [단계별 고정 명령어]\n"
        list_text += "- /ㅇㅈ 1단계 (또는 양식)\n"
        list_text += "- /ㅇㅈ 2단계 (또는 주의사항)\n"
        list_text += "- /ㅇㅈ 3단계 (또는 음성)\n\n"
        list_text += "📋 [시트 등록 인증멘트]\n"
        
        if keywords:
            list_text += "\n".join(f"- {k}" for k in keywords)
            reply_text = list_text
        else:
            reply_text = list_text + "📭 현재 등록된 인증 멘트가 없습니다."
            
    elif command in ["id", "내정보", "아이디"]:
        reply_text = f"👤 당신의 LINE User ID:\n{user_id}\n\n위 ID를 복사하여 관리자에게 전달해 주세요!"
    else:
        reply_text = f"명령어 확인. '{command}'이런 명령어는 없습니다. 😢"

    if reply_text:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
            )



# ==========================================
# [핸들러 2] 유저 입장 시 처리 핸들러 (3개 인증방 어디든 동시 대응)
# ==========================================
@handler.add(MemberJoinedEvent)
def handle_member_joined(event):
    welcome_text = (
        "안녕하세요\n"
        "𝔼·𝔻 ꕤ 𝔼·ℕ 신입 인증방에\n"
        "오신것을 환영합니다\n\n"
        "⭕️아래의 본문을 복사해서 빠.짐.없.이. 작성해주세요.\n\n"
        " - 닉네임(두글자):\n"
        " - 년생:\n"
        " - 나이: (만나이 ❌️):\n"
        " - 성별(남/여/남자/여자 중 하나만 입력):\n"
        " - 지역(시까지, 단 서울 및 광역시는 구까지):\n"
        " - 결혼유무(기/미/돌):\n"
        " - 군필여부(남자만):\n"
        " - 초대자:\n"
        " - 야단라경험유무(방 이름 및 임티, 기존에 썻던 닉):\n"
        " - 기존 다른방에서 나온이유(없다면 무) :\n"
        " - 다른 방에서 킥을 당한적 있는지(있다면 사유도) :"
    )
    
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=event.reply_token, 
                    messages=[TextMessage(text=welcome_text)]
                )
            )
    except Exception as e:
        print(f"환영인사 전송 에러: {e}")


# ==========================================
# [핸들러 3] 음성 메시지(음성 인증) 처리 핸들러
# ==========================================
@handler.add(MessageEvent, message=AudioMessageContent)
def handle_audio_message(event):
    user_id = event.source.user_id
    if not user_id:
        return

    try:
        should_alert = False
        nickname = "알수없음"

        with sheet_sync_lock():
            raw_user_ids = validation_sheet.col_values(5)
            clean_user_ids = [str(uid).strip() for uid in raw_user_ids]
            current_user_id = str(user_id).strip()

            if current_user_id in clean_user_ids:
                row_index = clean_user_ids.index(current_user_id) + 1
                row_data = validation_sheet.row_values(row_index)
                
                # L열(12번째) 상태 확인
                current_status = row_data[11].strip() if len(row_data) >= 12 else ""
                nickname = row_data[0].strip() if len(row_data) > 0 else "알수없음"

                if current_status == "음성대기":
                    # 1. 상태를 '승인대기'로 배치 업데이트
                    validation_sheet.update(range_name=f'L{row_index}', values=[['승인대기']])
                    should_alert = True

        if should_alert:
            reply_text = (
                "🎙️ 음성 인증 메시지가 성공적으로 제출되었습니다!\n\n"
                "인증자 확인 후 이후절차 진행 예정이니 잠시만 기다려주세요. 감사합니다! 😊"
            )
            
            admin_alert_text = (
                f"🔔 [음성 인증 제출 완료]\n\n"
                f"👤 닉네임: {nickname}\n"
                f"해당 신입 유저가 음성 인증을 제출하여 상태가 [승인대기]로 변경되었습니다. "
                f"1:1 채팅방에서 음성을 확인하시고 승인 절차를 진행해 주세요!"
            )
            
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(
                        reply_token=event.reply_token, 
                        messages=[TextMessage(text=reply_text)]
                    )
                )
                
                line_bot_api.push_message(
                    PushMessageRequest(
                        to=ADMIN_GROUP_CHAT_ID,
                        messages=[TextMessage(text=admin_alert_text)]
                    )
                )

    except Exception as e:
        print(f"음성 메시지 처리 에러: {e}")
        error_text = "⚠️ 음성 메시지 접수 중 시스템 오류가 발생했습니다. 잠시 후 다시 전송해 주세요."
        try:
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(
                        reply_token=event.reply_token, 
                        messages=[TextMessage(text=error_text)]
                    )
                )
        except:
            pass


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

import os
import random
import json
import gspread
import datetime
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, abort

# Line SDK v3 컴포넌트
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, ReplyMessageRequest, TextMessage, PushMessageRequest
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent, MemberJoinedEvent

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

# 유저 상태 기억용 전역 변수
notified_users = {}

# 구글 스프레드시트 연결
sheet = client.open("인증멘트").worksheet("멘트")
validation_sheet = client.open("인증멘트").worksheet("검증")

def search_keyword(keyword):
    data = sheet.get_all_records()
    for row in data:
        cell_value = str(row.get('인증', '')).strip()
        keywords_in_cell = [k.strip() for k in cell_value.split(',') if k.strip()]
        if keyword in keywords_in_cell:
            return row.get('출력')
    return None

def get_all_keywords():
    try:
        values = sheet.col_values(1)
        keywords = [str(val).strip() for val in values[1:] if str(val).strip()]
        return keywords
    except Exception as e:
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
    
    # 💡 [해결 2] 봇 친구 추가 안 한 상태에서 그룹방 채팅 시 에러 방지
    if not user_id:
        # User ID를 불러올 수 없으면 이후 로직(시트 기록 등)이 고장나므로 무시합니다.
        return 

    user_message = event.message.text.strip()
    reply_text = ""

        # 🔄 0. 점(.)만 입력된 경우 임시 저장 데이터 및 인증 상태 초기화
    if user_message == ".":
        # 1. 딕셔너리 초기화 (서버 재시작 등의 이유로 없을 수도 있으므로 try/except 대신 if문 유지)
        if user_id in notified_users:
            del notified_users[user_id]
            
        # 2. 구글 시트 초기화
        try:
            # E열(User ID)에서 해당 사용자의 행 찾기
            user_ids = validation_sheet.col_values(5)
            if user_id in user_ids:
                row_index = user_ids.index(user_id) + 1

                # K열과 L열만 비우기
                validation_sheet.update_cell(row_index, 11, "")
                validation_sheet.update_cell(row_index, 12, "")
        except Exception as e:
            print(f"초기화 시트 에러: {e}")
            
        # 3. (중요) except 블록 밖으로 빼서 무조건 안내 메시지가 세팅되도록 변경
        reply_text = (
            "🔄 임시 저장된 데이터와 인증 진행 상태가 초기화되었습니다.\n"
            "신입 인증 양식을 처음부터 다시 작성해 주세요!"
        )

        # 4. (중요) 메시지를 여기서 바로 전송하고 return 하여 아래 로직으로 넘어가지 않게 방어
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=event.reply_token, 
                    messages=[TextMessage(text=reply_text)]
                )
            )
        return


    # 🛠️ 1. 그룹/룸 고유 아이디 확인 명령어
    if user_message == "/여긴어디?":
        source_type = event.source.type
        current_id = ""
        
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
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text=reply_text)]
                )
            )
        return

    # 📝 2. 신입 인증 양식 검사 및 중복 데이터 필터링 로직
    if "닉네임" in user_message and "년생" in user_message and "성별" in user_message:
        if user_id in notified_users:
            del notified_users[user_id]
            
        lines = user_message.split("\n")
        missing_fields = []
        
        required_fields = [
            ("닉네임(두글자)", "닉네임(두글자)"),
            ("년생", "년생"),
            ("나이:", "나이"),
            ("성별", "성별"),
            ("지역", "지역"),
            ("결혼유무", "결혼유무"),
            ("군필여부", "군필여부"),
            ("초대자", "초대자"),
            ("야단라경험유무", "야단라경험유무"),
            ("기존 다른방에서 나온이유", "기존 다른방에서 나온이유"),
            ("다른 방에서 킥을 당한적 있는지", "다른 방에서 킥을 당한적 있는지")
        ]
        
        extracted_data = {display_name: "" for _, display_name in required_fields}
        
        for search_key, display_name in required_fields:
            field_line = [l for l in lines if search_key in l]
            if field_line:
                parts = field_line[0].split(":")
                if len(parts) < 2 or not parts[1].strip():
                    if display_name == "군필여부":
                        gender_val = extracted_data.get("성별", "").strip()
                        if gender_val in ["여자", "여"]:
                            continue
                    missing_fields.append(display_name)
                else:
                    extracted_data[display_name] = parts[1].strip()
            else:
                if display_name == "군필여부":
                    gender_val = extracted_data.get("성별", "").strip()
                    if gender_val in ["여자", "여"]:
                        continue
                missing_fields.append(display_name)
                
        # 🔍 성별 유효성 검사 추가 (남, 여, 남자, 여자 중 하나만 허용)
        gender_value = extracted_data.get("성별", "").strip()
        if gender_value and gender_value not in ["남", "여", "남자", "여자"]:
            if "성별 (남, 여, 남자, 여자 중 하나만 입력)" not in missing_fields:
                missing_fields.append("성별 (남, 여, 남자, 여자 중 하나만 입력)")

        if not missing_fields:
            nickname = extracted_data["닉네임(두글자)"].strip()
            birth_year = extracted_data["년생"].strip()
            gender = extracted_data["성별"].strip()
            region = extracted_data["지역"].strip()
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")

            # 🔍 구글 시트 내역 전체 정밀 비교 + 블랙 사유 추출
            try:
                all_records = validation_sheet.get_all_records()
                
                is_id_matched = False
                alert_status = ""
                color_emoji = ""
                found_black_reasons = [] 
                
                for record in all_records:
                    rec_id = str(record.get("아이디", "")).strip()
                    rec_name = str(record.get("닉네임", "")).strip()
                    rec_year = str(record.get("년생", "")).strip()
                    rec_gender = str(record.get("성별", "")).strip()
                    rec_region = str(record.get("사는지역", "")).strip()
                    rec_black = str(record.get("블랙사유", "")).strip()
                    
                    if (rec_id and rec_id == str(user_id).strip()) or (rec_name == nickname):
                        if rec_black:
                            found_black_reasons.append(f"[{rec_name}/{rec_year}년생] -> {rec_black}")

                    if rec_id and rec_id == str(user_id).strip():
                        is_id_matched = True

                    if rec_name == nickname:
                        match_score = 0
                        if rec_year == birth_year: match_score += 1
                        if rec_gender == gender: match_score += 1
                        if rec_region == region: match_score += 1

                        if match_score == 3:
                            alert_status = "🚨 [적색 경고] 닉네임 및 모든 정보 일치"
                            color_emoji = "🔴"
                        elif match_score in [1, 2]:
                            if alert_status != "🚨 [적색 경고] 닉네임 및 모든 정보 일치":
                                alert_status = "⚠️ [황색 경고] 닉네임 및 정보 일부 일치"
                                color_emoji = "🟡"
                        else:
                            if not alert_status:
                                alert_status = "🔵 [주의] 닉네임 일치 유저"
                                color_emoji = "🟦"

                if is_id_matched and not alert_status:
                    alert_status = "🔄 [주의] 재입장 유저 (동일 ID 확인)"
                    color_emoji = "🟪"

                if found_black_reasons:
                    alert_status = "💀 [위험] 블랙리스트 유저 감지"
                    color_emoji = "⚫"

                if alert_status:
                    black_section = ""
                    if found_black_reasons:
                        unique_reasons = list(set(found_black_reasons))
                        reasons_str = "\n".join(unique_reasons)
                        black_section = f"⚠️ [시트 내역 블랙 사유]\n{reasons_str}\n\n"

                    alert_text = (
                        f"{color_emoji} 신입 양식 작성 중복 필터링\n\n"
                        f"📌 분류 상태: {alert_status}\n"
                        f"👤 입력 닉네임: {nickname} ({birth_year}년생)\n"
                        f"📍 입력 지역/성별: {region} / {gender}\n"
                        f"🆔 유저 고유 ID:\n{user_id}\n\n"
                        f"{black_section}"
                        f"💡 관리자분들께서는 위 내용 및 블랙 사유를 기반으로 승인 여부를 검토하시기 바랍니다."
                    )
                    
                    with ApiClient(configuration) as api_client:
                        line_bot_api = MessagingApi(api_client)
                        line_bot_api.push_message(
                            PushMessageRequest(
                                to=ADMIN_GROUP_CHAT_ID,
                                messages=[TextMessage(text=alert_text)]
                            )
                        )
            except Exception as filter_err:
                print(f"중복 필터링 시스템 에러: {filter_err}")

            try:
                all_data = validation_sheet.get_all_values() 
                found_row_index = -1
                current_retry_count = 0

                for idx, row in enumerate(all_data):
                    if len(row) >= 5 and str(row[4]).strip() == str(user_id).strip():
                        found_row_index = idx + 1 
                        try:
                            count_val = row[7] if len(row) >= 8 else "0"
                            current_retry_count = int(count_val) if count_val.isdigit() else 0
                        except:
                            current_retry_count = 0
                        break

                if found_row_index != -1:
                    new_count = current_retry_count + 1
                    update_data = [nickname, gender, region, birth_year, user_id, current_date, "", new_count]
                    validation_sheet.update(f'A{found_row_index}:H{found_row_index}', [update_data])
                else:
                    row_to_insert = [nickname, gender, region, birth_year, user_id, current_date, "", 1]
                    validation_sheet.append_row(row_to_insert)

            except Exception as sheet_err:
                print(f"구글 시트 입력/수정 실패: {sheet_err}")

            # 사용자별 닉네임 임시 저장
            notified_users[user_id] = {
                "nickname": nickname
            }
            reply_text = ("저희 커뮤니티 내부규정상 내부자료(앨범을 비롯 노트내용들이나 대화내용에 대해 " 
                          "내부인원들의 동의없이 무단 유출은 개인정보보호법에 의거하여 추후 처벌대상이 될수도 있으니 꼭 유의하여 주세요\n\n" 
                          "방에 불편한분이 계시면 예고없이 강퇴당할수있으니 참고바랍니다\n\n" 
                          "읽고 확인이라고 입력해 주세요")
            
            try:
                col_k = validation_sheet.col_values(11) 
                next_row = len(col_k) + 1
                validation_sheet.update_acell(f'K{next_row}', user_id)
                validation_sheet.update_acell(f'L{next_row}', '입장대기')
            except Exception as e:
                print(f"시트 업데이트 에러: {e}")
        else:
            fields_str = "\n".join(f"- {f}" for f in missing_fields)
            reply_text = ("⚠️ 작성 내용 중 누락되었거나 형식이 올바르지 않은 항목이 있습니다!\n\n"
                          f"📝 다시 채워주셔야 할 항목:\n{fields_str}\n\n"
                          "성별은 **'남', '여', '남자', '여자'** 중 하나만 입력해주시고, 양식을 정확히 확인하여 다시 보내주세요! 😢")
                          
        if reply_text:
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
                )
        return

    
    # 🤝 3. 안내 확인 답변 처리
    if not user_message.startswith("/") and any(word in user_message for word in ["확인", "확인했습니다", "확인완료"]):
        try:
            col_k = validation_sheet.col_values(11)
            if user_id in col_k:
                row_index = col_k.index(user_id) + 1
                
                user_gender = str(validation_sheet.cell(row_index, 2).value).strip()
                user_nickname = notified_users.get(user_id, {}).get(
                "nickname",
                str(validation_sheet.cell(row_index, 1).value).strip()
                )
                recording_sheet = client.open("인증멘트").worksheet("녹음")
                
                col_male = [cell for cell in recording_sheet.col_values(1)[1:] if cell and cell.strip()]
                col_female = [cell for cell in recording_sheet.col_values(2)[1:] if cell and cell.strip()]
                
                # 수정된 로직 예시
                if user_gender in ["남", "남자"]:
                    # 남성일 때 남성용 멘트 선택
                    selected_ment = random.choice(col_male) if col_male else "인증 문구가 준비 중입니다."
                elif user_gender in ["여", "여자"]:
                    # 여성일 때 여성용 멘트 선택
                    selected_ment = random.choice(col_female) if col_female else "인증 문구가 준비 중입니다."
                else:
                    # 예외 처리
                    selected_ment = "성별 정보가 올바르지 않습니다."

                reply_text = (
                    "⭕️ 작성이 완료되었다면 음성인증을 진행합니다.\n\n"
                    "키보드 상단 음성메시지를 활용해서 진행합니다.\n\n"
                    "아래 문구를 정확하게 읽어주세요.\n\n"
                    f"\"제 닉네임은 {user_nickname}입니다. 오늘은 OO월 OO일, 초대자 ■■입니다. {selected_ment}\"\n\n"
                    "조용한 곳에서 천천히 또박또박 부탁드립니다."
                )
                
                validation_sheet.update_acell(f'L{row_index}', '음성대기')
                
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(
                        ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
                    )
        except Exception as e:
            print(f"인증멘트 로직 에러: {e}")
        return

     # 📂 4. 슬래시(/) 명령어 로직
    if not user_message.startswith("/"):
        return

    command = user_message[1:].strip()
    
    if command.startswith("인증 "):
        search_query = command.replace("인증 ", "").strip()
        result = search_keyword(search_query)
        if result:
            reply_text = result
        else:
            reply_text = f"😢 '{search_query}' 미 입력된 인증멘트. 오타에 주의해주세요!"
    elif command == "목록":
        keywords = get_all_keywords()
        if keywords:
            list_text = "\n".join(f"- {k}" for k in keywords)
            reply_text = f"📋 현재 등록된 인증 리스트입니다:\n\n{list_text}"
        else:
            reply_text = "📭 현재 등록된 인증 멘트가 없습니다."
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
# [핸들러 2] 유저 입장 시 처리 핸들러
# ==========================================
@handler.add(MemberJoinedEvent)
def handle_member_joined(event):
    
    # 💡 [해결 1] 그룹 입장 시 정확한 user_id 추출
    user_id = event.joined.members[0].user_id if event.joined.members else None
    
    welcome_text = (
        "안녕하세요\n"
        "𝔼·𝔻 ꕤ 𝔼·ℕ 신입 인증방에\n"
        "오신것을 환영합니다\n\n"
        "⭕️아래의 본문을 복사해서 빠.짐.없.이. 작성해주세요.\n\n"
        " - 닉네임(두글자):\n"
        " - 년생:\n"
        " - 나이: (만나이 ❌️)\n"
        " - 성별(남/여/남자/여자 중 하나만 입력):\n"
        " - 지역(시까지, 단 서울 및 광역시는 구까지):\n"
        " - 결혼유무(기/미/돌):\n"
        " - 군필여부(남자만):\n"
        " - 초대자:\n"
        " - 야단라경험유무(방 이름 및 임티, 기존에 썻던 닉):\n"
        " - 기존 다른방에서 나온이유(없다면 무) :\n"
        " - 다른 방에서 킥을 당한적 있는지(있다면 사유도) :"
    )
    
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=welcome_text)])
        )

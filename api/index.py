import os
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
last_joined_user_id = None

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
    user_message = event.message.text.strip()
    reply_text = ""

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
                    missing_fields.append(display_name)
                else:
                    extracted_data[display_name] = parts[1].strip()
            else:
                missing_fields.append(display_name)
                
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
                found_black_reasons = [] # 발견된 블랙 사유 저장용
                
                for record in all_records:
                    rec_id = str(record.get("아이디", "")).strip()
                    rec_name = str(record.get("닉네임", "")).strip()
                    rec_year = str(record.get("년생", "")).strip()
                    rec_gender = str(record.get("성별", "")).strip()
                    rec_region = str(record.get("사는지역", "")).strip()
                    rec_black = str(record.get("블랙사유", "")).strip()
                    
                    # 1. 고유 ID 또는 닉네임이 매칭될 때 해당 행에 블랙사유가 적혀있으면 수집
                    if (rec_id and rec_id == str(user_id).strip()) or (rec_name == nickname):
                        if rec_black:
                            found_black_reasons.append(f"[{rec_name}/{rec_year}년생] -> {rec_black}")

                    # 2. 고유 라인 ID 일치 체크 (재입장 여부)
                    if rec_id and rec_id == str(user_id).strip():
                        is_id_matched = True

                    # 3. 닉네임 일치 기반 정보 중복 판정
                    if rec_name == nickname:
                        match_score = 0
                        if rec_year == birth_year: match_score += 1
                        if rec_gender == gender: match_score += 1
                        if rec_region == region: match_score += 1

                        if match_score == 3:
                            alert_status = "🚨 [적색 경고] 닉네임 및 모든 정보 일치"
                            color_emoji = "🔴"
                            # 적색경고여도 블랙사유를 끝까지 다 찾아야 하므로 break하지 않고 계속 돕니다.
                        elif match_score in [1, 2]:
                            if alert_status != "🚨 [적색 경고] 닉네임 및 모든 정보 일치":
                                alert_status = "⚠️ [황색 경고] 닉네임 및 정보 일부 일치"
                                color_emoji = "🟡"
                        else:
                            if not alert_status:
                                alert_status = "🔵 [주의] 닉네임 일치 유저"
                                color_emoji = "🟦"

                # 4. 재입장 유저 판정 (정보 중복이 없고 ID만 매칭될 때)
                if is_id_matched and not alert_status:
                    alert_status = "🔄 [주의] 재입장 유저 (동일 ID 확인)"
                    color_emoji = "🟪"

                # 5. [핵심] 블랙리스트 기록이 한 건이라도 발견된 경우 분류 상태를 강제로 '블랙 유저 감지'로 격상시킵니다.
                if found_black_reasons:
                    alert_status = "💀 [위험] 블랙리스트 유저 감지"
                    color_emoji = "⚫"

                # 경고/알림/블랙이 트리거되었을 때 알림 발송
                if alert_status:
                    black_section = ""
                    if found_black_reasons:
                        # 중복된 사유가 들어올 수 있으므로 깔끔하게 정렬 및 고유화
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

            # 📊 시트 구조 순서대로 행 삽입 (A~F열 순서 유지, 신입은 기본 블랙사유 빈칸)
            try:
                row_to_insert = [nickname, gender, region, birth_year, user_id, current_date, ""]
                validation_sheet.append_row(row_to_insert)
            except Exception as sheet_err:
                print(f"구글 시트 입력 실패: {sheet_err}")

            reply_text = ("저희 커뮤니티 내부규정상 내부자료(앨범을 비롯 노트내용들이나 대화내용에 대해 " 
                          "내부인원들의 동의없이 무단 유출은 개인정보보호법에 의거하여 추후 처벌대상이 될수도 있으니 꼭 유의하여 주세요\n\n" 
                          "방에 불편한분이 계시면 예고없이 강퇴당할수있으니 참고바랍니다\n\n" 
                          "읽고 확인해주세요")
            # 👇 [여기에 추가!] 유저 ID와 '입장대기' 상태를 검증 시트에 기록
            try:
                col_k = validation_sheet.col_values(11) # K열(인증중인 아이디)
                next_row = len(col_k) + 1
                validation_sheet.update_acell(f'K{next_row}', user_id)
                validation_sheet.update_acell(f'L{next_row}', '입장대기')
            except Exception as e:
                print(f"시트 업데이트 에러: {e}")
        else:
            fields_str = "\n".join(f"- {f}" for f in missing_fields)
            reply_text = ("⚠️ 작성 내용 중 누락되었거나 비어있는 항목이 있습니다!\n\n"
                          f"📝 다시 채워주셔야 할 항목:\n{fields_str}\n\n"
                          "복사하신 본문 양식의 콜론(:) 오른쪽에 내용을 빠.짐.없.이. 작성해서 다시 보내주세요! 😢")
                          
        if reply_text:
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
                )
        return

    # 🤝 3. 안내 확인 답변 처리 (수정된 섹션)
    if not user_message.startswith("/") and any(word in user_message for word in ["확인", "확인했습니다", "확인완료"]):
        try:
            # 시트 전체를 가져오는 대신, 유저 ID를 검색하는 효율적인 방식 권장
            # 여기서는 기존 로직의 흐름을 유지하며 들여쓰기만 교정합니다.
            col_k = validation_sheet.col_values(11) 
            
            if user_id in col_k:
                row_index = col_k.index(user_id) + 1
                current_status = validation_sheet.acell(f'L{row_index}').value
                
                if current_status == '입장대기':
                    validation_sheet.update_acell(f'L{row_index}', '입장확인')
                    reply_text = "인증자가 확인중이니 잠시 대기하여 주세요"
                    
                    with ApiClient(configuration) as api_client:
                        line_bot_api = MessagingApi(api_client)
                        line_bot_api.reply_message_with_http_info(
                            ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
                        )
            else:
                # 리스트에 없는 경우에 대한 예외 처리
                reply_text = "작성하신 양식이 확인되지 않습니다. 양식을 먼저 작성해주세요."
                with ApiClient(configuration) as api_client:
                    line_bot_api = MessagingApi(api_client)
                    line_bot_api.reply_message_with_http_info(
                        ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
                    )
        except Exception as e:
            print(f"시트 조회/수정 에러: {e}")
            
        return # 이 return은 if문 블록 내에 위치해야 합니다.
    # 📂 4. 슬래시(/) 명령어 로직
    if not user_message.startswith("/"):
        return

    command = user_message[1:].strip()
    
    if "닉변" in command:
        reply_text = "닉넴 복붙하셔서 변경해주시고요 \n 프사는 도용사진이 아닌 본인사진 또는 아무사진이나 설정부탁드립니다 \n \n \n 그리고 헤르패스 확진판정 받으신적 있으실까요?"
    elif "개인정보" in command:
        reply_text = "저희 커뮤니티 내부규정상 내부자료(앨범을 비롯 노트내용들이나 대화내용에 대해 내부인원들의 동의없이 무단 유출은 개인정보보호법에 의거하여 추후 처벌대상이 될수도 있으니 꼭 유의하여 주세요 \n \n 방에 불편한분이 계시면 예고없이 강퇴당할수있으니 참고바랍니다 \n \n 읽고 확인해주세요"
    elif "확인" in command:
        reply_text = ("네 확인했습니다.\n\n"
                      "⚠️ 도용이거나 인증과정에서 거짓이 발견되면 경고 없이 킥 조치되오니 이 점 유의하여 주세요!\n\n"
                      "그리고 내부 인원과 불편한 관계가 있다면 저흰 내부 인원을 우선으로 생각하기에 별도 안내 없이 킥되실 수도 있습니다.\n\n"
                      "또 잦은 들낙도 블랙 사유가 될 수 있습니다.\n\n"
                      "입장하시면 족보 먼저 작성 부탁드리고 공지사항도 꼭 숙지 부탁드립니다.\n\n"
                      "인증방은 나가기 해주시면 본방 초대해 드리겠습니다.")
    elif command.startswith("인증 "):
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
    elif "입장" in command:
        reply_text = ("안녕하세요\n"
                      "𝔼·𝔻 ꕤ 𝔼·ℕ 신입 인증방에\n"
                      "오신것을 환영합니다\n\n"
                      "⭕️아래의 본문을 복사해서 빠.짐.없.이. 작성해주세요.\n\n"
                      " - 닉네임(두글자):\n"
                      " - 년생:\n"
                      " - 나이: (만나이 ❌️)\n"
                      " - 성별:\n"
                      " - 지역(시까지, 단 서울 및 광역시는 구까지):\n"
                      " - 결혼유무(기/미/돌):\n"
                      " - 군필여부(남자만):\n"
                      " - 초대자:\n"
                      " - 야단라경험유무(방 이름 및 임티, 기존에 썻던 닉):\n"
                      " - 기존 다른방에서 나온이유(없다면 무) :\n"
                      " - 다른 방에서 킥을 당한적 있는지(있다면 사유도) :")
    elif command == "처음":
        reply_text = ("안녕하세요.\n"
                      "저희 방은 일상대화, 19금대화, 만남을 하는 곳입니다.\n"
                      "사진, 영상도 본인 선택으로 올리고, 서로 마음도 맞고 관심 가는 사람이랑 만날 수도 있습니다!\n"
                      "커피 한 잔 마시기도 하고 담배만 피고 헤어지기도, 밥 먹기도, 술도, 그리고 성인들이니 합의하에 하고 싶은 것 할 수 있는 곳입니다.\n\n"
                      "하지만 이런 걸 원하지 않으시는 분들껜 죄송하지만 입장을 도와드리진 않습니다. 그저 방에 대한 설명이고, 이로 인해 불편한 감정을 느끼셨다면 죄송합니다.\n\n"
                      "중요한 건 본인이 원하신다는 조건하에, 상호 합의하에 가능한 일이에요!\n\n"
                      "다른 방도 그렇지만 저희 방도 미션이라고 여성 초대 하셔서 말마디 채우시면 미션 클리어 돼서 여성분한테 갠라도 받고 여성과 벙도 할 수 있어요! 여자초대 미션 괜찮으실까요?\n")
    elif command == "동반":
        reply_text = ("동반분과 커플, 원픽, 네토는 아니신가요?\n"
                      "동반 분이 다른분들과 벙을 해도 상관 없으신가요?")
    elif command == "퇴장":
        reply_text = ("네 확인했습니다\n"
                      "도용이거나 인증과정에서 거짓이 발견되면 경고없이 킥조치되오니 이점 유의하여 주세요 !\n\n"
                      "그리고 내부인원과 불편한 관계가 있다면 저흰 내부인원을 우선으로 생각하기에 별도 안내없이 킥되실수도 있습니다.\n\n"
                      "또 잦은 들낙도 블랙 사유가 될 수 있습니다.\n\n"
                      "입장하시면 족보먼저 작성부탁드리고 공지사항도 꼭 숙지부탁드립니다.\n\n"
                      "인증방은 나가기해주시면 본방초대해드리겠습니다.")
    elif command == "불가":
        reply_text = ("죄송합니다 저희 방 입장은 불가능할 것 같습니다.\n"
                      "인증방은 나가주세요.")
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
    global last_joined_user_id
    user_id = event.source.user_id
    last_joined_user_id = user_id
    
    welcome_text = (
        "안녕하세요\n"
        "𝔼·𝔻 ꕤ 𝔼·ℕ 신입 인증방에\n"
        "오신것을 환영합니다\n\n"
        "⭕️아래의 본문을 복사해서 빠.짐.없.이. 작성해주세요.\n\n"
        " - 닉네임(두글자):\n"
        " - 년생:\n"
        " - 나이: (만나이 ❌️)\n"
        " - 성별:\n"
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

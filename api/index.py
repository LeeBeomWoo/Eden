import os
import json
import gspread
import datetime
from oauth2client.service_account import ServiceAccountCredentials
from flask import Flask, request, abort

# ✨ Line SDK v3 필수 컴포넌트들을 정확히 import 해야 합니다.
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, ReplyMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

app = Flask(__name__)

# 환경변수 설정 및 핸들러 초기화
configuration = Configuration(access_token=os.environ.get("LINE_CHANNEL_ACCESS_TOKEN"))
handler = WebhookHandler(os.environ.get("LINE_CHANNEL_SECRET"))

# 환경변수에서 JSON 문자열을 가져와서 인증 객체 생성
json_key_str = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
json_key_dict = json.loads(json_key_str)
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(json_key_dict, scope)
client = gspread.authorize(creds)
# 유저가 대기 안내를 받았는지 기억하는 상자입니다. (서버가 켜져있는 동안 유지)
notified_users = {}
# 가장 마지막에 방에 입장한 유저의 ID를 기억하는 상자입니다.
last_joined_user_id = None

# 1. 시트 연결: 파일명 "인증멘트"의 "멘트" 탭을 엽니다.
sheet = client.open("인증멘트").worksheet("멘트")
validation_sheet = client.open("인증멘트").worksheet("검증")

def search_keyword(keyword):
    # 첫 번째 줄을 제목으로 인식하여 데이터를 가져옵니다.
    data = sheet.get_all_records()
    for row in data:
        # A열(인증)에 적힌 텍스트를 가져옵니다.
        cell_value = str(row.get('인증', '')).strip()
        
        # 콤마(,)를 기준으로 단어들을 쪼개고, 각 단어 앞뒤의 띄어쓰기(공백)를 깔끔하게 지워 리스트로 만듭니다.
        # 예: "홍길동, 김철수 ,이영희" -> ['홍길동', '김철수', '이영희']
        keywords_in_cell = [k.strip() for k in cell_value.split(',') if k.strip()]
        
        # 사용자가 입력한 검색어가 이 리스트 안에 정확히 존재하는지 확인합니다.
        if keyword in keywords_in_cell:
            # 존재한다면 B열(출력)의 데이터를 반환합니다.
            return row.get('출력')
            
    return None

    
def get_all_keywords():
    try:
        # A열(첫 번째 열)의 모든 값을 가져옵니다.
        values = sheet.col_values(1)
        # 첫 번째 줄(제목 '인증')을 제외하고 빈칸이 아닌 값들만 모읍니다.
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
# 파일 상단 핸들러 등록부 아래나, handle_message 함수 바로 위에 추가하세요.
# 유저가 대기 안내를 받았는지 기억하는 상자입니다. (서버가 켜져있는 동안 유지)
notified_users = {}

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = event.source.user_id  # 메시지를 보낸 유저의 고유 ID
    user_message = event.message.text.strip()
    reply_text = ""

    # 1. 신입 인증 양식 검사 로직 (제일 먼저 수행)
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
        
        # 각 필드의 값을 추출하기 위한 딕셔너리 초기화
        extracted_data = {display_name: "" for _, display_name in required_fields}
        
        for search_key, display_name in required_fields:
            field_line = [l for l in lines if search_key in l ]
            if field_line:
                parts = field_line [0 ] . split (":" )
                if len (parts ) < 2 or not parts [1 ] . strip () :
                    missing_fields. append (display_name )
                else:
                    # 💡 성공 시 저장을 위해 콜론(:) 오른쪽 값을 공백 제거 후 보관
                    extracted_data[display_name] = parts[1].strip()
            else :
                missing_fields. append (display_name )
                
        if not missing_fields:
            # ⭕ 양식 성공 시 -> 구글 시트 '검증' 탭에 데이터 입력 추가!
            try:
                # 데이터 파싱
                nickname = extracted_data["닉네임(두글자)"]
                gender = extracted_data["성별"]
                region = extracted_data["지역"]
                
                # 들어온 날짜 포맷팅 (예: 2026-07-19)
                current_date = datetime.datetime.now().strftime("%Y-%m-%d")
                
                # 💡 순서: [닉네임, 마지막 들어온 날짜, 성별, 사는지역, 아이디]
                # '검증' 시트 E열에 user_id가 들어가게 됩니다.
                row_to_insert = [nickname, current_date, gender, region, user_id]
                
                # 시트에 행 추가
                validation_sheet.append_row(row_to_insert)
            except Exception as sheet_err:
                print(f"구글 시트 입력 실패: {sheet_err}")

            # 기존 내부규정 안내 출력 멘트
            reply_text = ("저희 커뮤니티 내부규정상 내부자료(앨범을 비롯 노트내용들이나 대화내용에 대해 " 
                          "내부인원들의 동의없이 무단 유출은 개인정보보호법에 의거하여 추후 처벌대상이 될수도 있으니 꼭 유의하여 주세요\n\n" 
                          "방에 불편한분이 계시면 예고없이 강퇴당할수있으니 참고바랍니다\n\n" 
                          "읽고 확인해주세요" )
        else :
            # ❌ 빈칸이 있을 시 -> 안내 멘트
            fields_str = "\n" . join (f"- {f } " for f in missing_fields )
            reply_text = ("⚠️ 작성 내용 중 누락되었거나 비어있는 항목이 있습니다!\n\n"
                          f"📝 다시 채워주셔야 할 항목:\n {fields_str } \n\n"
                          "복사하신 본문 양식의 콜론(:) 오른쪽에 내용을 빠.짐.없.이. 작성해서 다시 보내주세요! 😢" )
        if reply_text:
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.reply_message_with_http_info(
                    ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
                )
        return

   # 1-2. 개인정보 규정 안내를 받고 사용자가 "확인" 대답을 했을 때 처리하는 로직
    if not user_message.startswith("/") and any(word in user_message for word in ["확인", "확인했습니다", "확인했어요", "넹", "네"]):
        global last_joined_user_id
        
        # 💡 [핵심 추가] 메시지를 보낸 사람이 '가장 마지막에 온 사람'이 아니라면 아예 무시(대답 안 함)
        if user_id != last_joined_user_id:
            return
            
        # 이미 대기 안내를 보낸 유저라면 봇이 대답하지 않고 무시(return)합니다.
        if notified_users.get(user_id) is True:
            return

        reply_text = "인증자가 확인중이니 잠시 대기하여 주세요"
        
        # 안내를 발송했음을 기록(True)합니다.
        notified_users[user_id] = True
        
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
            )
        return
    # 2. 기존의 '/'로 시작하는 명령어 처리 로직 (이하는 기존 코드 유지)
    # 2. 기존의 '/'로 시작하는 명령어 처리 로직
    if not user_message.startswith("/"):
        return

    command = user_message[1:].strip()
    
    # 명령어 분기 로직 (기존 코드 그대로 유지)
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
        user_id = event.source.user_id
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
    if user_message == "여긴어디?":
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
            
        # 바로 그룹방에 고유 ID를 답장(Reply)으로 전송
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text=reply_text)]
                )
            )
        return  # 그룹 ID를 확인한 후에는 아래의 신입인증 검사나 다른 로직이 실행되지 않고 종료되도록 리턴 처리
    else:
        reply_text = f"명령어 확인. '{command}'이런 명령어는 없습니다. 😢"

    if reply_text:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(reply_token=event.reply_token, messages=[TextMessage(text=reply_text)])
            )

from linebot.v3.webhooks import MemberJoinedEvent, JoinEvent

# 상황 1: 새로운 사람이 방에 들어왔을 때 (인증방에 유저가 입장)
@handler.add(MemberJoinedEvent)
def handle_member_joined(event):
    global last_joined_user_id
    last_joined_user_id = event.source.user_id  # 가장 마지막에 온 사람의 ID 저장
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
ADMIN_GROUP_CHAT_ID = "YOUR_ADMIN_GROUP_CHAT_ID"

@handler.add(FollowEvent)  # 또는 사용 환경에 따라 JoinEvent
def handle_join(event):
    global last_joined_user_id
    user_id = event.source.user_id
    last_joined_user_id = user_id  # 최근 입장 유저 ID 업데이트
    
    try:
        # 1. '검증' 시트의 모든 데이터 가져오기 (E열이 아이디라고 가정)
        # 헤더: 닉네임, 마지막 들어온 날짜, 성별, 사는지역, 아이디
        all_records = validation_sheet.get_all_records()
        
        # 2. 동일한 아이디가 몇 번이나 기록되어 있는지 확인
        # (과거에 들어왔다 나간 기록이 여러 번 있을 수 있으므로 횟수를 카운트합니다.)
        match_count = 0
        matched_nicknames = []
        
        for record in all_records:
            # 시트의 '아이디' 열에 기록된 값과 현재 들어온 user_id 비교
            if str(record.get("아이디")) == str(user_id):
                match_count += 1
                if record.get("닉네임(두글자)"):
                    matched_nicknames.append(record.get("닉네임(두글자)"))
                elif record.get("닉네임"):
                    matched_nicknames.append(record.get("닉네임"))

        # 3. 중복이 존재한다면 일치 횟수(위험도)에 따라 분류하여 인증자방에 알림
        if match_count > 0:
            status = ""
            color_emoji = ""
            
            if match_count >= 3:
                status = "🚨 [적색 경고] 대단히 위험"
                color_emoji = "🔴"
            elif match_count == 2:
                status = "⚠️ [황색 경고] 의심 유저"
                color_emoji = "🟡"
            else:
                status = "👀 [주의] 재입장 유저"
                color_emoji = "🔵"
                
            nicknames_str = ", ".join(matched_nicknames)
            
            # 인증자방에 보낼 경고 메시지 구성
            alert_text = (
                f"{color_emoji} 신입 입장 중복 알림\n\n"
                f"📌 상태: {status}\n"
                f"👤 매칭된 기존 닉네임: {nicknames_str}\n"
                f"횟수: 과거 {match_count}회 일치 기록 있음\n"
                f"🆔 유저 고유 ID:\n{user_id}\n\n"
                f"💡 해당 유저의 인증 절차 진행 시 주의하시기 바랍니다."
            )
            
            # 인증자방(그룹방)으로 메시지 강제 푸시(Push)
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.push_message(
                    PushMessageRequest(
                        to=ADMIN_GROUP_CHAT_ID,
                        messages=[TextMessage(text=alert_text)]
                    )
                )
                
    except Exception as e:
        print(f"입장 유저 중복 검사 중 오류 발생: {e}")ADMIN_GROUP_CHAT_ID = "YOUR_ADMIN_GROUP_CHAT_ID"

@handler.add(FollowEvent)  # 또는 사용 환경에 따라 JoinEvent
def handle_join(event):
    global last_joined_user_id
    user_id = event.source.user_id
    last_joined_user_id = user_id  # 최근 입장 유저 ID 업데이트
    
    try:
        # 1. '검증' 시트의 모든 데이터 가져오기 (E열이 아이디라고 가정)
        # 헤더: 닉네임, 마지막 들어온 날짜, 성별, 사는지역, 아이디
        all_records = validation_sheet.get_all_records()
        
        # 2. 동일한 아이디가 몇 번이나 기록되어 있는지 확인
        # (과거에 들어왔다 나간 기록이 여러 번 있을 수 있으므로 횟수를 카운트합니다.)
        match_count = 0
        matched_nicknames = []
        
        for record in all_records:
            # 시트의 '아이디' 열에 기록된 값과 현재 들어온 user_id 비교
            if str(record.get("아이디")) == str(user_id):
                match_count += 1
                if record.get("닉네임(두글자)"):
                    matched_nicknames.append(record.get("닉네임(두글자)"))
                elif record.get("닉네임"):
                    matched_nicknames.append(record.get("닉네임"))

        # 3. 중복이 존재한다면 일치 횟수(위험도)에 따라 분류하여 인증자방에 알림
        if match_count > 0:
            status = ""
            color_emoji = ""
            
            if match_count >= 3:
                status = "🚨 [적색 경고] 대단히 위험"
                color_emoji = "🔴"
            elif match_count == 2:
                status = "⚠️ [황색 경고] 의심 유저"
                color_emoji = "🟡"
            else:
                status = "👀 [주의] 재입장 유저"
                color_emoji = "🔵"
                
            nicknames_str = ", ".join(matched_nicknames)
            
            # 인증자방에 보낼 경고 메시지 구성
            alert_text = (
                f"{color_emoji} 신입 입장 중복 알림\n\n"
                f"📌 상태: {status}\n"
                f"👤 매칭된 기존 닉네임: {nicknames_str}\n"
                f"횟수: 과거 {match_count}회 일치 기록 있음\n"
                f"🆔 유저 고유 ID:\n{user_id}\n\n"
                f"💡 해당 유저의 인증 절차 진행 시 주의하시기 바랍니다."
            )
            
            # 인증자방(그룹방)으로 메시지 강제 푸시(Push)
            with ApiClient(configuration) as api_client:
                line_bot_api = MessagingApi(api_client)
                line_bot_api.push_message(
                    PushMessageRequest(
                        to=ADMIN_GROUP_CHAT_ID,
                        messages=[TextMessage(text=alert_text)]
                    )
                )
                
    except Exception as e:
        print(f"입장 유저 중복 검사 중 오류 발생: {e}")

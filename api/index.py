import os
import json
import gspread
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

# 1. 시트 연결: 파일명 "인증멘트"의 "멘트" 탭을 엽니다.
sheet = client.open("인증멘트").worksheet("멘트")

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

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_message = event.message.text.strip()
    
    # '/'로 시작하지 않으면 즉시 종료
    if not user_message.startswith("/"):
        return
        
    command = user_message[1:].strip()
    reply_text = ""
    
    # 명령어 분기 로직
    if "닉변" in command:
        reply_text = "닉넴 복붙하셔서 변경해주시고요 \n 프사는 도용사진이 아닌 본인사진 또는 아무사진이나 설정부탁드립니다 \n \n \n 그리고 헤르패스 확진판정 받으신적 있으실까요?"
    elif "개인정보" in command:
        reply_text = "저희 커뮤니티 내부규정상 내부자료(앨범을 비롯 노트내용들이나 대화내용에 대해 내부인원들의 동의없이 무단 유출은 개인정보보호법에 의거하여 추후 처벌대상이 될수도 있으니 꼭 유의하여 주세요 \n \n 방에 불편한분이 계시면 예고없이 강퇴당할수있으니 참고바랍니다 \n \n 읽고 확인해주세요"
    elif "확인" in command:
        reply_text = ("네 확인했습니다.\n\n" "⚠️ 도용이거나 인증과정에서 거짓이 발견되면 경고 없이 킥 조치되오니 이 점 유의하여 주세요!\n\n" "그리고 내부 인원과 불편한 관계가 있다면 저흰 내부 인원을 우선으로 생각하기에 별도 안내 없이 킥되실 수도 있습니다.\n\n" "또 잦은 들낙도 블랙 사유가 될 수 있습니다.\n\n" "입장하시면 족보 먼저 작성 부탁드리고 공지사항도 꼭 숙지 부탁드립니다.\n\n" "인증방은 나가기 해주시면 본방 초대해 드리겠습니다.")
    elif command.startswith("인증 "):
        # 사용자가 "/인증 키워드" 라고 치면 키워드만 추출합니다.
        search_query = command.replace("인증 ", "").strip()
        result = search_keyword(search_query)
        
        if result:
            # 인증멘트를 찾았을 때 (수식어 없이 결과값만 출력)
            reply_text = result
        else:
            # 못 찾았을 때의 답변
            reply_text = f"😢 '{search_query}' 미 입력된 인증멘트. 오타에 주의해주세요!"
    elif command == "목록":
        keywords = get_all_keywords()
        
        if keywords:
            # 가져온 목록을 줄바꿈(- 항목명) 형태로 깔끔하게 나열합니다.
            list_text = "\n".join(f"- {k}" for k in keywords)
            reply_text = f"📋 현재 등록된 인증 리스트입니다:\n\n{list_text}"
        else:
            reply_text = "📭 현재 등록된 인증 멘트가 없습니다."
    elif command in ["id", "내정보", "아이디"]:
        user_id = event.source.user_id
        reply_text = f"👤 당신의 LINE User ID:\n{user_id}\n\n위 ID를 복사하여 관리자에게 전달해 주세요!"
    elif "입장" in command:
        reply_text = ("안녕하세요\n" "𝔼·𝔻 ꕤ 𝔼·ℕ 신입 인증방에\n" "오신것을 환영합니다\n\n" "⭕️아래의 본문을 복사해서 빠.짐.없.이. 작성해주세요.\n\n" " - 닉네임(두글자):\n" " - 년생:\n" " - 나이: (만나이 ❌️)\n" " - 성별:\n" " - 지역(시까지, 단 서울 및 광역시는 구까지):\n" " - 결혼유무(기/미/돌):\n" " - 군필여부(남자만):\n" " - 초대자:\n" " - 야단라경험유무(방 이름 및 임티, 기존에 썻던 닉):\n" " - 기존 다른방에서 나온이유(없다면 무) :\n" " - 다른 방에서 킥을 당한적 있는지(있다면 사유도) :" )
    elif command == "처음":
        reply_text = ("안녕하세요.\n" "저희 방은 일상대화, 19금대화, 만남을 하는 곳입니다.\n" "사진, 영상도 본인 선택으로 올리고, 서로 마음도 맞고 관심 가는 사람이랑 만날 수도 있습니다!\n" "커피 한 잔 마시기도 하고 담배만 피고 헤어지기도, 밥 먹기도, 술도, 그리고 성인들이니 합의하에 하고 싶은 것 할 수 있는 곳입니다.\n\n" "하지만 이런 걸 원하지 않으시는 분들껜 죄송하지만 입장을 도와드리진 않습니다. 그저 방에 대한 설명이고, 이로 인해 불편한 감정을 느끼셨다면 죄송합니다.\n\n" "중요한 건 본인이 원하신다는 조건하에, 상호 합의하에 가능한 일이에요!\n\n" "다른 방도 그렇지만 저희 방도 미션이라고 여성 초대 하셔서 말마디 채우시면 미션 클리어 돼서 여성분한테 갠라도 받고 여성과 벙도 할 수 있어요! 여자초대 미션 괜찮으실까요?\n" )
    elif command == "동반":
        reply_text = ("동반분과 커플, 원픽, 네토는 아니신가요?\n" "동반 분이 다른분들과 벙을 해도 상관 없으신가요?" )
    elif command == "퇴장":
        reply_text = ("네 확인했습니다\n" "도용이거나 인증과정에서 거짓이 발견되면 경고없이 킥조치되오니 이점 유의하여 주세요 !\n\n" "그리고 내부인원과 불편한 관계가 있다면 저흰 내부인원을 우선으로 생각하기에 별도 안내없이 킥되실수도 있습니다.\n\n" "또 잦은 들낙도 블랙 사유가 될 수 있습니다.\n\n" "입장하시면 족보먼저 작성부탁드리고 공지사항도 꼭 숙지부탁드립니다.\n\n" "인증방은 나가기해주시면 본방초대해드리겠습니다." )
    elif command == "불가":
        reply_text = ("죄송합니다 저희 방 입장은 불가능할 것 같습니다.\n" "인증방은 나가주세요." )
    else:
        reply_text = f"없어. '{command}'이런 명령언. 😢\n\n 자꾸 없는거 치면 파업한다?"
        
    # 실제로 라인에 메시지를 전송하는 필수 로직입니다.
    if reply_text:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message_with_http_info(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text=reply_text)]
                )
            )

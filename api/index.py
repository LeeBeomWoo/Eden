import os
from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, 
    ReplyMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

app = Flask(__name__)

# 1. 여기에 웹 브라우저 접속용 기본 루트(/) 경로를 추가합니다!
@app.route("/")
def home():
    return "Eden LINE Bot Server is Running!"

# 환경변수 로드
configuration = Configuration(access_token=os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")) #
handler = WebhookHandler(os.environ.get("LINE_CHANNEL_SECRET")) #

# 기존 LINE 웹훅 주소 (이 경로는 라인 개발자 센터의 Webhook URL에 등록해야 합니다)
@app.route("/api", methods=['POST']) #
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
    user_message = event.message.text.strip()  # 공백 제거
    
    # 👇 맨 앞에 '/'가 없으면 챗봇이 아무 반응도 하지 않고 종료하도록 예외 처리 추가!
    if not user_message.startswith("/"):
        return

    # '/'를 제외한 실제 명령어만 추출 (예: "/확인" -> "확인")
    command = user_message[1:].strip()
    reply_text = ""

    # 1. 키워드별 고정 답변 분기 처리 (기존 user_message 대신 command 변수를 사용합니다)
    if "닉변" in command:
        reply_text = "닉넴 복붙하셔서 변경해주시고요 \n 프사는 도용사진이 아닌 본인사진 또는 아무사진이나 설정부탁드립니다 \n \n \n 그리고 헤르패스 확진판정 받으신적 있으실까요?"
        
    elif "개인정보" in command:
        reply_text = "저희 커뮤니티 내부규정상 내부자료(앨범을 비롯 노트내용들이나 대화내용에 대해 내부인원들의 동의없이 무단 유출은 개인정보보호법에 의거하여 추후 처벌대상이 될수도 있으니 꼭 유의하여 주세요 \n \n 방에 불편한분이 계시면 예고없이 강퇴당할수있으니 참고바랍니다 \n \n 읽고 확인해주세요"
        
    elif "확인" in command:
        reply_text = (
            "네 확인했습니다.\n\n"
            "⚠️ 도용이거나 인증과정에서 거짓이 발견되면 경고 없이 킥 조치되오니 이 점 유의하여 주세요!\n\n"
            "그리고 내부 인원과 불편한 관계가 있다면 저흰 내부 인원을 우선으로 생각하기에 별도 안내 없이 킥되실 수도 있습니다.\n\n"
            "또 잦은 들낙도 블랙 사유가 될 수 있습니다.\n\n"
            "입장하시면 족보 먼저 작성 부탁드리고 공지사항도 꼭 숙지 부탁드립니다.\n\n"
            "인증방은 나가기 해주시면 본방 초대해 드리겠습니다."
        )
        
    elif "입장" in command:
        reply_text = (
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
    else:
        # 2. 지정되지 않은 명령어가 입력되었을 때 기본 답변
        reply_text = f"죄송합니다. '{command}'에 대한 답변을 찾지 못했습니다. 😢\n\n'위치', '영업시간', '가격' 등 필요한 키워드를 정확히 입력해주세요!"

    # 3. LINE 메시지 전송 실행
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply_text)]
            )
        )

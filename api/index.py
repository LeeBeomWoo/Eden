import os
from flask import Flask, request, abort

# ✨ Line SDK v3 필수 컴포넌트들을 정확히 import 해야 합니다.
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent

app = Flask(__name__)

@app.route("/")
def home():
    return "Eden LINE Bot Server is Running!"

# 환경변수 설정 및 핸들러 초기화
configuration = Configuration(access_token=os.environ.get("LINE_CHANNEL_ACCESS_TOKEN"))
handler = WebhookHandler(os.environ.get("LINE_CHANNEL_SECRET"))

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
    elif command == "처음":
        reply_text = (
            "안녕하세요.\n"
            "저희 방은 일상대화, 19금대화, 만남을 하는 곳입니다.\n"
            "사진, 영상도 본인 선택으로 올리고, 서로 마음도 맞고 관심 가는 사람이랑 만날 수도 있습니다!\n"
            "커피 한 잔 마시기도 하고 담배만 피고 헤어지기도, 밥 먹기도, 술도, 그리고 성인들이니 합의하에 하고 싶은 것 할 수 있는 곳입니다.\n\n"
            "하지만 이런 걸 원하지 않으시는 분들껜 죄송하지만 입장을 도와드리진 않습니다. 그저 방에 대한 설명이고, 이로 인해 불편한 감정을 느끼셨다면 죄송합니다.\n\n"
            "중요한 건 본인이 원하신다는 조건하에, 상호 합의하에 가능한 일이에요!\n\n"
            "다른 방도 그렇지만 저희 방도 미션이라고 여성 초대 하셔서 말마디 채우시면 미션 클리어 돼서 여성분한테 갠라도 받고 여성과 벙도 할 수 있어요! 여자초대 미션 괜찮으실까요?\n"
        )
    elif command == "동반":
        reply_text = (
            "동반분과 커플, 원픽, 네토는 아니신가요?\n"
            "동반 분이 다른분들과 벙을 해도 상관 없으신가요?"
        )
    elif command == "퇴장":
        reply_text = (
            "네 확인했습니다\n"
            "도용이거나 인증과정에서 거짓이 발견되면 경고없이 킥조치되오니 이점 유의하여 주세요 !\n\n"
            "그리고 내부인원과 불편한 관계가 있다면 저흰 내부인원을 우선으로 생각하기에 별도 안내없이 킥되실수도 있습니다.\n\n"
            "또 잦은 들낙도 블랙 사유가 될 수 있습니다.\n\n"
            "입장하시면 족보먼저 작성부탁드리고 공지사항도 꼭 숙지부탁드립니다.\n\n"
            "인증방은 나가기해주시면 본방초대해드리겠습니다."
        )
    elif command == "불가":
        reply_text = (
            "죄송합니다 저희 방 입장은 불가능할 것 같습니다.\n"
            "인증방은 나가주세요."
        )
    else:
        reply_text = f"없어. '{command}'이런 명령언. 😢\n\n 자꾸 없는거 치면 파업한다?"

    # ✨ 메시지 전송 로직을 안전하게 실행
    try:
        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text=reply_text)]
                )
            )
    except Exception as e:
        print(f"메시지 전송 중 오류 발생: {e}")

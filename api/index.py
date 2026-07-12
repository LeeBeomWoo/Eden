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
    reply_text = ""

    # 1. 키워드별 고정 답변 분기 처리
    if "위치" in user_message or "주소" in user_message:
        reply_text = "📍 매장 위치 안내\n서울시 강남구 테헤란로 123 4층입니다.\n(주차 가능, 역삼역 2번 출구 도보 3분)"
        
    elif "시간" in user_message or "영업" in user_message:
        reply_text = "🕒 영업시간 안내\n- 평일: 09:00 ~ 18:00\n- 주말 및 공휴일은 휴무입니다.\n(점심시간: 12:00 ~ 13:00)"
        
    elif "가격" in user_message or "비용" in user_message:
        reply_text = "💰 이용 금액 안내\n- 기본 플랜: 월 19,000원\n- 프로 플랜: 월 49,000원\n자세한 내용은 홈페이지를 참고해주세요!"
        
    elif "안녕하세요" in user_message or "하이" in user_message:
        reply_text = "안녕하세요! 👋 무엇을 도와드릴까요?\n아래 키워드를 입력하시면 안내를 도와드립니다.\n👉 [위치], [영업시간], [가격]"
        
    else:
        # 2. 지정되지 않은 키워드가 입력되었을 때 기본 답변
        reply_text = f"죄송합니다. '{user_message}'에 대한 답변을 찾지 못했습니다. 😢\n\n'위치', '영업시간', '가격' 등 필요한 키워드를 정확히 입력해주세요!"

    # 3. LINE 메시지 전송 실행
    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.reply_message_with_http_info(
            ReplyMessageRequest(
                reply_token=event.reply_token,
                messages=[TextMessage(text=reply_text)]
            )
        )

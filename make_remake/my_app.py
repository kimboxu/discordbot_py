from os import environ
from flask import Flask, request, jsonify, g
from base import make_list_to_dict
from flask_cors import CORS
import asyncio
import threading
from supabase import create_client

app = Flask(__name__)
CORS(app)  # 크로스 오리진 요청 허용

def init_background_tasks():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    supabase = create_client(environ['supabase_url'], environ['supabase_key'])
    userStateData = supabase.table('userStateData').select("*").execute()
    userStateData = make_list_to_dict(userStateData.data)
    userStateData.index = userStateData["discordURL"]
    loop.close()
    return userStateData

@app.before_request
def initialize_app():
    app.userStateData = init_background_tasks()

@app.route('/', methods=['GET'])
def index():
    return jsonify({"message": "서버가 정상적으로 실행 중입니다."})

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        return jsonify({"message": "로그인 페이지입니다. POST로 요청해주세요."})
    
    # JSON 데이터 처리
    if request.is_json:
        data = request.get_json()
    else:
        # form-data 처리
        data = request.form
    
    username = data.get('username')
    discordWebhooksURL = data.get('discordWebhooksURL')
 
    if discordWebhooksURL in app.userStateData.index:
        db_username = app.userStateData.loc[discordWebhooksURL, "username"]
        check_have_id = True
    else: 
        check_have_id = False
    
    # 로그인 정보 출력 (디버깅용)
    print(f"로그인 시도: 사용자명: {username}, 디스코드 웹훅 URL: {discordWebhooksURL}")

    # 인증 로직
    if check_have_id and db_username == username:
        return jsonify({"status": "success", "message": "로그인 성공"})
    else:
        return jsonify({"status": "error", "message": "사용자명 또는 디스코드 웹훅 URL이 잘못되었습니다"}), 401

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'GET':
        return jsonify({"message": "회원가입 페이지입니다. POST로 요청해주세요."})
    
    # JSON 데이터 처리
    if request.is_json:
        data = request.get_json()
    else:
        # form-data 처리
        data = request.form
    
    username = data.get('username')
    discordWebhooksURL = data.get('discordWebhooksURL')
 
    if discordWebhooksURL in app.userStateData.index:
        check_have_id = "have_URL"

    elif not discordWebhooksURL.startswith(("https://discord.com/api/webhooks", "https://discordapp.com/api/webhooks/")):
        check_have_id = "not_discord_URL"

    elif discordWebhooksURL.startswith(("https://discord.com/api/webhooks/", "https://discordapp.com/api/webhooks/")):
        check_have_id = "OK"

    else: 
        check_have_id = "fail"
        

    # 로그인 정보 출력 (디버깅용)
    print(f"회원가입 시도: 사용자명: {username}, 디스코드 웹훅 URL: {discordWebhooksURL}")
    print(check_have_id)
    # 인증 로직
    if check_have_id == "OK":
        #DB에 유저 추가 하는 기능 함수 추가하기
        return jsonify({"status": "success", "message": "회원가입 성공"})
    elif check_have_id == "have_URL":
        return jsonify({"status": "error", "message": "디스코드 웹훅 URL이 이미 있습니다"}), 401
    elif check_have_id == "not_discord_URL":
        return jsonify({"status": "error", "message": "디스코드 웹훅 URL이 잘못되었습니다"}), 401
    else:
        return jsonify({"status": "error", "message": "사용자명 또는 디스코드 웹훅 URL이 잘못되었습니다"}), 401

if __name__ == '__main__':

    # If you want to initialize before the first request
    with app.app_context():
        app.init_var = init_background_tasks()
    
    app.run(host='0.0.0.0', port=5000, debug=True)
from os import environ
from flask import Flask, request, jsonify, render_template, g
from base import make_list_to_dict
from flask_cors import CORS
import asyncio
import threading
from json import loads
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

def save_user_data(discordWebhooksURL, username):
    supabase = create_client(environ['supabase_url'], environ['supabase_key'])
    supabase.table('userStateData').upsert({
        "discordURL": discordWebhooksURL, 
        "username": username,
    }).execute()

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
        # 사용자 알림 설정 조회
        user_data = app.userStateData.loc[discordWebhooksURL].to_dict() if isinstance(app.userStateData.loc[discordWebhooksURL], pd.Series) else app.userStateData.loc[discordWebhooksURL]
        
        # 기본 응답 데이터
        response_data = {
            "status": "success", 
            "message": "로그인 성공",
            "notification_settings": {
                "뱅온 알림": user_data.get("뱅온 알림", ""),
                "방제 변경 알림": user_data.get("방제 변경 알림", ""),
                "방종 알림": user_data.get("방종 알림", ""),
                "유튜브 알림": user_data.get("유튜브 알림", ""),
                "유튜브 알림": user_data.get("유튜브 알림", ""),
                "cafe_user_json": user_data.get("cafe_user_json", {}),
                "cafe_user_json": user_data.get("cafe_user_json", {})
            }
        }
        return jsonify(response_data)
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
        save_user_data(discordWebhooksURL, username)
        return jsonify({"status": "success", "message": "회원가입 성공"})
    elif check_have_id == "have_URL":
        return jsonify({"status": "error", "message": "디스코드 웹훅 URL이 이미 있습니다"}), 401
    elif check_have_id == "not_discord_URL":
        return jsonify({"status": "error", "message": "디스코드 웹훅 URL이 잘못되었습니다"}), 401
    else:
        return jsonify({"status": "error", "message": "사용자명 또는 디스코드 웹훅 URL이 잘못되었습니다"}), 401

@app.route('/get_user_settings', methods=['GET'])
def get_user_settings():
    discordWebhooksURL = request.args.get('discordWebhooksURL')
    username = request.args.get('username')
    
    if not discordWebhooksURL or not username:
        return jsonify({"status": "error", "message": "디스코드 웹훅 URL과 사용자명이 필요합니다"}), 400
    
    # 사용자 인증 확인
    if discordWebhooksURL in app.userStateData.index:
        db_username = app.userStateData.loc[discordWebhooksURL, "username"]
        if db_username != username:
            return jsonify({"status": "error", "message": "인증 실패"}), 401
    else:
        return jsonify({"status": "error", "message": "사용자를 찾을 수 없습니다"}), 404
    
    # Supabase에서 사용자 설정 가져오기
    supabase = create_client(environ['supabase_url'], environ['supabase_key'])
    result = supabase.table('userStateData').select('*').eq('discordURL', discordWebhooksURL).execute()
    
    if not result.data:
        return jsonify({"status": "error", "message": "설정을 찾을 수 없습니다"}), 404
    
    user_data = result.data[0]
    
    # 알림 설정 정보 추출
    settings = {
        "뱅온 알림": user_data.get("뱅온 알림", ""),
        "방제 변경 알림": user_data.get("방제 변경 알림", ""),
        "방종 알림": user_data.get("방종 알림", ""),
        "유튜브 알림": user_data.get("유튜브 알림", ""),
        "치지직 VOD": user_data.get("치지직 VOD", ""),
        "cafe_user_json": user_data.get("cafe_user_json", "{}"),
        "chat_user_json": user_data.get("chat_user_json", "{}")
    }
    settings["cafe_user_json"] = str(settings["cafe_user_json"])
    settings["chat_user_json"] = str(settings["chat_user_json"])

 
    return jsonify({"status": "success", "settings": settings})

@app.route('/save_user_settings', methods=['POST'])
def save_user_settings():
    # JSON 데이터 처리
    if request.is_json:
        data = request.get_json()
    else:
        # form-data 처리
        data = request.form
    
    discordWebhooksURL = data.get('discordWebhooksURL')
    username = data.get('username')
    
    if not discordWebhooksURL or not username:
        return jsonify({"status": "error", "message": "디스코드 웹훅 URL과 사용자명이 필요합니다"}), 400
    
    # 사용자 인증 확인
    if discordWebhooksURL in app.userStateData.index:
        db_username = app.userStateData.loc[discordWebhooksURL, "username"]
        if db_username != username:
            return jsonify({"status": "error", "message": "인증 실패"}), 401
    else:
        return jsonify({"status": "error", "message": "사용자를 찾을 수 없습니다"}), 404
    
    # 업데이트할 설정 데이터 추출
    update_data = {
        "discordURL": discordWebhooksURL,
        "username": username
    }
    
    # 선택적 설정 필드 추가
    for field in ["뱅온 알림", "방제 변경 알림", "방종 알림", "유튜브 알림", "치지직 VOD", "cafe_user_json", "chat_user_json"]:
        if field in data:
            update_data[field] = data.get(field)
    
    update_data["cafe_user_json"] = loads(update_data["cafe_user_json"].replace('\"', '"'))
    update_data["chat_user_json"] = loads(update_data["chat_user_json"].replace('\"', '"'))
    
    # Supabase에 설정 업데이트
    supabase = create_client(environ['supabase_url'], environ['supabase_key'])
    result = supabase.table('userStateData').upsert(update_data).execute()




    afreecaIDList = supabase.table('afreecaIDList').select("*").execute()
    chzzkIDList = supabase.table('chzzkIDList').select("*").execute()
    cafeData = supabase.table('cafeData').select("*").execute()

    afreecaIDList = make_list_to_dict(afreecaIDList.data)
    chzzkIDList = make_list_to_dict(chzzkIDList.data)
    cafeData = make_list_to_dict(cafeData.data)
    afreecaIDList.index = list(afreecaIDList["channelID"])
    chzzkIDList.index = list(chzzkIDList["channelID"])
    cafeData.index = list(cafeData["channelID"])
    

    channelName1 = afreecaIDList["channelName"]
    channelName2 = chzzkIDList["channelName"]
    channelName3 = cafeData["channelName"]
    profile_image1 = afreecaIDList["profile_image"]
    profile_image2 = chzzkIDList["profile_image"]
    cafeNameDict = cafeData["cafeNameDict"]
    print(channelName1)
    print(channelName2)


    
    


    return jsonify({"status": "success", "message": "설정이 저장되었습니다"})

@app.route('/get_streamers', methods=['GET'])
def get_streamers():
    try:
        # Supabase 연결
        supabase = create_client(environ['supabase_url'], environ['supabase_key'])
        
        # 아프리카TV 스트리머 정보 가져오기
        afreecaIDList = supabase.table('afreecaIDList').select("*").execute()
        
        # 치지직 스트리머 정보 가져오기
        chzzkIDList = supabase.table('chzzkIDList').select("*").execute()

        # 카페 정보 가져오기
        cafeData = supabase.table('cafeData').select("*").execute()

        
        return jsonify({
            "status": "success", 
            "afreecaStreamers": afreecaIDList.data,
            "chzzkStreamers": chzzkIDList.data,
            "cafeStreamers": cafeData.data,
        })
        
    except Exception as e:
        return jsonify({"status": "error", "message": f"스트리머 정보를 가져오는데 실패했습니다: {str(e)}"}), 500

if __name__ == '__main__':
    # If you want to initialize before the first request
    with app.app_context():
        # Need to import pandas at the top
        import pandas as pd
        app.init_var = init_background_tasks()

    app.run(host='0.0.0.0', port=5000, debug=True)
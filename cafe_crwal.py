# """
# [동적 크롤링을 이용하여 네이버 로그인 후 카페 게시글 가져오기]
# 자동입력방지 기능을 막기 위해 send_keys() 함수가 아닌  execute_script() 함수 사용.
# 로그인 버튼 태그와 선택자 > id : log.login
# 신규회원게시판 버튼 태그와 선택자 > id : menuLink90
# 첫 번째 게시물의 XPath > //*[@id="main-area"]/div[4]/table/tbody/tr[1]/td[1]/div[2]/div/a
# 게시물 내용의 태그와 선택자 > 태그 : div > class : se-module.se-module-text
# """
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
import time
from pprint import pprint
import datetime
import boto3
import os
from urllib import parse



cafe_list = [
    "gristmill",
    "molala",
    "jokddamo",
    "lamar10shi",
    "younribab",
    "chunhoe",
    "yangdoki",
    "haevelyn",
    "omonglens",
    "9nineho",
    "qooigloo",
    "owo0611",
    "zechupgook"
]
cafe_id = {
    "gristmill" : "30854332",
    "molala" : "30678599",
    "jokddamo": "30943421",
    "lamar10shi" : "30843952",
    "younribab" : "30945064",
    "chunhoe" : "30926426",
    "yangdoki" : "30947879",
    "haevelyn" : "30808342",
    "omonglens" : "30884052",
    "9nineho" : "30736552",
    "qooigloo" : "30723228",
    "owo0611" : "30909560",
    "zechupgook" : "30934090"
}

cafe_owner = {
    "gristmill" : "유봄냥",
    "molala" : "모라라",
    "jokddamo" : "족마신",
    "lamar10shi" : "라마",
    "younribab" : "연리",
    "chunhoe" : "계춘회",
    "yangdoki" : "양도끼",
    "haevelyn" : "해블린",
    "omonglens" : "이오몽",
    "9nineho" : "9호",
    "qooigloo" : "쿠우",
    "owo0611" : "단츄",
    "zechupgook" : "임재천"
}

search_word = [
]

translate_word = {
}

access_key = os.environ["access_key"]
secret_access_key = os.environ["secret_access_key"]
region = os.environ["region"]

session = boto3.Session(
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_access_key,
    region_name = region
)
client = session.client('dynamodb')

search_flag = False

def calculate_yesterday():
    month_31 = [1, 3, 5, 7, 8, 10, 12]
    month_30 = [4, 6, 9, 11]

    today = list(map(int, str(datetime.datetime.now()).split(' ')[0].split('-')))
    today[2] = int(today[2])
    today[2] -= 1
    
    if today[2] == 0:
        today[1] -= 1
        if today[1] == 2: # 어제가 2월이면
            if today[0] % 4 == 0: # 윤년이면
                today[2] = 29
            else:
                today[2] = 28
        elif today[1] in month_31:
            today[2] = 31
        else:
            today[2] = 30
    
    today[0] = str(today[0])
    today[1] = str(today[1])
    today[2] = str(today[2])
    
    if len(today[2]) < 2:
        today[2] = '0' + today[2]
    
    if len(today[1]) < 2:
        today[1] = '0' + today[1]
            
    return ".".join(str(x) for x in today) + '.'

# """네이버 로그인 페이지 접속"""
def crwal():
    print("데이터 수집을 시작합니다.")
    result = []
    options = ChromeOptions()  #자동화된 크롬창 실행
    options.add_argument("no-sandbox")
    options.add_argument('headless')
    options.add_argument("disable-gpu")
    options.add_argument("disable-dev-shm-usage")
    options.add_argument(f'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    service = ChromeService(executable_path=ChromeDriverManager().install())
    # service = ChromeService(excutable_path="./chromedriver")
    driver = webdriver.Chrome(service=service, options=options)
    
    # crawling
    url = 'https://nid.naver.com/nidlogin.login'
    id = os.environ["naver_id"]
    pw = os.environ["naver_password"]
    driver.get(url)

    driver.implicitly_wait(2)

    driver.execute_script("document.getElementsByName('id')[0].value=\'" + id + "\'")
    driver.execute_script("document.getElementsByName('pw')[0].value=\'" + pw + "\'")
    
    # 로그인 버튼 클릭
    driver.find_element(By.XPATH, '//*[@id="log.login"]').click()
    time.sleep(1)
        
    for cafe_name in cafe_list:
        for word in search_word:
            print(cafe_owner[cafe_name] + " 데이터 수집을 시작합니다. 검색어 : " + translate_word[word])
            # "콥"
            query_str = "?iframe_url=/{0}/ArticleSearchList.nhn%3Fsearch.clubid={1}%26search.searchBy=0%26search.query={2}".format(cafe_name, cafe_id[cafe_name], word)
            comu_url = "https://cafe.naver.com/{0}".format(cafe_name)
            driver.get(comu_url + query_str)
            driver.implicitly_wait(3)
            driver.switch_to.frame('cafe_main')
            elements = driver.find_elements(By.CLASS_NAME, "inner_number")
            dates = driver.find_elements(By.CLASS_NAME, "td_date")
            titles = driver.find_elements(By.CLASS_NAME, "article")
            driver.implicitly_wait(3)
            yesterday = calculate_yesterday()
            article_count = 0
            
            # for title in titles:
            #     print(title.text)
            
            for date in dates:
                # print(date.text)
                if len(date.text) < 11:
                    article_count += 1
                elif date.text == yesterday:
                    article_count += 1
                    
            for idx, e in enumerate(elements):
                if idx < article_count:
                    result.append([ '[' + cafe_owner[cafe_name] + ']'+ ' ' + titles[idx].text, comu_url + '/' + e.text])
                
    return sorted(result)

def solution(searching_word : list):
    try_count = 5
    words = []
    decode_search_word = {}
    global search_word
    global translate_word
    global search_flag
    if len(searching_word) > 0:
        print("검색어 입력을 감지했습니다.")
        search_flag = True
        for word in searching_word:
            t = parse.quote(word.encode(encoding='euc-kr'))
            words.append(t)
            decode_search_word[t] = word
        search_word = words
        translate_word = decode_search_word
        print(words)
        print(translate_word)
    for i in range(6):
        try:
            res = crwal()
        except:
            if i < try_count:
                print("재시도 중.. {}번째 시도 중".format(i+1))
                time.sleep(3)
                continue
            else:
                print("재시도 5회 끝")
                return "ERROR"
        break
    return sorted(res)

if __name__ == "__main__":
    solution(["9호"])
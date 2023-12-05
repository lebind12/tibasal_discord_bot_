import discord
from discord.ext import commands
import requests
from db_modify import get_data
import boto3
from botocore.config import Config
import functools
import typing
import asyncio
import os
import json
from bs4 import BeautifulSoup
from pprint import pprint
from datetime import datetime
import time
import pymysql
import traceback

# ENV Loader
from dotenv import load_dotenv
load_dotenv()

access_key = os.environ["access_key"]
secret_access_key = os.environ["secret_access_key"]
region = os.environ["region"]

intents = discord.Intents.default()
intents.message_content = True


def db_connect():
    # MariaDB_Connect
    db_env = {
        'user': os.environ["user"],
        'password': os.environ["password"],
        'host': os.environ["host"],
        'port': os.environ["port"],
        'database': os.environ["database"]
    }
    connect = pymysql.connect(host=db_env['host'],
                          user=db_env['user'],
                          password=db_env['password'],
                          db=db_env['database'],
                          charset='utf8',
                          use_unicode=True)
    return connect

session = boto3.Session(
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_access_key,
    region_name = region
)
my_config = Config(
    connect_timeout=900, read_timeout=900,
    retries={
        'max_attempts': 1,
        'mode': 'standard'
    } 
)


bot = commands.Bot(command_prefix='!', intents=intents)

def to_thread(func: typing.Callable):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        wrapped = functools.partial(func, *args, **kwargs)
        return await loop.run_in_executor(None, wrapped)
    return wrapper

@to_thread
def get_search_result(search_words):
    client = session.client('lambda', config=my_config)
    payload = {"words": search_words}
    res = client.invoke(
        FunctionName='nonoChalice',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )

    return json.loads(res['Payload'].read())['result']

@bot.command(name="어제", brief='어제 소식 가져오기', description="어제 소식을 가져옵니다.")
async def on_message(message):
    user = str(message.author)
    if user == "mm9372" or user == "eaglekop":
        await message.channel.send("어제 이후의 멤버들의 카페소식을 가져옵니다.", silent=True)
        res = get_data()
        if len(res) == 0:
            await message.channel.send("어제자 이후로 생성된 글이 없습니다.", silent=True)
        for r in res:
            name, url = r
            await message.channel.send(name, silent=True)
            await message.channel.send(url, silent=True)
        await message.channel.send("끝.", silent=True)
    
@bot.command(name="검색", brief='까페 검색', description="멤버들 까페에 검색 후 검색데이터를 가져옵니다.\n5분 내외의 시간이 걸릴 수 있습니다.")
async def search(ctx, *args):
    res = None
    await ctx.send("검색을 시작합니다. 검색에는 최대 5분이 걸립니다.", silent=True)
    print(', '.join(args))
    user = str(ctx.author)
    if len(args) == 0:
        await ctx.send("검색어가 없습니다.", silent=True)
    
    if user == "mm9372" or user == "eaglekop":
        for word in args:
            await ctx.send('[' + word + ']' +" 의 검색 중입니다.", silent=True)
            res = await get_search_result([word])
            print(res)
            await ctx.send('[' + word + ']' +" 의 검색 결과입니다.", silent=True)
            if res == "ERROR":
                await ctx.send("검색 중 에러가 발생했습니다.", silent=True)
            if len(res) == 0:
                await ctx.send('[' + word + ']' + "검색결과가 없습니다.", silent=True)
            for r in res:
                name, url = r
                await ctx.send(name, silent=True)
                await ctx.send(url, silent=True)
            await ctx.send('[' + word + '] 의 검색 결과는 여기까지입니다.', silent=True)
        await ctx.send("진짜 끝.", silent=True)

# 클립채널 메세지 인식 후 
@bot.event
async def on_message(ctx, *args):
    target_channel = bot.get_channel(1151373694040539167)
    connect = db_connect()
    cursor = connect.cursor()
    if ctx.channel == target_channel:
        # get latest_idx
        idx = int(json.loads(requests.get("https://axlgzm3y97.execute-api.ap-northeast-2.amazonaws.com/api/v1/latest").text)['result']) + 1
        # print(idx)
        # print(ctx.content)
        sentences = ctx.content.split('\n')
        for sentence in sentences:
            if sentence.startswith("https://youtube.com/clip/"):
                try:
                    print(sentence, ctx.created_at)
                    print(ctx.created_at.strftime("%Y%m%d"))
                    arguments = html_parsing(idx, sentence, ctx)
                    sql = "INSERT INTO kopflix "\
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, arguments)
                    idx += 1
                except Exception as e:
                    error_message = traceback.format_exc()
                    print(error_message)
    connect.commit()
    connect.close()

# 메세지 삭제시
@bot.event
async def on_message_delete(ctx, *args):
    target_channel = bot.get_channel(1151373694040539167)
    connect = db_connect()
    cursor = connect.cursor()
    if ctx.channel == target_channel:
        sentences = ctx.content.split('\n')
        for sentence in sentences:
            print(sentence)
            if sentence.startswith("https://youtube.com/clip/"):
                sentence = sentence.rstrip()
                sql = f"delete from kopflix where clip_url=\"{sentence}\" order by idx limit 1"
                cursor.execute(sql)
    connect.commit()
    connect.close()

# 메세지 변경시
@bot.event
async def on_message_edit(before, after):
    if before.content != after.content:
        target_channel = bot.get_channel(1151373694040539167)
        if target_channel == before.channel:
            connect = db_connect()
            cursor = connect.cursor()
            
            before_sentences = before.content.split('\n')
            after_sentences = after.content.split('\n')
            for i, sentence in enumerate(before_sentences):
                if i >= len(after_sentences):
                    break
                if sentence.startswith("https://youtube.com/clip/"):
                    # find before data
                    sql = f"SELECT idx FROM kopflix WHERE clip_url=\"{sentence}\" order by idx desc limit 1"
                    cursor.execute(sql)
                    res = cursor.fetchall()
                    # get before idx
                    idx = res[0][0] # return ((idx, ), )
                    if len(res) != 0:
                        idx = res[0][0]
                        arguments = html_parsing(idx, after_sentences[i], after)
                        sql = "UPDATE kopflix SET "\
                            "idx=%s, "\
                            "category=%s, "\
                            "original_image=%s, "\
                            "original_title=%s, "\
                            "title=%s, "\
                            "upload_date=%s, "\
                            "upload_unix_time=%s, "\
                            "url=%s, "\
                            "clip_url=%s "
                        sql = sql + f"WHERE idx={idx}"
                        cursor.execute(sql, arguments)
            # remove if before size lt after size
            for k in range(i, len(before_sentences)):
                sentence = before_sentences[k]
                if sentence.startswith("https://youtube.com/clip/"):
                    sentence = sentence.rstrip()
                    sql = f"delete from kopflix where clip_url=\"{sentence}\" order by idx limit 1"
                    cursor.execute(sql)
            connect.commit()
            connect.close()
    
                
def html_parsing(idx, sentence, message):
    r = requests.get(url=sentence)
    soup = BeautifulSoup(r.text, "html.parser")
    clip_url = sentence
    url = str(soup.select_one('meta[property="og:video:url"]')['content'])
    title = str(soup.select_one('meta[property="og:title"]')['content'])
    upload_date= message.created_at.strftime("%Y%m%d")
    original_title = str(soup.select_one('meta[property="og:description"]')['content']).split('Original video')[1]
    original_image = str(soup.select_one('meta[property="og:image"]')['content'])
    unix_time_stamp = int(time.mktime(message.created_at.timetuple()))
    category = "youtube_clip"
    
    arguments = (idx, category, original_image, original_title, \
                title, upload_date, unix_time_stamp, \
                url, clip_url)
    return arguments
    
        
# # 클립 채널 크롤링 코드
# @bot.command(name="crawl")
# async def on_message(ctx, *args):
#     '''
#     💖이글콥_클립저장소💖 : channel=<TextChannel id=1151373694040539167 name='💖이글콥_클립저장소💖' position=10 nsfw=False news=False category_id=1060887429302718536> 
#     ㅎㅎ : channel=<TextChannel id=1114850582868529172 name='ㅎㅎ' position=2 nsfw=False news=False category_id=701003030979411968>
#     '''
#     channel = bot.get_channel(1151373694040539167)
#     # idx = 0
#     idx = int(json.loads(requests.get("https://axlgzm3y97.execute-api.ap-northeast-2.amazonaws.com/api/v1/latest").text)['result']) + 1
#     print(ctx, idx)
#     async for message in channel.history(limit= 500000, oldest_first=True, after=datetime(2023, 12, 6, 4, 0, 0)):
#         # messages.append([message.content, message.created_at])
#         sentences = message.content.split('\n')
#         for sentence in sentences:
#             if sentence.startswith("https://youtube.com/clip/"):
#                 try:
#                     connect = db_connect()
#                     cursor = connect.cursor()
#                     print(sentence, message.created_at)
#                     print(message.created_at.strftime("%Y%m%d"))
#                     arguments = html_parsing(idx, sentence, message)
#                     print(arguments)
#                     sql = "INSERT INTO kopflix "\
#                         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
#                     cursor.execute(sql, arguments)
#                     connect.commit()
#                     connect.close()
#                     print(idx)
#                     idx += 1
#                 except Exception as e:
#                     print(e)
#                     pass

    
    
        
bot.run(os.environ["discord_token"])
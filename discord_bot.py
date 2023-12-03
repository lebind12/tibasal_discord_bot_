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



intents = discord.Intents.default()
intents.message_content = True

access_key = os.environ["access_key"]
secret_access_key = os.environ["secret_access_key"]
region = os.environ["region"]


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

client = boto3.client('dynamodb',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_access_key,
    region_name = region
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

# 클립채널 메세지 인식 후 DB 업로드
@bot.event
async def on_message(ctx, *args):
    target_channel = bot.get_channel(1151373694040539167)
    if ctx.channel == target_channel:
        # get latest_idx
        idx = int(json.loads(requests.get("https://nh6one1oj3.execute-api.ap-northeast-2.amazonaws.com/api/v1/latest").text)['result']) + 1
        print(idx)
        print(ctx.content)
        database_name = "kopflix_db"
        sentences = ctx.content.split('\n')
        for sentence in sentences:
            if sentence.startswith("https://youtube.com/clip/"):
                try:
                    print(sentence, ctx.created_at)
                    print(ctx.created_at.strftime("%Y%m%d"))
                    r = requests.get(url=sentence)
                    soup = BeautifulSoup(r.text, "html.parser")
                    url = str(soup.select_one('meta[property="og:video:url"]')['content'])
                    title = str(soup.select_one('meta[property="og:title"]')['content'])
                    upload_date= ctx.created_at.strftime("%Y%m%d")
                    original_title = str(soup.select_one('meta[property="og:description"]')['content']).split('Original video')[1]
                    original_image = str(soup.select_one('meta[property="og:image"]')['content'])
                    unix_time_stamp = int(time.mktime(ctx.created_at.timetuple()))
                    res = client.put_item(
                        TableName=database_name,
                        Item={
                            'category': {
                                'S' : 'youtube_clip'
                            },
                            'idx': {
                                'N' : str(idx)
                            },
                            'url' : {
                                'S' : url
                            },
                            'title' : {
                                'S' : title
                            },
                            'upload_date' : {
                                'S' : upload_date
                            },
                            'upload_unix_time' : {
                                'N' : str(unix_time_stamp)
                            },
                            'original_title' : {
                                'S' : original_title
                            },
                            'original_image' : {
                                'S' : original_image
                            }
                        }
                    )
                    print(res)
                    idx += 1
                except:
                    pass
        
# 클립 채널 크롤링 코드
# @bot.command(name="crawl")
# async def on_message(ctx, *args):
#     '''
#     💖이글콥_클립저장소💖 : channel=<TextChannel id=1151373694040539167 name='💖이글콥_클립저장소💖' position=10 nsfw=False news=False category_id=1060887429302718536> 
#     ㅎㅎ : channel=<TextChannel id=1114850582868529172 name='ㅎㅎ' position=2 nsfw=False news=False category_id=701003030979411968>
#     '''
#     channel = bot.get_channel(1151373694040539167)
#     # channel = bot.get_channel(1114850582868529172)
#     # await channel.send("test")
#     database_name = "kopflix_db"
#     idx = json.loads(requests.get("https://nh6one1oj3.execute-api.ap-northeast-2.amazonaws.com/api/v1/latest").text)['result'] + 1
#     async for message in channel.history(limit= 3000, oldest_first=True, after=datetime(2023, 11, 27)):
#         # messages.append([message.content, message.created_at])
#         sentences = message.content.split('\n')
#         for sentence in sentences:
#             if sentence.startswith("https://youtube.com/clip/"):
#                 try:
#                     print(sentence, message.created_at)
#                     print(message.created_at.strftime("%Y%m%d"))
#                     r = requests.get(url=sentence)
#                     soup = BeautifulSoup(r.text, "html.parser")
#                     url = str(soup.select_one('meta[property="og:video:url"]')['content'])
#                     title = str(soup.select_one('meta[property="og:title"]')['content'])
#                     upload_date= message.created_at.strftime("%Y%m%d")
#                     original_title = str(soup.select_one('meta[property="og:description"]')['content']).split('Original video')[1]
#                     original_image = str(soup.select_one('meta[property="og:image"]')['content'])
#                     unix_time_stamp = int(time.mktime(message.created_at.timetuple()))
#                     res = client.put_item(
#                         TableName=database_name,
#                         Item={
#                             'category': {
#                                 'S' : 'youtube_clip'
#                             },
#                             'idx': {
#                                 'N' : str(idx)
#                             },
#                             'url' : {
#                                 'S' : url
#                             },
#                             'title' : {
#                                 'S' : title
#                             },
#                             'upload_date' : {
#                                 'S' : upload_date
#                             },
#                             'upload_unix_time' : {
#                                 'N' : str(unix_time_stamp)
#                             },
#                             'original_title' : {
#                                 'S' : original_title
#                             },
#                             'original_image' : {
#                                 'S' : original_image
#                             }
#                         }
#                     )
#                     print(res)
#                     idx += 1
#                 except:
#                     pass
                
#     # print(messages)
    
    
        
bot.run(os.environ["discord_token"])
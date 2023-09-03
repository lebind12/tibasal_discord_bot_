import discord
from discord.ext import commands
from db_modify import get_data
import boto3
from botocore.config import Config
import functools
import typing
import asyncio
import os
import json

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
bot.run(os.environ["discord_token"])
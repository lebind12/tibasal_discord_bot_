import discord
from db_modify import get_data
import os

class MyClient(discord.Client):
    async def on_ready(self):
        print(f'Logged on as {self.user}!')

    async def on_message(self, message):
        print(message.channel.name)
        print(f'Message from {message.author}: {message.content}')
        command = message.content
        if command == "Bot command : 어제 소식 가져오기" and message.channel.name == "ㅎㅎ":
            await message.channel.send("어제 이후의 멤버들의 카페소식을 가져옵니다.")
            res = get_data()
            if len(res) == 0:
                await message.channel.send("어제자 이후로 생성된 글이 없습니다.")
            for r in res:
                name, url = r
                await message.channel.send(name)
                await message.channel.send(url)
            await message.channel.send("끝.")
        
        
intents = discord.Intents.default()
intents.message_content = True

client = MyClient(intents=intents)
client.run(os.environ["discord_token"])
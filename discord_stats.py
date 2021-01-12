import discord
from discord.ext import commands
from discord import File
import random
import os
import time
import pandas as pd
from collections import Counter
from emoji import UNICODE_EMOJI
import re

client = commands.Bot(command_prefix='!')

@client.event
async def on_ready():
    print('Logged on as {0}!'.format(client.user))

@client.event
async def on_message(message):
    idx_di = message.content.find('di') 
    if idx_di != -1:
        await message.channel.send(message.content[idx_di+2:])

    await client.process_commands(message)

@client.command()
@commands.is_owner()
async def shutdown(ctx):
    await ctx.channel.send("Je vais dodo, tu devrais faire pareil.\nLa bise.")
    await ctx.bot.logout()

@client.command(
    name="update",
    help="Récupère les logs de tout les channels",
	brief="Récupère les logs de tout les channels",
    pass_context = True
)
async def update(ctx):
    for chan in ctx.guild.text_channels:
        await log_channel(ctx, chan)
    

@client.command(
    name="ping",
	help="Il faut vraiment avoir un QI négatif pour ne pas savoir ce que fait la commande ping...",
	brief="Print pong"
)
async def ping(ctx):
    await ctx.channel.send("pong")

@client.command(
    name="pong",
	help="Il faut vraiment avoir un QI négatif pour ne pas savoir ce que fait la commande pong...",
	brief="Print ping"
)
async def pong(ctx):
    await ctx.channel.send("ping")


async def log_channel(ctx, channel):
    logFile = './data/{}.csv'.format(channel.name)
    if not os.path.exists(logFile):
        with open(logFile, 'w', encoding='UTF-8') as f:
            f.write('created_at,author,id,channel,content,reactions\n')
        message = None
    else:
        last_id = pd.read_csv(logFile, index_col=False)["id"].values[-1]
        message = await channel.fetch_message(int(last_id))

    with open(logFile, 'a', encoding='UTF-8') as f:
        await log_channel_worker(channel, f, message)

    #await ctx.send(f"logs {channel.name}:", file=File(logFile))
    await ctx.send(f"{channel.name} updated!")

@client.command(
    name="mass_stats",
	help="Stats sur le nombre de message, emoji et réactions",
	brief="Voilà les gross stats samèr"
)
async def send_stats(ctx, nb=10):
    to_send = mass_stats(nb)
    await partial_send(ctx, to_send)

async def partial_send(ctx, to_send):
    to_send = to_send.split("\n")
    for i in range(0, len(to_send), 10):
        current = "\n".join(to_send[i:i+10]) + "\n"
        await ctx.channel.send(current)

async def log_channel_worker(channel, f, message):
    kwargs = {"oldest_first":True, "limit":None}
    if message is not None:
        kwargs["after"] = message
    
    async for m in channel.history(**kwargs):
        date = m.created_at.strftime('%d.%m.%Y %H:%M:%S')
        txt = m.clean_content.replace("\n", "__").replace(",", " virgule ").replace(";", " pointvirgule ")

        f.write(f'{date},{m.author},{m.id},{m.channel.name},{txt},')
        for r in m.reactions:
            async for u in r.users():
                f.write(f'{r.emoji};{u.name}#{u.discriminator};')
        f.write('\n')

def only_emoji(txt):
    return ''.join([c for c in txt if c in UNICODE_EMOJI])

def get_custom_counter(li):
    return Counter(re.findall(r"<:\w*:\d*>", "\n".join(li)))

def get_all_logs(root="./data/"):
    all_df = [pd.read_csv(root + f, index_col=False) for f in os.listdir(root)]
    df = pd.concat(all_df).fillna("")
    df = df[df.author != "el famoso boto#2234"]
    return df

def mass_stats(nb):
    df = get_all_logs()
    authors = [("@"+k.split("#")[0], len(v)) for k, v in df.groupby(["author"])]
    top10_authors = get_top(nb, authors, f"Top {nb} writers:")
    
    channels = [(k, len(v)) for k, v in df.groupby(["channel"])]
    top10_channels = get_top(nb, channels, f"Top {nb} channels:")
    
    emoji = [only_emoji(line) for line in df["content"].values]

    emoji_counter = Counter(''.join(emoji)) + get_custom_counter(df["content"])
    top10_emoji = get_top(nb, list(emoji_counter.items()), f"Top {nb} emojis:")

    react = [only_emoji(line) for line in df["reactions"].values]
    react_counter = Counter(''.join(react)) + get_custom_counter(df["reactions"])
    top10_react = get_top(nb, list(react_counter.items()), f"Top {nb} reactions:")

    react_amoji_counter = react_counter + emoji_counter
    top10_react_emoji = get_top(nb, list(react_amoji_counter.items()), f"Top {nb} emoji+reactions:")

    return "\n".join([top10_authors, top10_channels, top10_emoji, top10_react, top10_react_emoji]) 

@client.command(
    name="custom_stats",
	help="Stats sur le nombre de message, emoji et réactions",
	brief="Voilà les gross stats samèr"
)
async def custom_stats(ctx):
    df = get_all_logs()
    emoji_counter = get_custom_counter(df["content"])
    react_counter = get_custom_counter(df["reactions"])
    react_emoji_counter = emoji_counter + react_counter
    top_custom_emoji = get_top("all", list(emoji_counter.items()), "Custom emojis:")
    top_custom_react = get_top("all", list(react_counter.items()), "Custom reactions:")
    top_react_emoji = get_top("all", list(react_emoji_counter.items()), "Custom emojis+reactions:")

    to_send = "\n".join([top_custom_emoji, top_custom_react, top_react_emoji])
    await partial_send(ctx, to_send)

@client.command(
    name="best_quotes",
	help="Best messages",
	brief="Best messages samèr"
)
async def best_quotes(ctx, nb=None):
    if nb is None:
        nb = 10
    if type(nb) == str:
        nb = int(nb)
    df = get_all_logs()
    df = df[df.reactions != ""]
    quotes = zip(df["author"].values, df["content"].values)
    quotes = [("@" + author.split("#")[0], content.replace("__","\n").replace(" virgule ", ",").replace(" pointvirgule ", ";")) for author, content in quotes]
    quotes = [" - ".join([author, content]) for author, content in quotes]

    react = [Counter(only_emoji(line)) + get_custom_counter(line) for line in df["reactions"].values]
    best_content = zip(quotes, [sum(c.values()) for c in react])
    top10_channels = get_top(nb, list(best_content), f"Top {nb} message (by number of reactions):")

    
    people_reacting = [get_people_reacting(line) for line in df["reactions"].values]
    best_by_people = zip(quotes, [len(c.values()) for c in people_reacting])
    top10_by_people = get_top(nb, list(best_by_people), f"Top {nb} message (by unique people reacting):")

    await partial_send(ctx, top10_channels)
    await partial_send(ctx, top10_by_people)

def get_people_reacting(txt):
    found = re.findall(r"(\w+ \w+#\d+)|(\w+#\d+)", txt)
    found = [d1 if len(d1) > len(d2) else d2 for d1, d2 in found]
    return Counter(found)

def get_top(nb, li, title) -> str:
    s = title + "\n"
    top10 = sorted(li, key=lambda item: item[1], reverse=True)
    if nb != "all":
        top10 = top10[:nb]
    for (i, (t, v)) in enumerate(top10):
        s += f"\t{i+1}. {t} ({v})\n"
    return s

#mass_stats()


client.run(open("token.scord", 'r').read())
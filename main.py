import discord
from discord.ext import commands
import sqlite3
import json
from dotenv import load_dotenv
import os
import asyncio

# Load environment variables
load_dotenv()

# Bot configuration
TOKEN = os.getenv('DISCORD_TOKEN')
COMMAND_PREFIX = os.getenv('COMMAND_PREFIX', '!')

# Initialize the bot
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix=COMMAND_PREFIX, intents=intents)

# Database setup
DB_NAME = 'messages.db'

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS messages
                 (id INTEGER PRIMARY KEY,
                  message_id TEXT UNIQUE,
                  content TEXT,
                  author_id TEXT,
                  author_name TEXT,
                  channel_id TEXT,
                  channel_name TEXT,
                  guild_id TEXT,
                  guild_name TEXT,
                  created_at TEXT,
                  edited_at TEXT,
                  attachments TEXT,
                  embeds TEXT,
                  reactions TEXT,
                  mentions TEXT,
                  channel_mentions TEXT,
                  role_mentions TEXT,
                  reference_id TEXT,
                  jump_url TEXT)''')
    conn.commit()
    conn.close()

@bot.event
async def on_ready():
    print(f'{bot.user} has connected to Discord!')
    init_db()

def store_message(message):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    attachments = json.dumps([{
        'id': a.id,
        'filename': a.filename,
        'url': a.url
    } for a in message.attachments])
    
    embeds = json.dumps([e.to_dict() for e in message.embeds])
    
    reactions = json.dumps({str(r.emoji): r.count for r in message.reactions})
    
    mentions = json.dumps([user.id for user in message.mentions])
    channel_mentions = json.dumps([ch.id for ch in message.channel_mentions])
    role_mentions = json.dumps([role.id for role in message.role_mentions])
    
    c.execute('''INSERT OR REPLACE INTO messages
                 (message_id, content, author_id, author_name, channel_id, channel_name,
                  guild_id, guild_name, created_at, edited_at, attachments, embeds,
                  reactions, mentions, channel_mentions, role_mentions, reference_id, jump_url)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (str(message.id), message.content, str(message.author.id), message.author.name,
                 str(message.channel.id), message.channel.name, 
                 str(message.guild.id) if message.guild else None,
                 message.guild.name if message.guild else None,
                 str(message.created_at), str(message.edited_at) if message.edited_at else None,
                 attachments, embeds, reactions, mentions, channel_mentions, role_mentions,
                 str(message.reference.message_id) if message.reference else None,
                 message.jump_url))
    
    conn.commit()
    conn.close()

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return

    store_message(message)
    await bot.process_commands(message)

@bot.command(name='lastmessages')
async def last_messages(ctx, limit: int = 5):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''SELECT content, author_name, channel_name, created_at
                 FROM messages ORDER BY id DESC LIMIT ?''', (limit,))
    messages = c.fetchall()
    conn.close()

    response = f"Last {limit} messages:\n"
    for msg in messages:
        response += f"{msg[1]} in {msg[2]} at {msg[3]}: {msg[0]}\n"
    
    await ctx.send(response)

async def scrape_channel_history(channel):
    try:
        message_count = 0
        async for message in channel.history(limit=None):
            store_message(message)
            message_count += 1
            if message_count % 100 == 0:
                await asyncio.sleep(1)  # To avoid rate limiting
        return message_count
    except Exception as e:
        print(f"Error scraping channel {channel.name}: {e}")
        return 0

@bot.command(name='scrape_history')
@commands.has_permissions(administrator=True)
async def scrape_history(ctx, channel: discord.TextChannel = None):
    if channel:
        channels = [channel]
    else:
        channels = ctx.guild.text_channels

    total_messages = 0
    for channel in channels:
        await ctx.send(f"Scraping messages from {channel.name}...")
        message_count = await scrape_channel_history(channel)
        total_messages += message_count
        await ctx.send(f"Scraped {message_count} messages from {channel.name}")

    await ctx.send(f"Finished scraping. Total messages scraped: {total_messages}")

# Modify the store_message function to handle potential errors
def store_message(message):
    try:
        conn = sqlite3.connect(DB_NAME)
        c = conn.cursor()
        
        attachments = json.dumps([{
            'id': a.id,
            'filename': a.filename,
            'url': a.url
        } for a in message.attachments])
        
        embeds = json.dumps([e.to_dict() for e in message.embeds])
        
        reactions = json.dumps({str(r.emoji): r.count for r in message.reactions})
        
        mentions = json.dumps([user.id for user in message.mentions])
        channel_mentions = json.dumps([ch.id for ch in message.channel_mentions])
        role_mentions = json.dumps([role.id for role in message.role_mentions])
        
        c.execute('''INSERT OR REPLACE INTO messages
                     (message_id, content, author_id, author_name, channel_id, channel_name,
                      guild_id, guild_name, created_at, edited_at, attachments, embeds,
                      reactions, mentions, channel_mentions, role_mentions, reference_id, jump_url)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (str(message.id), message.content, str(message.author.id), message.author.name,
                     str(message.channel.id), message.channel.name, 
                     str(message.guild.id) if message.guild else None,
                     message.guild.name if message.guild else None,
                     str(message.created_at), str(message.edited_at) if message.edited_at else None,
                     attachments, embeds, reactions, mentions, channel_mentions, role_mentions,
                     str(message.reference.message_id) if message.reference else None,
                     message.jump_url))
        
        conn.commit()
    except Exception as e:
        print(f"Error storing message {message.id}: {e}")
    finally:
        conn.close()

# Run the bot
if __name__ == "__main__":
    bot.run(TOKEN)
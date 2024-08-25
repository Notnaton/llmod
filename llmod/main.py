import os
from dotenv import load_dotenv
import discord
from discord.ext import commands
import sqlite3

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

# Assuming bot is already set up as before
async def scrape_channel_history(channel):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()

    try:
        messages = []
        async for message in channel.history(limit=None):
            messages.append((
                message.content,
                str(message.author),
                str(message.channel),
                str(message.created_at)
            ))
            
            # Insert in batches of 100 for efficiency
            if len(messages) >= 100:
                c.executemany("""
                    INSERT OR IGNORE INTO messages (content, author, channel, timestamp)
                    VALUES (?, ?, ?, ?)
                """, messages)
                conn.commit()
                messages = []

        # Insert any remaining messages
        if messages:
            c.executemany("""
                INSERT OR IGNORE INTO messages (content, author, channel, timestamp)
                VALUES (?, ?, ?, ?)
            """, messages)
            conn.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

@bot.command(name='scrape_history')
@commands.has_permissions(administrator=True)
async def scrape_history(ctx):
    await ctx.send("Starting to scrape message history. This may take a while...")
    
    for channel in ctx.guild.text_channels:
        await ctx.send(f"Scraping messages from {channel.name}...")
        await scrape_channel_history(channel)
    
    await ctx.send("Finished scraping message history.")

# Add this to your on_ready event or wherever you initialize your bot
async def setup_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS messages
                 (id INTEGER PRIMARY KEY,
                  content TEXT,
                  author TEXT,
                  channel TEXT,
                  timestamp TEXT,
                  UNIQUE(author, channel, timestamp))''')
    conn.commit()
    conn.close()

@bot.event
async def on_ready():
    #print(f'{bot.user} has connected to Discord!')
    await setup_db()

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return

    # Store message in the database
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("INSERT INTO messages (content, author, channel, timestamp) VALUES (?, ?, ?, ?)",
              (message.content, str(message.author), str(message.channel), str(message.created_at)))
    conn.commit()
    conn.close()

    await bot.process_commands(message)

@bot.command(name='lastmessages')
async def last_messages(ctx, limit: int = 5):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("SELECT content, author, channel, timestamp FROM messages ORDER BY id DESC LIMIT ?", (limit,))
    messages = c.fetchall()
    conn.close()

    response = f"Last {limit} messages:\n"
    for msg in messages:
        response += f"{msg[1]} in {msg[2]} at {msg[3]}: {msg[0]}\n"
    
    await ctx.send(response)

# Run the bot
if __name__ == "__main__":
    bot.run(TOKEN)
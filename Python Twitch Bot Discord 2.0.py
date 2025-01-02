import logging
import requests 
import sys
import os
import aiohttp
import discord
import psutil
from discord.ext import commands
from discord import app_commands
import aiosqlite
import asyncio
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, Any
from dataclasses import dataclass

@dataclass
class Stream:
    title: str
    streamer: str
    game: str
    thumbnail_url: str
    started_at: datetime

class TwitchAPI:
    def __init__(self):
        self.base_url = "https://api.twitch.tv/helix/streams"
        self.client_id = os.getenv('TWITCH_CLIENT_ID')
        self.client_secret = os.getenv('TWITCH_CLIENT_SECRET')
        self.token = None

    def _get_auth_token(self):
        if self.token:
            return self.token
            
        auth_url = "https://id.twitch.tv/oauth2/token"
        params = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials'
        }
        
        response = requests.post(auth_url, params=params)
        if response.status_code == 200:
            self.token = response.json().get('access_token')
            return self.token
        else:
            raise Exception(f"Failed to get auth token: {response.status_code}")

    def check_if_live(self, channel):
        try:
            token = self._get_auth_token()
            headers = {
                'Client-ID': self.client_id,
                'Authorization': f'Bearer {token}'
            }
            
            params = {'user_login': channel}
            logger.info(f"Making API request to: {self.base_url}")
            response = requests.get(self.base_url, headers=headers, params=params)
            logger.info(f"API response status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"API response data: {data}")
                
                if data.get('data'):
                    stream_data = data['data'][0]
                    return Stream(
                        title=stream_data.get('title', ''),
                        streamer=stream_data.get('user_name', ''),
                        game=stream_data.get('game_name', ''),
                        thumbnail_url=stream_data.get('thumbnail_url', '').replace('{width}', '320').replace('{height}', '180'),
                        started_at=datetime.strptime(stream_data['started_at'], '%Y-%m-%dT%H:%M:%SZ')
                    )
                else:
                    logger.info("Stream is not live")
            else:
                logger.error(f"API request failed with status: {response.status_code}")
                
            return None
        except Exception as e:
            logger.error(f"Error checking stream status: {str(e)}")
            raise Exception(f"Error checking stream status: {str(e)}")

twitch_api = TwitchAPI()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Configuration
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
TWITCH_CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
TWITCH_CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')
NOTIFICATION_CHANNEL_ID = int(os.getenv('NOTIFICATION_CHANNEL_ID'))
PREMIUM_NOTIFICATION_CHANNEL_ID = int(os.getenv('PREMIUM_NOTIFICATION_CHANNEL_ID'))

# Initialize Twitch API
twitch_api = TwitchAPI()

# Constants
TWITCH_API_URL = 'https://api.twitch.tv/helix'
TWITCH_OAUTH_URL = 'https://id.twitch.tv/oauth2/token'
API_RETRY_COUNT = 3
API_RETRY_DELAY = 2
TOKEN_CACHE_DURATION = 3600  # 1 hour in seconds
MAX_API_CALLS_PER_MINUTE = 60
RATE_LIMIT_WINDOW = 60  # seconds

# Rate limiter state
api_call_count = 0
last_api_call_time = datetime.now()

# Validate configuration
if not all([DISCORD_TOKEN, TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET]):
    logger.error("Missing required configuration values")
    exit(1)

class RateLimitError(Exception):
    """Exception raised when API rate limit is exceeded"""
    pass

class DatabaseError(Exception):
    """Exception raised for database-related errors"""
    pass

class APIError(Exception):
    """Exception raised for API-related errors"""
    pass

# Data structures
@dataclass
class TwitchToken:
    token: str
    expires_at: datetime

# Global state
token_cache: Optional[TwitchToken] = None

# Create bot instance with required intents
intents = discord.Intents.default()
intents.message_content = True

class MyBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = datetime.now()
        
    async def setup_hook(self):
        # Remove the default help command
        self.remove_command('help')

bot = MyBot(command_prefix="!", intents=intents)

# Register commands after bot initialization
@bot.command()
@commands.has_permissions(administrator=True)
async def addtwitch(ctx, username: str):
    """Add a Twitch account to monitor with validation"""
    if not await validate_username(username):
        await ctx.send('Invalid Twitch username. Must be between 2 and 25 characters.')
        return
    
    # Check if username already exists
    async with aiosqlite.connect(DATABASE) as db:
        async with db.execute('''
            SELECT username FROM accounts 
            WHERE user_id = ? AND username = ?
        ''', (ctx.author.id, username.lower())) as cursor:
            if await cursor.fetchone():
                await ctx.send(f'{username} is already in your monitored accounts!')
                return
    
    await execute_db_operation(
        ctx,
        operation='add',
        query=''' 
            INSERT INTO accounts (user_id, username, is_premium)
            VALUES (?, ?, 0)
        ''',
        params=(ctx.author.id, username.lower()),
        success_message=f'Successfully added {username} to your monitored accounts!'
    )

@bot.command()
@commands.has_permissions(administrator=True)
async def premiumtwitchadd(ctx, username: str):
    """Add a premium Twitch account to monitor with @everyone notifications"""
    if not await validate_username(username):
        await ctx.send('Invalid Twitch username. Must be between 2 and 25 characters.')
        return
    
    # Check if username already exists
    async with aiosqlite.connect(DATABASE) as db:
        async with db.execute('''
            SELECT username FROM accounts 
            WHERE user_id = ? AND username = ?
        ''', (ctx.author.id, username.lower())) as cursor:
            if await cursor.fetchone():
                await ctx.send(f'{username} is already in your monitored accounts!')
                return
    
    await execute_db_operation(
        ctx,
        operation='add',
        query=''' 
            INSERT INTO accounts (user_id, username, is_premium)
            VALUES (?, ?, 1)
        ''',
        params=(ctx.author.id, username.lower()),
        success_message=f'Successfully added {username} as a premium account!'
    )


@bot.command()
@commands.has_permissions(administrator=True)
async def removetwitch(ctx, username: str):
    """Remove a Twitch account from monitoring with validation"""
    if not await validate_username(username):
        await ctx.send('Invalid Twitch username. Must be between 2 and 25 characters.')
        return
    
    await execute_db_operation(
        ctx,
        operation='remove',
        query=''' 
            DELETE FROM accounts
            WHERE user_id = ? AND username = ?
        ''',
        params=(ctx.author.id, username.lower()),
        success_message=f'Successfully removed {username} from your monitored accounts!',
        not_found_message=f'{username} is not in your monitored accounts!'
    )

@bot.command()
async def ping(ctx):
    """Check if the bot is responsive and show latency"""
    latency = round(bot.latency * 1000)  # Convert to milliseconds
    await ctx.send(f'Pong! Latency: {latency}ms')

@bot.command()
async def debug(ctx):
    """Check server rates and status with detailed information"""
    try:
        # Get system information
        cpu_usage = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        # Get bot process information
        process = psutil.Process()
        bot_memory = process.memory_info().rss / 1024 / 1024
        bot_threads = process.num_threads()
        
        # Get API rate limit status
        time_since_last_call = (datetime.now() - last_api_call_time).total_seconds()
        rate_limit_remaining = max(0, RATE_LIMIT_WINDOW - time_since_last_call)
        
        # Get Twitch token status
        token_status = "Valid" if token_cache and token_cache.expires_at > datetime.now() else "Expired"
        
        # Get database status
        db_status = "Connected"
        monitored_accounts = 0
        try:
            async with aiosqlite.connect(DATABASE) as db:
                await db.execute("SELECT 1")
                # Get number of monitored accounts
                cursor = await db.execute("SELECT COUNT(*) FROM accounts")
                monitored_accounts = (await cursor.fetchone())[0]
        except Exception:
            db_status = "Disconnected"
        
        # Get bot uptime
        uptime = datetime.now() - bot.start_time
        hours, remainder = divmod(uptime.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        # Get active tasks
        active_tasks = len(asyncio.all_tasks())
        
        # Build debug message
        embed = discord.Embed(
            title="📊 Detailed Server Debug Information",
            color=0x00FF00 if db_status == "Connected" else 0xFF0000
        )
        
        # System Information
        embed.add_field(
            name="🖥️ System Status",
            value=f"• CPU Usage: {cpu_usage}%\n"
                  f"• Memory Usage: {memory.percent}% ({memory.used / 1024 / 1024:.1f} MB / {memory.total / 1024 / 1024:.1f} MB)\n"
                  f"• Disk Usage: {disk.percent}% ({disk.used / 1024 / 1024:.1f} GB / {disk.total / 1024 / 1024:.1f} GB)",
            inline=False
        )
        
        # Bot Information
        embed.add_field(
            name="🤖 Bot Status",
            value=f"• Uptime: {int(hours)}h {int(minutes)}m {int(seconds)}s\n"
                  f"• Memory Usage: {bot_memory:.2f} MB\n"
                  f"• Threads: {bot_threads}\n"
                  f"• Active Tasks: {active_tasks}\n"
                  f"• Latency: {round(bot.latency * 1000)}ms",
            inline=False
        )
        
        # Connection Information
        embed.add_field(
            name="🌐 Connection Status",
            value=f"• Connected Guilds: {len(bot.guilds)}\n"
                  f"• Database: {db_status}\n"
                  f"• Monitored Accounts: {monitored_accounts}",
            inline=False
        )
        
        # API Information
        embed.add_field(
            name="🔌 API Status",
            value=f"• Twitch Token: {token_status}\n"
                  f"• Token Expires: {token_cache.expires_at.strftime('%H:%M:%S') if token_cache else 'N/A'}\n"
                  f"• API Calls: {api_call_count}/{MAX_API_CALLS_PER_MINUTE}\n"
                  f"• Rate Limit Resets in: {rate_limit_remaining:.1f}s",
            inline=False
        )
        
        # Add timestamp
        embed.set_footer(text=f"Debug information generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        await ctx.send(embed=embed)
        
    except aiosqlite.Error as e:
        logger.error(f"Database error in debug command: {e}")
        await ctx.send("❌ Failed to connect to database. Please check database configuration.")
    except aiohttp.ClientError as e:
        logger.error(f"API connection error in debug command: {e}")
        await ctx.send("❌ Failed to connect to Twitch API. Please check API credentials and network connection.")
    except psutil.NoSuchProcess as e:
        logger.error(f"Process monitoring error in debug command: {e}")
        await ctx.send("❌ Failed to retrieve system information. Please ensure the bot has proper permissions.")
    except Exception as e:
        logger.error(f"Unexpected error in debug command: {e}")
        await ctx.send(f"❌ An unexpected error occurred: {str(e)}")

@bot.command()
async def removepremiumtwitch(ctx, username: str):
    """Remove a premium Twitch account from monitoring"""
    if not await validate_username(username):
        await ctx.send('Invalid Twitch username. Must be between 2 and 25 characters.')
        return
    
    await execute_db_operation(
        ctx,
        operation='remove',
        query=''' 
            DELETE FROM accounts
            WHERE user_id = ? AND username = ? AND is_premium = 1
        ''',
        params=(ctx.author.id, username.lower()),
        success_message=f'Successfully removed premium account {username}!',
        not_found_message=f'{username} is not in your premium accounts!'
    )

@bot.command()
async def listtwitch(ctx):
    """List all Twitch accounts being monitored by the user"""
    try:
        async with aiosqlite.connect(DATABASE) as db:
            async with db.execute('''
                SELECT username, is_premium FROM accounts 
                WHERE user_id = ?
                ORDER BY username
            ''', (ctx.author.id,)) as cursor:
                accounts = await cursor.fetchall()
                
                if not accounts:
                    await ctx.send("You're not monitoring any Twitch accounts yet!")
                    return
                    
                embed = discord.Embed(
                    title="📺 Your Monitored Twitch Accounts",
                    color=0x9146FF
                )
                
                for username, is_premium in accounts:
                    status = "⭐ Premium" if is_premium else "Regular"
                    embed.add_field(
                        name=username,
                        value=status,
                        inline=False
                    )
                    
                embed.set_footer(text=f"Total accounts: {len(accounts)}")
                await ctx.send(embed=embed)
                
    except aiosqlite.Error as e:
        logger.error(f"Database error in listtwitch command: {e}")
        await ctx.send("❌ Failed to retrieve account list. Please try again later.")
    except Exception as e:
        logger.error(f"Unexpected error in listtwitch command: {e}")
        await ctx.send(f"❌ An unexpected error occurred: {str(e)}")

# Sync commands on startup
@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user}')
    
    # Verify bot token
    try:
        app_info = await bot.application_info()
        logger.info(f"Bot owner: {app_info.owner}")
        logger.info(f"Bot public: {app_info.bot_public}")
        logger.info(f"Bot requires code grant: {app_info.bot_require_code_grant}")
    except discord.HTTPException as e:
        logger.error(f"Failed to verify bot token: {e}")
        logger.error("Please ensure the bot token is correct and valid")
        return
    
    # Force refresh guild cache
    await bot.wait_until_ready()
    await bot.fetch_guilds().flatten()
    
    # Log guild connection status
    if bot.guilds:
        logger.info(f"Connected to {len(bot.guilds)} guild(s):")
        for guild in bot.guilds:
            logger.info(f" - Guild: {guild.name} (ID: {guild.id})")
            logger.info(f"   Owner: {guild.owner}")
            logger.info(f"   Member Count: {guild.member_count}")
            permissions = guild.me.guild_permissions
            logger.info(f"   Bot Permissions: {permissions.value}")
            logger.info(f"   Manage Server: {permissions.manage_guild}")
            logger.info(f"   Use Slash Commands: {permissions.use_slash_commands}")
            logger.info(f"   Send Messages: {permissions.send_messages}")
            
            # Verify bot is actually in the server
            try:
                member = await guild.fetch_member(bot.user.id)
                logger.info(f"   Bot is present in server: {member is not None}")
                logger.info(f"   Bot roles: {[role.name for role in member.roles]}")
                logger.info(f"   Bot permissions: {member.guild_permissions.value}")
                
                # Verify bot has necessary permissions
                if not permissions.manage_guild:
                    logger.error("Bot does not have 'Manage Server' permission")
                if not permissions.use_slash_commands:
                    logger.error("Bot does not have 'Use Slash Commands' permission")
                if not permissions.send_messages:
                    logger.error("Bot does not have 'Send Messages' permission")
                    
            except discord.NotFound:
                logger.warning("   Bot is not actually present in this server")
                logger.info("Please ensure you've properly invited the bot using this URL:")
                logger.info(f"https://discord.com/oauth2/authorize?client_id={bot.user.id}&permissions=8&scope=bot%20applications.commands")
                logger.info("After inviting, please restart the bot")
    else:
        logger.warning("Bot is not connected to any guilds")
        logger.info("Please ensure you've invited the bot using this URL:")
        logger.info(f"https://discord.com/oauth2/authorize?client_id={bot.user.id}&permissions=8&scope=bot%20applications.commands")
        logger.info("Make sure you've selected the correct server when inviting the bot")
        logger.info("Also ensure that:")
        logger.info("1. The bot is not banned from the server")
        logger.info("2. The bot has been properly authorized with all required permissions")
        logger.info("3. The bot's token is correct and valid")
        
    try:
        # Initialize database
        await init_db()
        
        # Verify bot permissions
        logger.info("Bot permissions:")
        if bot.guilds:
            for guild in bot.guilds:
                permissions = guild.me.guild_permissions
                logger.info(f"Guild: {guild.name} (ID: {guild.id})")
                logger.info(f" - Manage Server: {permissions.manage_guild}")
                logger.info(f" - Use Slash Commands: {permissions.use_slash_commands}")
                logger.info(f" - Send Messages: {permissions.send_messages}")
        else:
            logger.warning("Bot is not in any guilds")
            
        # Verify commands are registered
        guild = bot.guilds[0] if bot.guilds else None
        if guild:
            guild_commands = await guild.fetch_commands()
            logger.info(f"Found {len(guild_commands)} commands registered in guild")
            for cmd in guild_commands:
                logger.info(f"Guild command: {cmd.name} (ID: {cmd.id})")
        else:
            logger.warning("Bot is not in any guilds")
            
        # Start background tasks
        asyncio.create_task(check_streams())
            
    except discord.Forbidden as e:
        logger.error(f"Permission denied: {e}")
        logger.error("Please ensure the bot has the following permissions:")
        logger.error("1. 'application.commands' scope in the OAuth2 URL")
        logger.error("2. 'Manage Server' permission in the server")
        logger.error("3. 'Use Slash Commands' permission")
        logger.error("4. 'Send Messages' permission")
    except discord.HTTPException as e:
        logger.error(f"HTTP error occurred: {e}")
        logger.error("This might be due to rate limits or API issues")
    except Exception as e:
        logger.error(f"Failed to initialize bot: {e}")
        logger.error("Please check the bot's permissions and try again")

# Database configuration
DATABASE = 'twitch_accounts.db'

async def init_db() -> None:
    """Initialize the database with required tables"""
    retry_count = 0
    while retry_count < API_RETRY_COUNT:
        try:
            async with aiosqlite.connect(DATABASE) as db:
                # Enable WAL mode for better concurrency
                await db.execute('PRAGMA journal_mode=WAL')
                await db.execute('''
                    CREATE TABLE IF NOT EXISTS accounts (
                        user_id INTEGER NOT NULL,
                        username TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        notified INTEGER DEFAULT 0,
                        is_premium INTEGER DEFAULT 0,
                        UNIQUE(user_id, username)
                    )
                ''')
                # Check if notified column exists
                cursor = await db.execute("PRAGMA table_info(accounts)")
                columns = await cursor.fetchall()
                column_names = [column[1] for column in columns]
                
                if 'notified' not in column_names:
                    await db.execute('''
                        ALTER TABLE accounts ADD COLUMN notified INTEGER DEFAULT 0
                    ''')
                if 'is_premium' not in column_names:
                    await db.execute('''
                        ALTER TABLE accounts ADD COLUMN is_premium INTEGER DEFAULT 0
                    ''')
                # Create index for faster lookups
                await db.execute('''
                    CREATE INDEX IF NOT EXISTS idx_username 
                    ON accounts(username)
                ''')
                await db.commit()
                return
        except aiosqlite.Error as e:
            retry_count += 1
            logger.error(f"Database initialization failed (attempt {retry_count}): {e}")
            if retry_count < API_RETRY_COUNT:
                await asyncio.sleep(API_RETRY_DELAY)
            else:
                raise DatabaseError("Failed to initialize database after multiple attempts")

async def get_twitch_token() -> Optional[str]:
    """Get OAuth token from Twitch API with caching"""
    global token_cache
    
    # Return cached token if valid
    if token_cache and token_cache.expires_at > datetime.now():
        return token_cache.token
    
    params = {
        'client_id': TWITCH_CLIENT_ID,
        'client_secret': TWITCH_CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    
    retry_count = 0
    while retry_count < API_RETRY_COUNT:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(TWITCH_OAUTH_URL, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    token = data.get('access_token')
                    if token:
                        # Cache the token with expiration time
                        expires_in = data.get('expires_in', TOKEN_CACHE_DURATION)
                        token_cache = TwitchToken(
                            token=token,
                            expires_at=datetime.now() + timedelta(seconds=expires_in)
                        )
                        return token
                    return None
        except aiohttp.ClientError as e:
            retry_count += 1
            logger.error(f"Failed to get Twitch token (attempt {retry_count}): {e}")
            if retry_count < API_RETRY_COUNT:
                await asyncio.sleep(API_RETRY_DELAY)
    
    return None

async def check_live_status(user_login: str, token: str) -> Optional[Dict[str, Any]]:
    """Check if a Twitch user is currently live with retry mechanism"""
    url = f'{TWITCH_API_URL}/streams?user_login={user_login}'
    headers = {
        'Client-ID': TWITCH_CLIENT_ID,
        'Authorization': f'Bearer {token}'
    }
    
    retry_count = 0
    while retry_count < API_RETRY_COUNT:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    response.raise_for_status()
                    return await response.json()
        except aiohttp.ClientError as e:
            retry_count += 1
            logger.error(f"Failed to check live status for {user_login} (attempt {retry_count}): {e}")
            if retry_count < API_RETRY_COUNT:
                await asyncio.sleep(API_RETRY_DELAY)
    
    return None

async def validate_username(username: str) -> bool:
    """Validate Twitch username format"""
    return username and 2 <= len(username) <= 25

async def execute_db_operation(
    ctx: commands.Context,
    operation: str,
    query: str,
    params: tuple,
    success_message: str,
    not_found_message: str = None
) -> None:
    """Execute a database operation with retry mechanism"""
    retry_count = 0
    while retry_count < API_RETRY_COUNT:
        try:
            async with aiosqlite.connect(DATABASE) as db:
                # Check if account exists for operations that require it
                if operation in ['remove', 'check']:
                    async with db.execute('''
                        SELECT username FROM accounts 
                        WHERE user_id = ? AND username = ?
                    ''', (ctx.author.id, params[1].lower())) as cursor:
                        if not await cursor.fetchone():
                            await ctx.send(not_found_message)
                            return
                
                # Execute the main operation
                await db.execute(query, params)
                await db.commit()
                
                await ctx.send(success_message)
                return
        except aiosqlite.Error as e:
            retry_count += 1
            logger.error(f"Failed to {operation} Twitch account (attempt {retry_count}): {e}")
            if retry_count < API_RETRY_COUNT:
                await asyncio.sleep(API_RETRY_DELAY)
    
    await ctx.send(f'An error occurred while {operation}ing the account. Please try again later.')
    raise DatabaseError(f"Failed to {operation} Twitch account after {API_RETRY_COUNT} attempts")


async def check_streams() -> None:
    """Check if monitored accounts are live"""
    while True:
        try:
            async with aiosqlite.connect(DATABASE) as db:
                async with db.execute('SELECT DISTINCT username FROM accounts') as cursor:
                    usernames = [row[0] for row in await cursor.fetchall()]

                if not usernames:
                    logger.info("No accounts to monitor")
                    await asyncio.sleep(60)  # Wait 1 minute before checking again
                    continue

                # Check live status for each username
                for username in usernames:
                    try:
                        logger.info(f"Checking live status for {username}")
                        stream = twitch_api.check_if_live(username)
                        if stream:
                            logger.info(f"Stream found for {username}: {stream.title}")
                            channel = bot.get_channel(NOTIFICATION_CHANNEL_ID)
                            if channel:
                                # Check if we've already notified about this stream
                                async with db.execute('''
                                    SELECT notified FROM accounts 
                                    WHERE username = ?
                                ''', (username.lower(),)) as cursor:
                                    result = await cursor.fetchone()
                                    if result and not result[0]:
                                        # Send notification
                                        embed = discord.Embed(
                                            title=stream.title,
                                            description=f"{stream.streamer} is now live on Twitch!",
                                            color=0x9146FF,
                                            url=f"https://twitch.tv/{username}"
                                        )
                                        embed.add_field(name="Game", value=stream.game)
                                        embed.set_thumbnail(url=stream.thumbnail_url.format(width=320, height=180))
                                        embed.set_footer(text=f"Started at {stream.started_at.strftime('%Y-%m-%d %H:%M:%S')}")
                                        
                                        # Check if this is a premium account
                                        async with db.execute('''
                                            SELECT is_premium FROM accounts 
                                            WHERE username = ?
                                        ''', (username.lower(),)) as cursor:
                                            premium_result = await cursor.fetchone()
                                            if premium_result and premium_result[0]:
                                                logger.info(f"Account {username} is premium, sending to premium channel")
                                                premium_channel = bot.get_channel(PREMIUM_NOTIFICATION_CHANNEL_ID)
                                                if premium_channel:
                                                    logger.info(f"Sending notification to premium channel {PREMIUM_NOTIFICATION_CHANNEL_ID}")
                                                    await premium_channel.send('@everyone', embed=embed)
                                                else:
                                                    logger.error(f"Could not find premium channel {PREMIUM_NOTIFICATION_CHANNEL_ID}")
                                            else:
                                                logger.info(f"Account {username} is regular, sending to regular channel")
                                                await channel.send(embed=embed)
                                        # Mark as notified
                                        await db.execute('''
                                            UPDATE accounts SET notified = 1 
                                            WHERE username = ?
                                        ''', (username.lower(),))
                                        await db.commit()
                    except Exception as e:
                        logger.error(f"Error checking status for {username}: {e}")

                # Reset notified status for offline streams
                for username in usernames:
                    if not twitch_api.check_if_live(username):
                        await db.execute('''
                            UPDATE accounts SET notified = 0 
                            WHERE username = ?
                        ''', (username.lower(),))
                        await db.commit()

                await asyncio.sleep(15)  # Wait 15 seconds before checking again

        except Exception as e:
            logger.error(f"Error in check_streams: {e}")
            await asyncio.sleep(15)  # Wait 15 seconds before retrying

@bot.event
async def on_ready() -> None:
    """Initialize the bot and start background tasks"""
    logger.info(f'Logged in as {bot.user}')
    try:
        await init_db()
        asyncio.create_task(check_streams())
    except Exception as e:
        logger.error(f"Failed to initialize bot: {e}")
        raise

# Run the bot
try:
    bot.run(DISCORD_TOKEN)
except Exception as e:
    logger.error(f"Failed to start bot: {e}")

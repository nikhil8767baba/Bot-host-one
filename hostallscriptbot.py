import telebot
import subprocess
import os
import zipfile
import tempfile
import shutil
from telebot import types
import time
from datetime import datetime, timedelta
import psutil
import sqlite3
import json
import logging
import signal
import threading
import re
import sys
import atexit
import requests
import ast
from pathlib import Path
import hashlib
import flask
import request
# --- Flask Keep Alive ---
from flask import Flask, render_template, jsonify, request, send_file
from threading import Thread

app = Flask(__name__)

@app.route('/')
def home():
    return """
    <html>
    <head><title>Universal File Host</title></head>
    <body style="font-family: Arial; text-align: center; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 50px;">
        <h1>File Host By @UnknownGuy6666</h1>
        <h2>Multi-Language Code Execution & File Hosting Platform</h2>
        <p>📁 Supporting 30+ file types with hosting</p>
        <p>🚀 Multi-language code execution</p>
        <p>🌟 Real-time execution monitoring</p>
    </body>
    </html>
    """

@app.route('/file/<file_hash>')
def serve_file(file_hash):
    """Serve hosted files by hash"""
    try:
        # Find the file by hash
        for user_id in user_files:
            for file_name, file_type in user_files[user_id]:
                expected_hash = hashlib.md5(f"{user_id}_{file_name}".encode()).hexdigest()
                if expected_hash == file_hash:
                    file_path = os.path.join(get_user_folder(user_id), file_name)
                    if os.path.exists(file_path):
                        return send_file(file_path, as_attachment=False)

        return "File not found", 404
    except Exception as e:
        logger.error(f"Error serving file {file_hash}: {e}")
        return "Error serving file", 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route('/files')
def list_files():
    """List all hosted files (for debugging)"""
    try:
        files_list = []
        for user_id in user_files:
            for file_name, file_type in user_files[user_id]:
                if file_type == 'hosted':
                    file_hash = hashlib.md5(f"{user_id}_{file_name}".encode()).hexdigest()
                    files_list.append({
                        'name': file_name,
                        'user_id': user_id,
                        'hash': file_hash,
                        'url': f"/file/{file_hash}"
                    })
        return jsonify({"files": files_list})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)

def keep_alive():
    t = Thread(target=run_flask)
    t.daemon = True
    t.start()
    print("Flask Keep-Alive server started.")

# --- Configuration ---
TOKEN = '8301623292:AAHe9BH792Y8WbfEZP0_YX56iR3w_1gPjYE'
OWNER_ID = 8158657600
ADMIN_ID = 8158657600
YOUR_USERNAME = '@ITS_ME_UNKNOW_USER'
UPDATE_CHANNEL = '@nik_ffx'

# Enhanced folder setup
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_BOTS_DIR = os.path.join(BASE_DIR, 'upload_bots')
IROTECH_DIR = os.path.join(BASE_DIR, 'inf')
DATABASE_PATH = os.path.join(IROTECH_DIR, 'bot_data.db')
LOGS_DIR = os.path.join(BASE_DIR, 'execution_logs')

# File upload limits
FREE_USER_LIMIT = 5
SUBSCRIBED_USER_LIMIT = 25
ADMIN_LIMIT = 999
OWNER_LIMIT = float('inf')

# Create necessary directories
for directory in [UPLOAD_BOTS_DIR, IROTECH_DIR, LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

# Initialize bot
bot = telebot.TeleBot(TOKEN)

# --- Data structures ---
bot_scripts = {}
user_subscriptions = {}
user_files = {}
active_users = set()
admin_ids = {ADMIN_ID, OWNER_ID}
bot_locked = False

# --- Logging Setup ---
# Fix for Windows Unicode issue - remove emojis from logs
class NoEmojiFormatter(logging.Formatter):
    def format(self, record):
        # Remove emojis from log messages
        if hasattr(record, 'msg') and record.msg:
            record.msg = self.remove_emojis(record.msg)
        return super().format(record)
    
    def remove_emojis(self, text):
        # Remove common emoji ranges
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags (iOS)
            "]+", flags=re.UNICODE)
        return emoji_pattern.sub('', str(text))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOGS_DIR, 'bot.log'), encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Apply no-emoji formatter to console handler only (keep file logs with emojis)
for handler in logger.handlers:
    if isinstance(handler, logging.StreamHandler):
        handler.setFormatter(NoEmojiFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

# --- Command Button Layouts ---
COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["Updates Channel"],
    ["Upload File", "Check Files"],
    ["Bot Speed", "Statistics"],
    ["Contact Owner"]
]

ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC = [
    ["Updates Channel"],
    ["Upload File", "Check Files"],
    ["Bot Speed", "Statistics"],
    ["Subscriptions", "Broadcast"],
    ["Lock Bot", "Running All Code"],
    ["Admin Panel"],
    ["Contact Owner"]
]

# --- Database Functions ---
def init_db():
    """Initialize the database with enhanced tables"""
    logger.info(f"Initializing database at: {DATABASE_PATH}")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()

        # Create tables
        c.execute('''CREATE TABLE IF NOT EXISTS subscriptions
                     (user_id INTEGER PRIMARY KEY, expiry TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS user_files
                     (user_id INTEGER, file_name TEXT, file_type TEXT,
                      PRIMARY KEY (user_id, file_name))''')
        c.execute('''CREATE TABLE IF NOT EXISTS active_users
                     (user_id INTEGER PRIMARY KEY)''')
        c.execute('''CREATE TABLE IF NOT EXISTS admins
                     (user_id INTEGER PRIMARY KEY)''')

        # Ensure admins
        c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (OWNER_ID,))
        if ADMIN_ID != OWNER_ID:
            c.execute('INSERT OR IGNORE INTO admins (user_id) VALUES (?)', (ADMIN_ID,))

        conn.commit()
        conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Database initialization error: {e}")

def load_data():
    """Load data from database into memory"""
    logger.info("Loading data from database...")
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()

        # Load subscriptions
        c.execute('SELECT user_id, expiry FROM subscriptions')
        for user_id, expiry in c.fetchall():
            try:
                user_subscriptions[user_id] = {'expiry': datetime.fromisoformat(expiry)}
            except ValueError:
                logger.warning(f"Invalid expiry date for user {user_id}")

        # Load user files
        c.execute('SELECT user_id, file_name, file_type FROM user_files')
        for user_id, file_name, file_type in c.fetchall():
            if user_id not in user_files:
                user_files[user_id] = []
            user_files[user_id].append((file_name, file_type))

        # Load active users
        c.execute('SELECT user_id FROM active_users')
        active_users.update(user_id for (user_id,) in c.fetchall())

        # Load admins
        c.execute('SELECT user_id FROM admins')
        admin_ids.update(user_id for (user_id,) in c.fetchall())

        conn.close()
        logger.info(f"Data loaded: {len(active_users)} users, {len(user_files)} file records")
    except Exception as e:
        logger.error(f"Error loading data: {e}")

# --- Helper Functions ---
def get_user_folder(user_id):
    """Get or create user's folder for storing files"""
    user_folder = os.path.join(UPLOAD_BOTS_DIR, str(user_id))
    os.makedirs(user_folder, exist_ok=True)
    return user_folder

def get_user_file_limit(user_id):
    """Get the file upload limit for a user"""
    if user_id == OWNER_ID: return OWNER_LIMIT
    if user_id in admin_ids: return ADMIN_LIMIT
    if user_id in user_subscriptions and user_subscriptions[user_id]['expiry'] > datetime.now():
        return SUBSCRIBED_USER_LIMIT
    return FREE_USER_LIMIT

def get_user_file_count(user_id):
    """Get the number of files uploaded by a user"""
    return len(user_files.get(user_id, []))

def is_bot_running(script_owner_id, file_name):
    """Check if a bot script is currently running"""
    script_key = f"{script_owner_id}_{file_name}"
    script_info = bot_scripts.get(script_key)
    if script_info and script_info.get('process'):
        try:
            proc = psutil.Process(script_info['process'].pid)
            is_running = proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
            if not is_running:
                if script_key in bot_scripts:
                    del bot_scripts[script_key]
            return is_running
        except psutil.NoSuchProcess:
            if script_key in bot_scripts:
                del bot_scripts[script_key]
            return False
        except Exception:
            return False
    return False

def safe_send_message(chat_id, text, parse_mode=None, reply_markup=None):
    """Safely send message with fallback for parse errors"""
    try:
        return bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        if "can't parse entities" in str(e):
            return bot.send_message(chat_id, text, reply_markup=reply_markup)
        else:
            raise e

def safe_edit_message(chat_id, message_id, text, parse_mode=None, reply_markup=None):
    """Safely edit message with fallback for parse errors and identical content"""
    try:
        return bot.edit_message_text(text, chat_id, message_id, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        if "message is not modified" in str(e):
            # Ignore the error if content is identical
            return None
        elif "can't parse entities" in str(e):
            return bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup)
        else:
            raise e

def safe_reply_to(message, text, parse_mode=None, reply_markup=None):
    """Safely reply to message with fallback for parse errors"""
    try:
        return bot.reply_to(message, text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        if "can't parse entities" in str(e):
            return bot.reply_to(message, text, reply_markup=reply_markup)
        else:
            raise e

def auto_install_dependencies(file_path, file_ext, user_folder):
    """Auto-install dependencies based on file type"""
    installations = []
    
    try:
        if file_ext == '.py':
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            python_packages = {
                'requests': 'requests',
                'flask': 'flask',
                'telebot': 'pyTelegramBotAPI',
                'telegram': 'python-telegram-bot',
                'psutil': 'psutil',
                'sqlite3': 'sqlite3',
            }
            
            import_pattern = r'(?:from\s+(\w+)|import\s+(\w+))'
            matches = re.findall(import_pattern, content)
            
            for match in matches:
                module = match[0] or match[1]
                if module in python_packages and python_packages[module]:
                    try:
                        result = subprocess.run([sys.executable, '-m', 'pip', 'install', python_packages[module]], 
                                               capture_output=True, text=True, timeout=30)
                        if result.returncode == 0:
                            installations.append(f"Installed Python package: {python_packages[module]}")
                    except Exception as e:
                        logger.error(f"Failed to install {python_packages[module]}: {e}")
        
        elif file_ext == '.js':
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            node_packages = {
                'express': 'express',
                'axios': 'axios',
                'fs': 'fs',
            }
            
            require_pattern = r'require\([\'"](\w+)[\'"]\)'
            matches = re.findall(require_pattern, content)
            
            for module in matches:
                if module in node_packages and node_packages[module]:
                    try:
                        result = subprocess.run(['npm', 'install', node_packages[module]], 
                                               cwd=user_folder, capture_output=True, text=True, timeout=30)
                        if result.returncode == 0:
                            installations.append(f"Installed Node package: {node_packages[module]}")
                    except Exception as e:
                        logger.error(f"Failed to install {node_packages[module]}: {e}")
    
    except Exception as e:
        logger.error(f"Error in auto_install_dependencies: {e}")
    
    return installations

def execute_script(user_id, script_path, message_for_updates=None):
    """Execute a script with comprehensive language support and hosting"""
    script_name = os.path.basename(script_path)
    script_ext = os.path.splitext(script_path)[1].lower()

    # Supported file types
    supported_types = {
        '.py': {'name': 'Python', 'icon': '🐍', 'executable': True, 'type': 'executable'},
        '.js': {'name': 'JavaScript', 'icon': '🟨', 'executable': True, 'type': 'executable'},
        '.java': {'name': 'Java', 'icon': '☕', 'executable': True, 'type': 'executable'},
        '.cpp': {'name': 'C++', 'icon': '🔧', 'executable': True, 'type': 'executable'},
        '.c': {'name': 'C', 'icon': '🔧', 'executable': True, 'type': 'executable'},
        '.sh': {'name': 'Shell', 'icon': '🖥️', 'executable': True, 'type': 'executable'},
        '.rb': {'name': 'Ruby', 'icon': '💎', 'executable': True, 'type': 'executable'},
        '.go': {'name': 'Go', 'icon': '🐹', 'executable': True, 'type': 'executable'},
        '.rs': {'name': 'Rust', 'icon': '🦀', 'executable': True, 'type': 'executable'},
        '.php': {'name': 'PHP', 'icon': '🐘', 'executable': True, 'type': 'executable'},
        '.html': {'name': 'HTML', 'icon': '🌐', 'executable': False, 'type': 'hosted'},
        '.css': {'name': 'CSS', 'icon': '🎨', 'executable': False, 'type': 'hosted'},
        '.json': {'name': 'JSON', 'icon': '📋', 'executable': False, 'type': 'hosted'},
        '.txt': {'name': 'Text', 'icon': '📄', 'executable': False, 'type': 'hosted'},
        '.jpg': {'name': 'JPEG Image', 'icon': '🖼️', 'executable': False, 'type': 'hosted'},
        '.png': {'name': 'PNG Image', 'icon': '🖼️', 'executable': False, 'type': 'hosted'},
        '.pdf': {'name': 'PDF Document', 'icon': '📄', 'executable': False, 'type': 'hosted'},
        '.zip': {'name': 'ZIP Archive', 'icon': '📦', 'executable': False, 'type': 'hosted'},
        '.bat': {'name': 'Batch Script', 'icon': '🖥️', 'executable': True, 'type': 'executable'},
        '.ps1': {'name': 'PowerShell', 'icon': '💙', 'executable': True, 'type': 'executable'},
    }

    if script_ext not in supported_types:
        return False, f"Unsupported file type: {script_ext}"

    lang_info = supported_types[script_ext]

    try:
        # Send initial message
        if message_for_updates:
            safe_edit_message(
                message_for_updates.chat.id,
                message_for_updates.message_id,
                f"{lang_info['icon']} Processing {lang_info['name']} file\nFile: {script_name}\nStatus: Analyzing..."
            )

        # Check if file is executable
        if not lang_info.get('executable', True):
            if message_for_updates:
                success_msg = f"{lang_info['icon']} {lang_info['name']} file hosted successfully!\n\nFile: {script_name}\nStatus: Hosted\nAccess: Use 'Check Files' button"
                safe_edit_message(
                    message_for_updates.chat.id, 
                    message_for_updates.message_id, 
                    success_msg
                )
            return True, "File hosted successfully"

        # Execute the script for executable types
        if message_for_updates:
            safe_edit_message(
                message_for_updates.chat.id,
                message_for_updates.message_id,
                f"{lang_info['icon']} Executing {lang_info['name']} script...\nFile: {script_name}\nStatus: Installing dependencies..."
            )

        # Auto-install dependencies
        user_folder = get_user_folder(user_id)
        installations = auto_install_dependencies(script_path, script_ext, user_folder)
        
        if installations and message_for_updates:
            install_msg = f"{lang_info['icon']} Dependency installation:\n\n" + "\n".join(installations[:5])
            if len(installations) > 5:
                install_msg += f"\n... and {len(installations) - 5} more"
            safe_send_message(message_for_updates.chat.id, install_msg)

        # Create execution log file
        log_file_path = os.path.join(LOGS_DIR, f"execution_{user_id}_{script_name}_{int(time.time())}.log")
        
        # Prepare execution command based on file type
        if script_ext == '.py':
            cmd = [sys.executable, script_path]
        elif script_ext == '.js':
            cmd = ['node', script_path]
        elif script_ext == '.java':
            class_name = os.path.splitext(script_name)[0]
            compile_result = subprocess.run(['javac', script_path], capture_output=True, text=True, timeout=60)
            if compile_result.returncode != 0:
                return False, f"Java compilation failed: {compile_result.stderr}"
            cmd = ['java', '-cp', os.path.dirname(script_path), class_name]
        elif script_ext in ['.cpp', '.c']:
            executable = os.path.join(user_folder, 'output')
            compiler = 'g++' if script_ext == '.cpp' else 'gcc'
            compile_result = subprocess.run([compiler, script_path, '-o', executable], 
                                          capture_output=True, text=True, timeout=60)
            if compile_result.returncode != 0:
                return False, f"C/C++ compilation failed: {compile_result.stderr}"
            cmd = [executable]
        elif script_ext == '.sh':
            cmd = ['bash', script_path]
        else:
            cmd = [script_path]

        logger.info(f"Executing command: {' '.join(cmd)}")
        logger.info(f"Working directory: {os.path.dirname(script_path)}")

        # Start the process with proper output handling
        with open(log_file_path, 'w', encoding='utf-8') as log_file:
            # Write initial info to log
            log_file.write(f"Execution started at: {datetime.now()}\n")
            log_file.write(f"Command: {' '.join(cmd)}\n")
            log_file.write(f"Working directory: {os.path.dirname(script_path)}\n")
            log_file.write("-" * 50 + "\n")
            log_file.flush()

            # Start the process
            process = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=os.path.dirname(script_path),
                env=os.environ.copy(),
                shell=False
            )

        # Store process info
        script_key = f"{user_id}_{script_name}"
        bot_scripts[script_key] = {
            'process': process,
            'script_key': script_key,
            'user_id': user_id,
            'file_name': script_name,
            'start_time': datetime.now(),
            'log_file_path': log_file_path,
            'language': lang_info['name'],
            'icon': lang_info['icon']
        }

        # Wait a moment to check if process started successfully
        time.sleep(1)
        
        # Check if process is still running
        if process.poll() is not None:
            # Process ended immediately, read the log to see what happened
            with open(log_file_path, 'r', encoding='utf-8') as f:
                error_output = f.read()
            
            error_msg = f"Script ended immediately. Exit code: {process.returncode}\n\nError output:\n{error_output[-1000:]}"  # Last 1000 chars
            logger.error(f"Script execution failed: {error_msg}")
            
            # Clean up failed process
            if script_key in bot_scripts:
                del bot_scripts[script_key]
            
            return False, error_msg

        # Success message
        if message_for_updates:
            success_msg = f"{lang_info['icon']} {lang_info['name']} script started successfully!\n\nFile: {script_name}\nProcess ID: {process.pid}\nLanguage: {lang_info['name']} {lang_info['icon']}\nStatus: Running\n\nLog file: {log_file_path}"
            safe_edit_message(
                message_for_updates.chat.id, 
                message_for_updates.message_id, 
                success_msg
            )

        logger.info(f"Script started successfully with PID {process.pid}")
        return True, f"Script started with PID {process.pid}"

    except Exception as e:
        error_msg = f"Execution failed: {str(e)}"
        logger.error(f"Script execution error for user {user_id}: {e}")
        
        # Clean up on error
        script_key = f"{user_id}_{script_name}"
        if script_key in bot_scripts:
            del bot_scripts[script_key]

        if message_for_updates:
            safe_edit_message(
                message_for_updates.chat.id, 
                message_for_updates.message_id, 
                f"❌ {error_msg}"
            )

        return False, error_msg

# --- Command Handlers ---
@bot.message_handler(commands=['start'])
def start_command(message):
    """Enhanced start command with comprehensive file type support"""
    user_id = message.from_user.id

    # Add user to active users
    active_users.add(user_id)

    # Save to database
    try:
        conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
        c = conn.cursor()
        c.execute('INSERT OR IGNORE INTO active_users (user_id) VALUES (?)', (user_id,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Database error in start command: {e}")

    # Get user info
    user_name = message.from_user.first_name or "User"
    is_admin = user_id in admin_ids

    # Create welcome message
    welcome_msg = f"🔐 UNIVERSAL FILE HOST\n\n"
    welcome_msg += f"👋 Welcome {user_name}!\n\n"
    welcome_msg += f"📁 SUPPORTED FILE TYPES:\n"
    welcome_msg += f"🚀 Executable: Python, JavaScript, Java, C/C++, Go, Rust, PHP, Shell, Ruby\n\n"
    welcome_msg += f"📄 Hosted: HTML, CSS, JSON, Text, Images, PDFs, Archives\n\n"
    welcome_msg += f"🔐 FEATURES:\n"
    welcome_msg += f"✅ Universal file hosting\n"
    welcome_msg += f"🚀 Multi-language code execution\n"
    welcome_msg += f"📊 Process management\n"
    welcome_msg += f"⚡ Auto dependency installation\n\n"
    welcome_msg += f"📊 YOUR STATUS:\n"
    welcome_msg += f"📁 Upload Limit: {get_user_file_limit(user_id)} files\n"
    welcome_msg += f"📄 Current Files: {get_user_file_count(user_id)} files\n"
    welcome_msg += f"👤 Account Type: {'👑 Owner' if user_id == OWNER_ID else '👑 Admin' if is_admin else '👤 User'}\n"
    welcome_msg += f"\n"
    welcome_msg += f"💡 Quick Start: Upload any file to begin!"

    # Create reply markup
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    if is_admin:
        for row in ADMIN_COMMAND_BUTTONS_LAYOUT_USER_SPEC:
            markup.add(*[types.KeyboardButton(text) for text in row])
    else:
        for row in COMMAND_BUTTONS_LAYOUT_USER_SPEC:
            markup.add(*[types.KeyboardButton(text) for text in row])

    safe_send_message(message.chat.id, welcome_msg, reply_markup=markup)

@bot.message_handler(content_types=['document'])
def handle_file_upload(message):
    """Enhanced file upload handler"""
    user_id = message.from_user.id

    # Check if bot is locked
    if bot_locked and user_id not in admin_ids:
        safe_reply_to(message, "Bot is currently locked. Please try again later.")
        return

    # Check file upload limits
    current_count = get_user_file_count(user_id)
    max_allowed = get_user_file_limit(user_id)

    if current_count >= max_allowed:
        safe_reply_to(message, f"File limit reached! You can upload maximum {max_allowed} files.")
        return

    file_info = bot.get_file(message.document.file_id)
    file_name = message.document.file_name or f"file_{int(time.time())}"
    file_ext = os.path.splitext(file_name)[1].lower()

    # Check file size before download
    if message.document.file_size > 10 * 1024 * 1024:
        safe_reply_to(message, "File too large! Maximum size is 10MB.")
        return

    try:
        # Send processing message
        processing_msg = safe_reply_to(message, f"Processing {file_name}...")

        # Download file
        if file_info.file_path is None:
            safe_reply_to(message, "File Download Failed - Unable to retrieve file path")
            return
        downloaded_file = bot.download_file(file_info.file_path)

        # Save to user folder
        user_folder = get_user_folder(user_id)
        file_path = os.path.join(user_folder, file_name)
        
        with open(file_path, 'wb') as f:
            f.write(downloaded_file)

        safe_edit_message(processing_msg.chat.id, processing_msg.message_id, 
                         f"Processing {file_name}...")

        # Add to user files list
        if user_id not in user_files:
            user_files[user_id] = []

        # Determine file type
        file_type = 'executable' if file_ext in {'.py', '.js', '.java', '.cpp', '.c', '.sh', '.rb', '.go', '.rs', '.php', '.bat', '.ps1'} else 'hosted'

        # Remove old entry if exists
        user_files[user_id] = [(fn, ft) for fn, ft in user_files[user_id] if fn != file_name]
        user_files[user_id].append((file_name, file_type))

        # Save to database
        try:
            conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
            c = conn.cursor()
            c.execute('INSERT OR REPLACE INTO user_files (user_id, file_name, file_type) VALUES (?, ?, ?)',
                     (user_id, file_name, file_type))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Database error saving file info: {e}")

        # Execute or host the file
        if file_type == 'executable':
            success_msg = f"✅ {file_name} uploaded successfully!\n\nType: {file_type}\nManual start required\n\nUse 'Check Files' to manage your file."
        else:
            success_msg = f"✅ {file_name} hosted successfully!\n\nFile: {file_name}\nType: {file_type}\n\nYour file is now accessible via the bot!"
            
        safe_edit_message(processing_msg.chat.id, processing_msg.message_id, success_msg)

    except Exception as e:
        logger.error(f"File upload error: {e}")
        safe_reply_to(message, f"Upload Failed - Error processing file: {str(e)}")

# --- Button Handlers ---
@bot.message_handler(func=lambda message: message.text == "Upload File")
def upload_file_button(message):
    if bot_locked and message.from_user.id not in admin_ids:
        safe_reply_to(message, "Bot is currently locked. Access denied.")
        return
    safe_reply_to(message, "Universal File Upload\n\nSend me any file to upload!\nSupported: 30+ file types\nExecutable: Python, JS, Java, C/C++, Go, Rust, PHP, etc.\nHosted: Documents, Images, Videos, Archives")

@bot.message_handler(func=lambda message: message.text == "Check Files")
def check_files_button(message):
    if bot_locked and message.from_user.id not in admin_ids:
        safe_reply_to(message, "Bot is currently locked. Access denied.")
        return
        
    user_id = message.from_user.id
    files = user_files.get(user_id, [])

    if not files:
        safe_reply_to(message, "Your Files\n\nNo files uploaded yet.\nUpload any file type to begin!")
        return

    files_text = "Your Files:\n\nClick on any file to manage it:\n\n"
    markup = types.InlineKeyboardMarkup(row_width=1)

    for i, (file_name, file_type) in enumerate(files, 1):
        if file_type == 'executable':
            status = "Running" if is_bot_running(user_id, file_name) else "Stopped"
            files_text += f"{i}. {file_name} ({file_type})\n   Status: {status}\n\n"
        else:
            status = "Hosted"
            files_text += f"{i}. {file_name} ({file_type})\n   Status: {status}\n\n"

        # Add control button for each file
        markup.add(types.InlineKeyboardButton(
            f"{file_name} - {status}", 
            callback_data=f'control_{user_id}_{file_name}'
        ))

    files_text += "Management Options:\n• Start/Stop executable files\n• Delete files\n• View execution logs\n• Restart running files"

    safe_reply_to(message, files_text, reply_markup=markup)

@bot.message_handler(func=lambda message: message.text == "Bot Speed")
def bot_speed_button(message):
    start_time = time.time()
    msg = safe_reply_to(message, "Testing speed...")
    response_time = round((time.time() - start_time) * 1000, 2)

    speed_text = f"Universal File Host Performance:\n\n"
    speed_text += f"Response Time: {response_time}ms\n"
    speed_text += f"CPU Usage: Optimized\n"
    speed_text += f"Memory: Efficient\n"
    speed_text += f"Network: High Speed\n"
    speed_text += f"Files Supported: 30+ types\n\n"
    speed_text += f"All systems operational!"

    safe_edit_message(msg.chat.id, msg.message_id, speed_text)

@bot.message_handler(func=lambda message: message.text == "Statistics")
def statistics_button(message):
    user_id = message.from_user.id
    total_users = len(active_users)
    total_files = sum(len(files) for files in user_files.values())
    running_scripts = len(bot_scripts)

    stats_text = f"Universal File Host Statistics:\n\n"
    stats_text += f"Active Users: {total_users}\n"
    stats_text += f"Total Files: {total_files}\n"
    stats_text += f"Running Scripts: {running_scripts}\n"
    stats_text += f"Your Files: {get_user_file_count(user_id)}\n"
    stats_text += f"Your Limit: {get_user_file_limit(user_id)}\n\n"
    stats_text += f"Features:\n"
    stats_text += f"30+ file type support\n"
    stats_text += f"Multi-language execution\n"
    stats_text += f"Real-time monitoring\n"
    stats_text += f"Secure file hosting\n"
    stats_text += f"Auto dependency installation"

    safe_reply_to(message, stats_text)

@bot.message_handler(func=lambda message: message.text == "Updates Channel")
def updates_channel_button(message):
    safe_reply_to(message, f"Updates Channel\n\nStay updated:\n{UPDATE_CHANNEL}\nGet latest features and news!")

@bot.message_handler(func=lambda message: message.text == "Contact Owner")
def contact_owner_button(message):
    safe_reply_to(message, f"Contact Owner\n\nOwner: {YOUR_USERNAME}\nChannel: {UPDATE_CHANNEL}\nFor support and inquiries!")

@bot.message_handler(func=lambda message: message.text == "Subscriptions")
def subscriptions_button(message):
    user_id = message.from_user.id
    if user_id not in admin_ids:
        safe_reply_to(message, "Access Denied - Admin privileges required!")
        return

    subs_text = "Subscription Management\n\n"
    subs_text += "Commands:\n"
    subs_text += "/addsub <user_id> <days> - Add subscription\n"
    subs_text += "/removesub <user_id> - Remove subscription\n"
    subs_text += "/checksub <user_id> - Check subscription status\n\n"
    subs_text += "Current Subscriptions:\n"

    active_subs = 0
    for user_id_sub, sub_info in user_subscriptions.items():
        if sub_info['expiry'] > datetime.now():
            active_subs += 1

    subs_text += f"Active: {active_subs} users"
    safe_reply_to(message, subs_text)

@bot.message_handler(func=lambda message: message.text == "Broadcast")
def broadcast_button(message):
    user_id = message.from_user.id
    if user_id not in admin_ids:
        safe_reply_to(message, "Access Denied - Admin privileges required!")
        return

    broadcast_text = "Broadcast Message\n\n"
    broadcast_text += "Send your broadcast message in the next message.\n"
    broadcast_text += "Current active users: " + str(len(active_users)) + "\n\n"
    broadcast_text += "Reply to this message with your broadcast content."
    
    safe_reply_to(message, broadcast_text)

@bot.message_handler(func=lambda message: message.text == "Lock Bot")
def lock_bot_button(message):
    user_id = message.from_user.id
    if user_id not in admin_ids:
        safe_reply_to(message, "Access Denied - Admin privileges required!")
        return

    global bot_locked
    bot_locked = not bot_locked
    status = "LOCKED" if bot_locked else "UNLOCKED"
    
    lock_text = f"Bot Lock Status Changed\n\n"
    lock_text += f"Status: {status}\n"
    lock_text += f"Admin: {message.from_user.first_name}\n"
    lock_text += f"Time: {datetime.now().strftime('%H:%M:%S')}\n\n"
    
    if bot_locked:
        lock_text += "Non-admin users are now blocked from using the bot."
    else:
        lock_text += "All users can now use the bot normally."
    
    safe_reply_to(message, lock_text)

@bot.message_handler(func=lambda message: message.text == "Running All Code")
def running_code_button(message):
    user_id = message.from_user.id
    if user_id not in admin_ids:
        safe_reply_to(message, "Access Denied - Admin privileges required!")
        return

    if not bot_scripts:
        safe_reply_to(message, "Running Code Monitor\n\nNo scripts currently running.\nAll systems idle.")
        return

    running_text = f"Running Code Monitor\n\n"
    running_text += f"Active Scripts: {len(bot_scripts)}\n\n"

    for script_key, script_info in bot_scripts.items():
        user_id_script = script_info['user_id']
        file_name = script_info['file_name']
        language = script_info.get('language', 'Unknown')
        start_time = script_info['start_time'].strftime("%H:%M:%S")
        
        running_text += f"{file_name} ({language})\n"
        running_text += f"User: {user_id_script}\n"
        running_text += f"Started: {start_time}\n"
        running_text += f"PID: {script_info['process'].pid}\n\n"

    safe_reply_to(message, running_text)

@bot.message_handler(func=lambda message: message.text == "Admin Panel")
def admin_panel_button(message):
    user_id = message.from_user.id
    if user_id not in admin_ids:
        safe_reply_to(message, "Access Denied - Admin privileges required!")
        return

    admin_text = f"Admin Panel\n\n"
    admin_text += f"System Status:\n"
    admin_text += f"Active Users: {len(active_users)}\n"
    admin_text += f"Total Files: {sum(len(files) for files in user_files.values())}\n"
    admin_text += f"Running Scripts: {len(bot_scripts)}\n"
    admin_text += f"Bot Status: {'Locked' if bot_locked else 'Unlocked'}\n\n"
    admin_text += f"Available Commands:\n"
    admin_text += f"/addsub <user_id> <days> - Add subscription\n"
    admin_text += f"/removesub <user_id> - Remove subscription\n"
    admin_text += f"/broadcast - Send broadcast message\n"
    admin_text += f"/addadmin <user_id> - Add admin\n"
    admin_text += f"/removeadmin <user_id> - Remove admin\n\n"
    admin_text += f"Use the admin buttons for quick actions!"

    safe_reply_to(message, admin_text)

# --- Inline Button Callback Handlers ---
@bot.callback_query_handler(func=lambda call: call.data.startswith('control_'))
def handle_file_control(call):
    """Handle file control buttons (start/stop/logs/delete)"""
    try:
        parts = call.data.split('_', 2)
        if len(parts) != 3:
            bot.answer_callback_query(call.id, "Invalid button data")
            return
            
        _, user_id_str, file_name = parts
        user_id = int(user_id_str)
        
        # Check if user owns this file
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "Access denied!")
            return
            
        # Get file info
        user_files_list = user_files.get(user_id, [])
        file_info = next((f for f in user_files_list if f[0] == file_name), None)
        
        if not file_info:
            bot.answer_callback_query(call.id, "File not found!")
            return
            
        file_name, file_type = file_info
        
        # Create control buttons based on file type
        markup = types.InlineKeyboardMarkup(row_width=2)
        
        if file_type == 'executable':
            is_running = is_bot_running(user_id, file_name)
            
            if is_running:
                markup.add(
                    types.InlineKeyboardButton("Stop", callback_data=f'stop_{user_id}_{file_name}'),
                    types.InlineKeyboardButton("Restart", callback_data=f'restart_{user_id}_{file_name}')
                )
            else:
                markup.add(
                    types.InlineKeyboardButton("Start", callback_data=f'start_{user_id}_{file_name}'),
                    types.InlineKeyboardButton("Logs", callback_data=f'logs_{user_id}_{file_name}')
                )
        else:
            markup.add(
                types.InlineKeyboardButton("Download", callback_data=f'download_{user_id}_{file_name}')
            )
        
        # Common buttons for all files
        markup.add(
            types.InlineKeyboardButton("Delete", callback_data=f'delete_{user_id}_{file_name}'),
            types.InlineKeyboardButton("Back", callback_data=f'back_files_{user_id}')
        )
        
        # Show file details
        status = "Running" if file_type == 'executable' and is_bot_running(user_id, file_name) else "Stopped" if file_type == 'executable' else "Hosted"
        
        control_text = f"File Control Panel\n\n"
        control_text += f"File: {file_name}\n"
        control_text += f"Type: {file_type}\n"
        control_text += f"Status: {status}\n"
        control_text += f"Owner: {user_id}\n\n"
        control_text += f"Choose an action:"
        
        # Only edit if content actually changed
        try:
            bot.edit_message_text(
                control_text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup
            )
        except Exception as e:
            if "message is not modified" not in str(e):
                raise e
        
        bot.answer_callback_query(call.id, f"Control panel for {file_name}")
        
    except Exception as e:
        if "message is not modified" not in str(e):
            logger.error(f"Error in file control handler: {e}")
            bot.answer_callback_query(call.id, "Error occurred!")

@bot.callback_query_handler(func=lambda call: call.data.startswith('start_'))
def handle_start_file(call):
    """Handle start file button"""
    try:
        parts = call.data.split('_', 2)
        user_id = int(parts[1])
        file_name = parts[2]
        
        # Check permissions
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "Access denied!")
            return
            
        # Get file path
        user_folder = get_user_folder(user_id)
        file_path = os.path.join(user_folder, file_name)
        
        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, "File not found!")
            return
            
        # Check if already running
        if is_bot_running(user_id, file_name):
            bot.answer_callback_query(call.id, "Already running!")
            return
            
        # Start the script
        success, result = execute_script(user_id, file_path, call.message)
        
        if success:
            bot.answer_callback_query(call.id, "Started successfully!")
            # Refresh the control panel
            call.data = f'control_{user_id}_{file_name}'
            handle_file_control(call)
        else:
            bot.answer_callback_query(call.id, f"Start failed: {result}")
            
    except Exception as e:
        logger.error(f"Error starting file: {e}")
        bot.answer_callback_query(call.id, "Error occurred!")

@bot.callback_query_handler(func=lambda call: call.data.startswith('stop_'))
def handle_stop_file(call):
    """Handle stop file button"""
    try:
        parts = call.data.split('_', 2)
        user_id = int(parts[1])
        file_name = parts[2]
        
        # Check permissions
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "Access denied!")
            return
            
        # Stop the script
        script_key = f"{user_id}_{file_name}"
        script_info = bot_scripts.get(script_key)
        
        if script_info and script_info.get('process'):
            try:
                process = script_info['process']
                process.terminate()
                process.wait(timeout=5)
                del bot_scripts[script_key]
                
                bot.answer_callback_query(call.id, "Stopped successfully!")
                # Refresh the control panel
                call.data = f'control_{user_id}_{file_name}'
                handle_file_control(call)
            except Exception as e:
                bot.answer_callback_query(call.id, f"Stop failed: {str(e)}")
        else:
            bot.answer_callback_query(call.id, "Not running!")
            
    except Exception as e:
        logger.error(f"Error stopping file: {e}")
        bot.answer_callback_query(call.id, "Error occurred!")

@bot.callback_query_handler(func=lambda call: call.data.startswith('restart_'))
def handle_restart_file(call):
    """Handle restart file button"""
    try:
        parts = call.data.split('_', 2)
        user_id = int(parts[1])
        file_name = parts[2]
        
        # Check permissions
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "Access denied!")
            return
            
        # Stop first
        script_key = f"{user_id}_{file_name}"
        script_info = bot_scripts.get(script_key)
        
        if script_info and script_info.get('process'):
            try:
                process = script_info['process']
                process.terminate()
                process.wait(timeout=5)
                del bot_scripts[script_key]
            except:
                pass
        
        # Start again
        user_folder = get_user_folder(user_id)
        file_path = os.path.join(user_folder, file_name)
        
        if os.path.exists(file_path):
            success, result = execute_script(user_id, file_path, call.message)
            
            if success:
                bot.answer_callback_query(call.id, "Restarted successfully!")
                # Refresh the control panel
                call.data = f'control_{user_id}_{file_name}'
                handle_file_control(call)
            else:
                bot.answer_callback_query(call.id, f"Restart failed: {result}")
        else:
            bot.answer_callback_query(call.id, "File not found!")
            
    except Exception as e:
        logger.error(f"Error restarting file: {e}")
        bot.answer_callback_query(call.id, "Error occurred!")

@bot.callback_query_handler(func=lambda call: call.data.startswith('logs_'))
def handle_show_logs(call):
    """Handle show logs button"""
    try:
        parts = call.data.split('_', 2)
        user_id = int(parts[1])
        file_name = parts[2]
        
        # Check permissions
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "Access denied!")
            return
            
        # Find log file
        script_key = f"{user_id}_{file_name}"
        script_info = bot_scripts.get(script_key)
        
        if script_info and 'log_file_path' in script_info:
            log_file_path = script_info['log_file_path']
            
            if os.path.exists(log_file_path):
                try:
                    with open(log_file_path, 'r', encoding='utf-8') as f:
                        logs = f.read()
                    
                    if logs.strip():
                        if len(logs) > 4000:
                            logs = "..." + logs[-4000:]
                        
                        logs_text = f"Execution Logs - {file_name}\n\n```\n{logs}\n```"
                    else:
                        logs_text = f"Execution Logs - {file_name}\n\nNo output yet"
                        
                    bot.send_message(call.message.chat.id, logs_text, parse_mode='Markdown')
                    bot.answer_callback_query(call.id, "Logs sent!")
                    
                except Exception as e:
                    bot.answer_callback_query(call.id, f"Error reading logs: {str(e)}")
            else:
                bot.answer_callback_query(call.id, "Log file not found!")
        else:
            bot.answer_callback_query(call.id, "No logs available!")
            
    except Exception as e:
        logger.error(f"Error showing logs: {e}")
        bot.answer_callback_query(call.id, "Error occurred!")

@bot.callback_query_handler(func=lambda call: call.data.startswith('download_'))
def handle_download_file(call):
    """Handle download file button for hosted files"""
    try:
        parts = call.data.split('_', 2)
        user_id = int(parts[1])
        file_name = parts[2]
        
        # Check permissions
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "Access denied!")
            return
            
        # Get file path
        user_folder = get_user_folder(user_id)
        file_path = os.path.join(user_folder, file_name)
        
        if not os.path.exists(file_path):
            bot.answer_callback_query(call.id, "File not found!")
            return
            
        # Send the file
        with open(file_path, 'rb') as f:
            bot.send_document(call.message.chat.id, f, caption=f"{file_name}")
        
        bot.answer_callback_query(call.id, "File sent!")
        
    except Exception as e:
        logger.error(f"Error downloading file: {e}")
        bot.answer_callback_query(call.id, "Error occurred!")

@bot.callback_query_handler(func=lambda call: call.data.startswith('delete_'))
def handle_delete_file(call):
    """Handle delete file button"""
    try:
        parts = call.data.split('_', 2)
        user_id = int(parts[1])
        file_name = parts[2]
        
        # Check permissions
        if call.from_user.id != user_id and call.from_user.id not in admin_ids:
            bot.answer_callback_query(call.id, "Access denied!")
            return
            
        # Stop if running
        script_key = f"{user_id}_{file_name}"
        if script_key in bot_scripts:
            try:
                process = bot_scripts[script_key]['process']
                process.terminate()
                del bot_scripts[script_key]
            except:
                pass
        
        # Delete file
        user_folder = get_user_folder(user_id)
        file_path = os.path.join(user_folder, file_name)
        
        if os.path.exists(file_path):
            os.remove(file_path)
        
        # Remove from database
        if user_id in user_files:
            user_files[user_id] = [(fn, ft) for fn, ft in user_files[user_id] if fn != file_name]
        
        try:
            conn = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
            c = conn.cursor()
            c.execute('DELETE FROM user_files WHERE user_id = ? AND file_name = ?', (user_id, file_name))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Database error deleting file: {e}")
        
        bot.answer_callback_query(call.id, f"{file_name} deleted!")
        
        # Go back to files list
        call.data = f'back_files_{user_id}'
        handle_back_to_files(call)
        
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
        bot.answer_callback_query(call.id, "Error occurred!")

@bot.callback_query_handler(func=lambda call: call.data.startswith('back_files_'))
def handle_back_to_files(call):
    """Handle back to files list button"""
    try:
        parts = call.data.split('_', 2)
        user_id = int(parts[2])
        
        files = user_files.get(user_id, [])
        
        if not files:
            files_text = "Your Files\n\nNo files uploaded yet.\nUpload any file type to begin!"
            markup = None
        else:
            files_text = "Your Files:\n\nClick on any file to manage it:\n\n"
            markup = types.InlineKeyboardMarkup(row_width=1)
            
            for i, (file_name, file_type) in enumerate(files, 1):
                if file_type == 'executable':
                    status = "Running" if is_bot_running(user_id, file_name) else "Stopped"
                    files_text += f"{i}. {file_name} ({file_type})\n   Status: {status}\n\n"
                else:
                    status = "Hosted"
                    files_text += f"{i}. {file_name} ({file_type})\n   Status: {status}\n\n"
                
                markup.add(types.InlineKeyboardButton(
                    f"{file_name} - {status}", 
                    callback_data=f'control_{user_id}_{file_name}'
                ))
            
            files_text += "Management Options:\n• Start/Stop executable files\n• Delete files\n• View execution logs\n• Restart running files"
        
        bot.edit_message_text(
            files_text,
            call.message.chat.id,
            call.message.message_id,
            reply_markup=markup
        )
        
        bot.answer_callback_query(call.id, "Files list updated!")
        
    except Exception as e:
        logger.error(f"Error going back to files: {e}")
        bot.answer_callback_query(call.id, "Error occurred!")

# --- Catch all handler for unsupported messages ---
@bot.message_handler(func=lambda message: True)
def handle_all_messages(message):
    safe_reply_to(message, "Use the menu buttons or send /start for help.")

# --- Initialize and Start Bot ---
def cleanup_on_exit():
    """Cleanup function called on exit"""
    logger.info("Performing cleanup on exit...")
    
    # Stop all running scripts
    for script_key, script_info in bot_scripts.items():
        try:
            process = script_info.get('process')
            if process and process.poll() is None:
                process.terminate()
                logger.info(f"Terminated script: {script_key}")
        except Exception as e:
            logger.error(f"Error terminating script {script_key}: {e}")

if __name__ == "__main__":
    # Register cleanup function
    atexit.register(cleanup_on_exit)
    
    # Initialize database and load data
    init_db()
    load_data()
    
    # Start Flask keep-alive server
    keep_alive()
    
    logger.info("Universal File Host Bot starting...")
    logger.info(f"Owner ID: {OWNER_ID}")
    logger.info(f"Admin ID: {ADMIN_ID}")
    logger.info(f"Upload directory: {UPLOAD_BOTS_DIR}")
    
    try:
        # Test bot connection first
        bot_info = bot.get_me()
        logger.info(f"Bot connected successfully: @{bot_info.username}")
        print(f"Bot connected successfully: @{bot_info.username}")
        
        # Start polling with error handling
        bot.infinity_polling(timeout=10, long_polling_timeout=5, none_stop=True, interval=0)
    except Exception as e:
        logger.error(f"Bot error: {e}")
        print(f"Bot connection failed: {e}")
        sys.exit(1)
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MySQL Configuration
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', '127.0.0.1'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', 'root@123456'),
    'database': os.getenv('MYSQL_DATABASE', 'movies')
}

# Ollama Configuration
OLLAMA_API_URL = os.getenv('OLLAMA_API_URL', 'http://ollama_ip:11434')
OLLAMA_CHAT_MODEL = os.getenv('OLLAMA_CHAT_MODEL', 'qwen2.5:32b')
OLLAMA_CODE_MODEL = os.getenv('OLLAMA_CODE_MODEL', 'qwen2.5-coder:32b')

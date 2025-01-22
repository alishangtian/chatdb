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

# MongoDB Configuration
MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'movies')
MONGODB_USER = os.getenv('MONGODB_USER', 'root')
MONGODB_PASSWORD = os.getenv('MONGODB_PASSWORD', 'root@123456')

# Build MongoDB connection string with authentication
if MONGODB_USER and MONGODB_PASSWORD:
    # Parse existing URI
    from urllib.parse import urlparse
    parsed_uri = urlparse(MONGODB_URI)
    # Reconstruct URI with authentication
    MONGODB_URI = f"mongodb://{MONGODB_USER}:{MONGODB_PASSWORD}@{parsed_uri.hostname}:{parsed_uri.port or 27017}"

# Ollama Configuration
OLLAMA_API_URL = os.getenv('OLLAMA_API_URL', 'http://10.234.20.35:11434')
OLLAMA_CHAT_MODEL = os.getenv('OLLAMA_CHAT_MODEL', 'qwen2.5:32b')
OLLAMA_CODE_MODEL = os.getenv('OLLAMA_CODE_MODEL', 'qwen2.5-coder:32b')

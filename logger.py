import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

class Logger:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialize_logger()
        return cls._instance
    
    def _initialize_logger(self):
        """Initialize the logger with custom configuration"""
        # Create logs directory if it doesn't exist
        log_dir = "/app/logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        # Generate log filename with current date
        current_date = datetime.now().strftime("%Y-%m-%d")
        log_file = os.path.join(log_dir, f"nlp2sql_{current_date}.log")
        
        # Create logger
        self.logger = logging.getLogger("NLP2SQL")
        self.logger.setLevel(logging.INFO)
        
        # Create handlers
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # File handler with rotation (max 10MB per file, keep 5 backup files)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        
        # Create formatters and add it to handlers
        log_format = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(module)s:%(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        console_handler.setFormatter(log_format)
        file_handler.setFormatter(log_format)
        
        # Add handlers to the logger
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
    
    @classmethod
    def get_logger(cls):
        """Get the logger instance"""
        if cls._instance is None:
            cls()
        return cls._instance.logger

# Create a convenience function to get logger
def get_logger():
    return Logger.get_logger()

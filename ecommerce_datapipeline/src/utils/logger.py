import sys 
from pathlib import Path
from loguru import logger
from src.utils.config import settings

def setup_logger():
    """
    Configure loguru logger for the etl pipeline. sets up both file and console logging with 
    appropiate formats
    """
    logger.remove() #  Remove default logger

    Path(settings.log_path).mkdir(parents=True, exist_ok=True)

    # added file logging info level and above
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
        colorize=True
    )

    # file logging debug level and above
    logger.add(
        f"{settings.log_path}/etl.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level= "DEBUG",
        rotation="10 MB",
        retention="1 week",
        compression="zip"
    )
    # Error file logging (ERROR level and above)
    logger.add(
        f"{settings.log_path}/errors.log",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="ERROR",
        rotation="5 MB",
        retention="1 month"
    )
    
    logger.info("Logger configured successfully")
    return logger
# Initialize logger
etl_logger = setup_logger()






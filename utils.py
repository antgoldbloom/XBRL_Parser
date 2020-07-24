import logging
from pathlib import Path

def setup_logging(log_folder,log_file,logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter("%(levelname)s - %(message)s") 

    Path(log_folder).mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(f"{log_folder}{log_file}") 
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

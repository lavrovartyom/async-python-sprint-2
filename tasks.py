import os

import requests

from util.logger_config import setup_logging

logger = setup_logging()


def file_system_task():
    try:
        if not os.path.exists("test_dir"):
            os.mkdir("test_dir")
        yield
        if os.path.exists("test_dir"):
            os.rmdir("test_dir")
        yield
    except Exception as e:
        logger.error(f"File system task error: {e}")
    logger.info("File system task completed.")
    yield


def file_task():
    logger.info("File task started.")
    try:
        with open("test_file.txt", "w") as f:
            f.write("Hello, world!")
        yield
        with open("test_file.txt", "r") as f:
            content = f.read()
            logger.info(f"Read from file: {content}")
        yield
        os.remove("test_file.txt")
    except Exception as e:
        logger.error(f"File task error: {e}")
    logger.info("File task completed.")
    yield


def network_task():
    logger.info("Network task started.")
    try:
        response = requests.get("https://www.google.com/")
        logger.info(f"GET request status: {response.status_code}")
    except Exception as e:
        logger.error(f"Network task error: {e}")
    logger.info("Network task completed.")
    yield

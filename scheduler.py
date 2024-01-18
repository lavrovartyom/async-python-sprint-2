import concurrent.futures
import json
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

from job import Job
from util.logger_config import setup_logging

logger = setup_logging()


class Scheduler:
    def __init__(self, pool_size=10):
        self.pool_size = pool_size
        self.tasks_queue = deque()
        self.futures = []
        self.executor = ThreadPoolExecutor(max_workers=self.pool_size)

    def schedule(self, job):
        self.tasks_queue.append(job)
        logger.info(f"Job {job.__class__.__name__} scheduled.")

    def run(self):
        while self.tasks_queue:
            job = self.tasks_queue.popleft()
            future = self.executor.submit(job.run)
            self.futures.append(future)

        for future in as_completed(self.futures):
            future.result()

        self.executor.shutdown()
        logger.info("All tasks have been completed.")

    def stop(self):
        self.is_running = False
        self.executor.shutdown(wait=True)
        logger.info("Scheduler stopped.")

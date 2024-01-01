import concurrent.futures
import time
from collections import deque

from job import Job
from util.logger_config import setup_logging

logger = setup_logging()


class Scheduler:
    def __init__(self, pool_size: int = 10):
        self.pool_size = pool_size
        self.tasks_queue = deque()
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.pool_size
        )
        self.is_running = False

    def schedule(self, job: Job):
        self.tasks_queue.append(job)
        logger.info(f"Job {job.__class__.__name__} scheduled.")

    def run(self):
        self.is_running = True
        while self.is_running:
            if self.tasks_queue:
                job = self.tasks_queue.popleft()
                self.executor.submit(job.run)
            time.sleep(1)

    def stop(self):
        self.is_running = False
        self.executor.shutdown(wait=True)
        logger.info("Scheduler stopped.")

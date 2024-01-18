import json
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from util.logger_config import setup_logging

logger = setup_logging()


class Scheduler:
    def __init__(self, pool_size=10, state_file="scheduler_state.json"):
        self.pool_size = pool_size
        self.tasks_queue = deque()
        self.futures = []
        self.executor = ThreadPoolExecutor(max_workers=self.pool_size)
        self.completed_jobs = []
        self.state_file = state_file

    def schedule(self, job):
        self.tasks_queue.append(job)
        logger.info(f"Job {job.__class__.__name__} scheduled.")

    def run(self):
        while self.tasks_queue:
            job = self.tasks_queue.popleft()
            future = self.executor.submit(job.run)
            future.add_done_callback(lambda f, job=job: self.job_completed(job))

        self.executor.shutdown(wait=True)
        logger.info("All tasks have been completed.")
        self.save_state()

    def job_completed(self, job):
        self.completed_jobs.append(job.to_dict())

    def save_state(self):
        with open(self.state_file, "w") as f:
            json.dump({"tasks": self.completed_jobs}, f)

    def stop(self):
        self.is_running = False
        self.executor.shutdown(wait=True)
        logger.info("Scheduler stopped.")

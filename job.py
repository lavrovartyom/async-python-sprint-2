import threading
import time

from util.logger_config import setup_logging

logger = setup_logging()


class Job:
    def __init__(
        self, action, start_at=None, max_working_time=None, tries=0, dependencies=None
    ):
        self.action = action
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies if dependencies is not None else []
        self.coro = None
        self.start_time = None
        self.is_running = False
        self.completed = threading.Event()

    def run(self):
        if self.start_at and time.time() < self.start_at:
            time.sleep(self.start_at - time.time())

        if self.dependencies:
            for dep in self.dependencies:
                dep.completed.wait()

        if not self.coro:
            self.coro = self.action()
            self.start_time = time.time()
            self.is_running = True

        try:
            while True:
                next(self.coro)
        except StopIteration:
            pass
        except Exception as e:
            self.tries -= 1
            if self.tries > 0:
                self.coro = None
                return self.run()
            else:
                logger.error(f"Job failed: {self.action.__name__} - {e}")

        self.is_running = False
        self.completed.set()
        logger.info(f"Job completed: {self.action.__name__}")

    def is_completed(self):
        return self.completed.is_set()

    def stop(self):
        self.is_running = False
        self.coro = None
        logger.info(f"Job stopped: {self.action.__name__}")

    def to_dict(self):
        return {
            "action": self.action.__name__,
            "start_at": self.start_at,
            "max_working_time": self.max_working_time,
            "tries": self.tries,
            "completed": self.is_completed(),
        }

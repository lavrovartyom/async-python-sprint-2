from job import Job
from scheduler import Scheduler
from tasks import file_system_task, file_task, network_task

# Создание экземпляров Job для каждой задачи
file_system_job = Job(file_system_task)
file_job = Job(file_task)
network_job = Job(network_task)

# Добавление зависимостей
file_job.dependencies.append(file_system_job)
network_job.dependencies.append(file_job)


def main():
    scheduler = Scheduler()
    scheduler.schedule(file_system_job)
    scheduler.schedule(file_job)
    scheduler.schedule(network_job)
    scheduler.run()
    scheduler.stop()


if __name__ == "__main__":
    main()

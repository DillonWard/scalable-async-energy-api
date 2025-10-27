import os
from celery import Celery

redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')

celery = Celery(
    'energy_pipeline',
    broker=redis_url,
    backend=redis_url,
    include=['app.tasks']
)

celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_always_eager=False,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    result_expires=3600,
)

# celery.conf.beat_schedule = {
#     'aggregate-energy-data': {
#         'task': 'app.tasks.aggregate_energy_data',
#         'schedule': 300.0,
#     },
# }

if __name__ == '__main__':
    celery.start()
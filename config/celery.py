from celery import Celery
from config.settings import get_celery_config

app = Celery('energy_pipeline')
app.config_from_object(get_celery_config(), namespace='')

app.conf.update(
    include=[
        'app.tasks',
    ]
)

if __name__ == '__main__':
    app.start()
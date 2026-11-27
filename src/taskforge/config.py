import os

#Redis connection URL
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

POSTGRES_DSN = os.getenv(
    'POSTGRES_DSN',
    'postgresql://taskforge:taskforge_dev@localhost:5432/taskforge'
)
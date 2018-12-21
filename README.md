# Django2 example

start uvicorn
- uvicorn django2_example.asgi:application --port 8081

start celery

- concurrency 并发数
- celery -A django2_example worker -l info --concurrency 1

test url:

- http://localhost:8081/ops/add
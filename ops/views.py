from django.shortcuts import render

# Create your views here.

from django.http import HttpResponse
import time, asyncio
from .tasks import add


def index(request):
    print('mmp')
    # asyncio.sleep(2)
    time.sleep(2)
    print('success')
    return HttpResponse("Hello, world. You're at the polls index.")


def task_add(request):
    print('start exec task add...')
    t = add.apply_async((10, 10), serializer='json')
    r = t.get()
    print(r)
    print('success exec task add.')
    return HttpResponse(r)

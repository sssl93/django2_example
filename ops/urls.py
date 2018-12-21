from django.urls import path

from . import views

urlpatterns = [
    path('index', views.index, name='index'),
    path('add', views.task_add, name='add'),
]

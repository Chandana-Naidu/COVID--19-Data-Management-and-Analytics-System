from django.urls import path,include
from . import views

app_name= 'app'
urlpatterns = [
    path('', views.getdata, name="getdata"),
    path('fetch', views.fetch,name="fetch"),
    path('getdata', views.getdata,name="getdata"),
    path('visualize', views.visualize,name="visualize"),
    path('download', views.download,name="download"),
    path('districts/<int:state_id>',views.districts,name='districts'),
    path('contact', views.contact_view, name='contact'),
]
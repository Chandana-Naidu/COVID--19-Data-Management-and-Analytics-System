from django.contrib import admin
from django.urls import path, include
from django.shortcuts import render


urlpatterns = [
    path('', include('app.urls')),
    path('admin/', admin.site.urls),
    path('accounts/', include('allauth.urls')),
    path('accounts/profile/', include('app.urls')),
    path('app/',include('app.urls')),
    
]
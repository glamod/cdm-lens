""" URL Configuration for the site. """

__author__ = "William Tucker"
__date__ = "2019-10-03"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


from django.contrib import admin
from django.urls import path, include


urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('cdm_interface.urls')),
]

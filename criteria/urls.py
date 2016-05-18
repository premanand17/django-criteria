'''URLs for pydgin_auth and  django auth'''
from django.conf.urls import url
from django.contrib import admin
from django.conf import settings
from criteria import views


admin.autodiscover()

try:
    base_html_dir = settings.BASE_HTML_DIR
except AttributeError:
    base_html_dir = ''


# Registration URLs
urlpatterns = [url(r'^home/$',  views.criteria_home),

               ]

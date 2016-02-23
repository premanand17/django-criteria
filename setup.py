import os
from setuptools import setup, find_packages


# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))
ROOT = os.path.abspath(os.path.dirname(__file__))

setup(
    name='django-criteria',
    version='0.0.1',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    url='http://github.com/D-I-L/django-criteria',
    description='A Django app to provide build and manage criteria indexes',
    install_requires=["requests>=2.7.0", "Django>=1.8.4,<1.9"],
    classifiers=[
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.4',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
)

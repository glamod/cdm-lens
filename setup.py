""" A setuptools based setup module. """

__author__ = "William Tucker"
__date__ = "2019-10-03"
__copyright__ = "Copyright 2019 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level directory"


from setuptools import setup, find_packages


with open("README.md") as readme_file:
    long_description = readme_file.read()


setup(
    name = "glamod-lens",
    version = "0.0.1",
    description = "Django application which provides upload functionality.",
    author='William Tucker',
    author_email='william.tucker@stfc.ac.uk',
    url = "https://github.com/glamod/cdm-lens",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    include_package_data=True,
    packages = find_packages(),
    install_requires = [
        'django',
        'requests',
        'furl',
        'pandas',
    ],
    classifiers = [
        'Development Status :: 1 - Planning',
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
    ],
    zip_safe = False,
)

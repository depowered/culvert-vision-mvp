# flake8: noqa

from os import path as op
import io
from setuptools import (setup, find_namespace_packages)

here = op.abspath(op.dirname(__file__))
with io.open(op.join(here, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')
install_requires = [x.strip() for x in all_reqs if 'git+' not in x]

name='rastervision_culvert_vision'
version='0.21.2'
description='A Raster Vision plugin for detecting culvert locations in digital elevation models'

setup(
    name=name,
    version=version,
    description=description,
    url='https://github.com/depowered/culvert-vision-mvp',
    author='Devin Power',
    author_email='',
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
    keywords=
    'raster deep-learning ml computer-vision earth-observation geospatial geospatial-processing',
    packages=find_namespace_packages(exclude=['integration_tests*', 'tests*']),
    install_requires=install_requires,
    zip_safe=False
)

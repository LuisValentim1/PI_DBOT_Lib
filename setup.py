  
#!/usr/bin/env python

from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / 'README.md').read_text(encoding='utf-8')

setup(
      name='Libs',
      version='0.1',
      description='DBOT Libs',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Luís Valentim',
      author_email='lvalentim@ua.pt',
      url='https://github.com/LuisValentim1/PI_DBOT_Lib',
      packages=find_packages(),
      install_requires=['cassandra_driver==3.22.0']
)
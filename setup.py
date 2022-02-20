from setuptools import setup

setup(
   name='tscache',
   version='1.0.2',
   description='Time Series Data Cache',
   url='https://github.com/bodhion/tscache',
   author='bodhion',
   author_email='tscache@bodhion.com',
   license='MIT',
   packages=['tscache'],  
   install_requires=['msgpack'],
)
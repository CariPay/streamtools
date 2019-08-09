from distutils.core import setup

setup(
    name='Stream Tools',
    version='0.1dev',
    packages=['streamtools',],
    install_requires=[
        'aiohttp==3.4.4',
        'aiokafka==0.5.1',
        'requests==2.22.0',
        'aio-pika==5.6.0',
      ],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
)

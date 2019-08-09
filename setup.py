from distutils.core import setup

setup(
    name='Stream Tools',
    version='0.1dev',
    packages=['streamtools',],
    install_requires=[
        'aiokafka',
        'aio-pika',
        'aiohttp',
        'requests',
      ],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
)

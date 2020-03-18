from distutils.core import setup

setup(
    name='streamtools',
    version='0.1dev',
    packages=['streamtools',],
    install_requires=[
        'aiohttp==3.6.2',
        'aiokafka==0.5.1',
        'aio-pika==5.6.0',
      ],
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
)

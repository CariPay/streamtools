import asyncio
import json
import os
import time
import traceback
from abc import ABC, abstractmethod

from aiokafka import AIOKafkaConsumer
import aio_pika

from logger import log

HOST = os.environ['KAFKA_HOST']
ENCODING = os.environ['ENCODING']

RMQ_USER = os.environ['RMQ_USER']
RMQ_PASS = os.environ['RMQ_PASS']
RMQ_HOST = os.environ['RMQ_HOST']

class AsyncioQueue(asyncio.Queue):
    def __aiter__(self): return self
    async def __anext__(self): return await self.get()


class ConsumerABC(ABC):
    '''
    An abstract base class for defining consumer decorator loops
    for consuming input messages sent to the agent and messages
    produced internally.
    '''
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.loop = asyncio.get_event_loop()
        self.consumer = None
        self.a_init_ran = False
        self.a_init_ran_msg = f"Consumer object `a_init` method didn't run for {self}"

    async def a_init(self):
        self.a_init_ran = True

    def update_attr(self, **kwargs_from_agent):
        pass

    def __call__(self, func):
        return self._decorator(func)

    @abstractmethod
    def _decorator(self, func):
        async def wrapper(containing_class_self):
            '''
            Message processing loop decorator to be added to
            agent's `start` task.

            Note: this call includes an object argument (containing_class_self)
            with the expectation that it will be used inside a class to decorate
            a class method.
            '''
            while True:
                async for msg in self.consumer:
                    # Decorated function comes in here
                    await func(containing_class_self, msg)
        return wrapper


class KafkaConsumerLoop(ConsumerABC):
    QUEUE_TYPE = "Kafka"
    '''
    A class decorator for decorating methods inside a class (bound
    methods) that need to execute some code when a new Kafka topic
    message comes in.

    This object currently will not work for decorating an unbound
    method.
    '''
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.topic = self.kwargs["topic"]
        self.passed_from_agent = self.kwargs.get("passed_from_agent", "")
        self.consumer = AIOKafkaConsumer(
            self.topic,
            loop=self.loop, bootstrap_servers=HOST,
            key_deserializer=lambda v: v.decode(ENCODING),
            value_deserializer=lambda v: json.loads(v.decode(ENCODING)),
            #group_id="my-group"
        )

    def _decorator(self, func):
        async def wrapper(containing_class_self):
            """
            Message processing loop decorator to be added to `start` task.

            Note: this call includes an object argument (containing_class_self)
            with the expectation that it will be used inside a class to decorate
            a class method.
            """
            try:
                await self.consumer.start()
            except AssertionError as e:
                print(e, '\ncontinuing...\n')

            while True:
                async for msg in self.consumer:
                    log.info('Got a message')

                    try:
                        TOPIC_DICT = getattr(containing_class_self, self.passed_from_agent)
                        valid_raw_msg = (msg.key == TOPIC_DICT['uuid'])
                        log.debug(f"Agent uuid matches Kafka msg key received?: {valid_raw_msg}")
                    except AttributeError as e:
                        valid_raw_msg = False
                        log.debug(f"Message key wasn't passed to \"consumer decorator object\" from agent")

                    if valid_raw_msg:
                        msg = msg.value
                        msg = json.dumps(msg) \
                            if not isinstance(msg, (str, bytes, bytearray)) \
                            else msg

                        # Decorated function comes in here
                        await func(containing_class_self, msg)

        return wrapper


class AsyncIOConsumerLoop(ConsumerABC):
    QUEUE_TYPE = "AsyncIO"

    '''
    A class decorator for decorating methods inside a class (bound
    methods) that need to execute some code when a new AsyncIO queue
    message comes in.

    This object currently will not work for decorating an unbound
    method.
    '''
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.consumer = self.queue_ref = AsyncioQueue()

    def _decorator(self, func):
        async def wrapper(containing_class_self):
            """
            Message processing loop decorator to be added to `start` task.

            Note: this call includes an object argument (containing_class_self)
            with the expectation that it will be used inside a class to decorate
            a class method.
            """
            while True:
                async for msg in self.consumer:
                    log.info('Got a message')

                    # Decorated function comes in here
                    await func(containing_class_self, msg)
        return wrapper


class RMQIOConsumerLoop(ConsumerABC):
    QUEUE_TYPE = "RabbitMQ"

    '''
    A class decorator for decorating methods inside a class (bound
    methods) that need to execute some code when a new AsyncIO queue
    message comes in.

    This object currently will not work for decorating an unbound
    method.
    '''
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.routing_key = self.kwargs["topic"]

    async def a_init(self):
        self.connection = self.queue_ref = await aio_pika.connect_robust(
            f"amqp://{RMQ_USER}:{RMQ_PASS}@{RMQ_HOST}/", loop=self.loop
        )
        # Creating channel
        self.channel = await self.connection.channel()    # type: aio_pika.Channel

        # Declaring queue
        self.queue = await self.channel.declare_queue(
            self.routing_key,
            auto_delete=True
        )   # type: aio_pika.Queue
        self.a_init_ran = True

    def update_attr(self, **kwargs_from_agent):
        self.key = kwargs_from_agent[self.QUEUE_TYPE]
        self.routing_key += f"-{self.key[:5]}"

    def _decorator(self, func):
        async def wrapper(containing_class_self):
            """
            Message processing loop decorator to be added to `start` task.

            Note: this call includes an object argument (containing_class_self)
            with the expectation that it will be used inside a class to decorate
            a class method.
            """
            async with self.connection, \
                       self.queue.iterator() as consumer:
                # Cancel consuming after __aexit__
                async for msg in consumer:
                    async with msg.process():
                        msg = msg.body

                        # Decorated function comes in here
                        await func(containing_class_self, msg)

        return wrapper

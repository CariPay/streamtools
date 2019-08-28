import asyncio
import json
import logging
import os
import socket
import time
import traceback
from abc import ABC, abstractmethod

from aiohttp import web
from aiokafka import AIOKafkaConsumer
import aio_pika

from .libs import log, clean_route_string, get_free_port, AsyncioQueue, ClassRouteTableDef
from .libs import ENCODING, RMQ_USER, RMQ_PASS, RMQ_HOST, KAFKA_HOST


class ConsumerABC(ABC):
    '''
    An abstract base class for defining consumer decorator loops
    for consuming input messages sent to the agent and messages
    produced internally.
    '''
    def __init__(self, queue_name, queues_labels={}, **kwargs):
        self.queue_name = queue_name
        self.queues_labels = queues_labels
        self.loop = asyncio.get_event_loop()
        self.consumer = None
        self.a_init_ran = False
        self.a_init_ran_msg = f"Consumer object `a_init` method didn't run for {self}"

    async def a_init(self):
        self.a_init_ran = True

    def add_agent_uuid(self, **kwargs_from_agent):
        pass

    def __call__(self, string="", override=False, *args, **kwargs):
        queue = ""
        if self.queue_name:
            assert self.queue_name in self.queues_labels, f"'queue_name' arg must be one of {tuple(self.queues_labels.keys())}"
            queue = self.queues_labels[self.queue_name]["queue"]

        route = queue if not override else string
        route = queue if not route else route
        route = string if not route else route
        assert route, "Internal route not set by HandleMsgs class or included as a decorator arg."
        route = clean_route_string(route)

        log.info(f"{self} routing on '{route}'")
        if (string and string != route) and isinstance(self, HTTPConsumerLoop):
            print(f"Note: cosmetic route '{string}' arg passed is not the same as internal route '{route}' being used.", \
                        "\n (use `override=True` arg on route decorator to change this)")

        return self._decorator(route, *args, **kwargs)

    @abstractmethod
    def _decorator(self, string):
        def wrapper(func):
            async def wrapped(*args):
                '''
                Message processing loop decorator to be added to
                agent's `start` task.
                '''
                while True:
                    async for msg in self.consumer:
                        # Decorated function comes in here
                        await func(*args, msg)
            return wrapped
        return wrapper



class KafkaConsumerLoop(ConsumerABC):
    '''
    Uses the `AIOKafkaConsumer` decorator class from
    the aiokafka library to consume messages from a
    Kafka broker.

    This object currently will not work for decorating
    an unbound method.
    '''
    QUEUE_TYPE = "Kafka"

    def __init__(self, queue_name, queues_labels):
        super().__init__(queue_name, queues_labels)
        self.topic = self.queues_labels[queue_name]["queue"]
        self.consumer = AIOKafkaConsumer(
            self.topic,
            loop=self.loop, bootstrap_servers=KAFKA_HOST,
            key_deserializer=lambda v: v.decode(ENCODING),
            value_deserializer=lambda v: json.loads(v.decode(ENCODING)),
            #group_id="my-group"
        )

    def _decorator(self, string):
        def wrapper(func):
            async def wrapped(*args):
                """
                Message processing loop decorator to be added to `start` task.
                """
                try:
                    await self.consumer.start()
                except AssertionError as e:
                    print(e, '\ncontinuing...\n')

                while True:
                    async for msg in self.consumer:
                        log.info('Got a message')
                        valid_raw_msg = (msg.key == self.queues_labels[self.queue_name]['uuid'])
                        log.debug(f"Agent uuid matches Kafka msg key received?: {valid_raw_msg}")
                        if valid_raw_msg:
                            msg = msg.value
                            msg = json.dumps(msg) \
                                if not isinstance(msg, (str, bytes, bytearray)) \
                                else msg

                            # Decorated function comes in here
                            await func(*args, msg)

            return wrapped
        return wrapper


class AsyncIOConsumerLoop(ConsumerABC):
    '''
    Uses an internal AsyncIO queue to pass messages around.
    '''
    QUEUE_TYPE = "AsyncIO"

    def __init__(self, queue_name, queues_labels):
        super().__init__(queue_name, queues_labels)
        self.consumer = self.queue_ref = AsyncioQueue()

    def _decorator(self, string):
        def wrapper(func):
            async def wrapped(*args):
                """
                Message processing loop decorator to be added to `start` task.
                """
                while True:
                    async for msg in self.consumer:
                        log.info('Got a message')

                        # Decorated function comes in here
                        await func(*args, msg)
            return wrapped
        return wrapper


class RMQIOConsumerLoop(ConsumerABC):
    '''
    Uses the `RMQIOConsumerLoop` decorator class
    to consumer messages from a RabbitMQ queue.
    '''
    QUEUE_TYPE = "RabbitMQ"

    def __init__(self, queue_name, queues_labels, **kwargs):
        super().__init__(queue_name, queues_labels, **kwargs)
        self.routing_key = self.queues_labels[queue_name]["queue"]

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

    def add_agent_uuid(self, **kwargs_from_agent):
        self.key = kwargs_from_agent[self.QUEUE_TYPE]
        self.routing_key += f"-{self.key[:5]}"

    def _decorator(self, string):
        def wrapper(func):
            async def wrapped(*args):
                """
                Message processing loop decorator to be added to `start` task.
                """
                async with self.connection, \
                        self.queue.iterator() as consumer:
                    # Cancel consuming after __aexit__
                    async for msg in consumer:
                        async with msg.process():
                            msg = msg.body

                            # Decorated function comes in here
                            await func(*args, msg)

            return wrapped
        return wrapper


class HTTPConsumerLoop(ConsumerABC):
    '''
    Uses the `RMQIOConsumerLoop` decorator class
    to consumer messages from a RabbitMQ queue.
    '''
    QUEUE_TYPE = "HTTP"

    QUEUE_HTTP_ROUTES = ClassRouteTableDef()
    QUEUE_HTTP_PORT = get_free_port()


    def __init__(self, queue_name="", queues_labels={}, in_class=True, **kwargs):
        super().__init__(queue_name, queues_labels, **kwargs)

        if not in_class:
            self.QUEUE_HTTP_ROUTES = web.RouteTableDef()
        self.routes = self.QUEUE_HTTP_ROUTES

        self.default_method = "post"

        self.port = kwargs.get("port", self.QUEUE_HTTP_PORT)
        self.queue_ref = self.port
        log.info(f"{self} routing on port '{self.port}'")


    async def a_init(self):
        self.a_init_ran = True

    def add_agent_uuid(self, **kwargs_from_agent):
        pass

    def _decorator(self, string, method="post"):
        routes_methods = {
            "get": self.routes.get,
            "post": self.routes.post,
        }
        route_send = routes_methods.get(method.lower(), routes_methods[self.default_method])
        return route_send(string)

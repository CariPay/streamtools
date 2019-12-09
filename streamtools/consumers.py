import asyncio
from functools import wraps
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

from .libs import log, clean_queue_label, get_free_port, check_queue_type, AsyncioQueue, ClassRouteTableDef
from .libs import ENCODING, RMQ_USER, RMQ_PASS, RMQ_HOST, KAFKA_HOST


class ConsumerABC(ABC):
    '''
    An abstract base class for defining consumer decorator loops
    for consuming input messages sent to the agent and messages
    produced internally.
    '''
    QUEUE_TYPE = [""]

    def __init__(self, queue_name, queues_labels={}, **kwargs):
        check_queue_type(self)
        self.queue_name = queue_name
        self.queues_labels = queues_labels
        self.queue_label = (self.queues_labels
                                .get(queue_name, {})
                                .get("queue", queue_name))

        self.loop = asyncio.get_event_loop()
        self.consumer = None
        self.a_init_ran = False
        self.a_init_ran_msg = f"Consumer object `a_init` method didn't run for {self}"

    async def a_init(self):
        self.a_init_ran = True

    def add_agent_uuid(self, **kwargs_from_agent):
        pass

    def __call__(self, queue_arg="", override=False, *args, **kwargs):
        # Checks:
        # 1. if override is True and `queue_arg` is not empty, then
        #    `queue_arg` overrides `self.queue_label`
        # 2. if override is False but (and) `self.queue_label` is empty then
        #    `queue_arg` overrides `self.queue_label`
        check_1: bool = override and queue_arg
        check_2: bool = not override and not self.queue_label
        queue_checks: bool = check_1 or check_2

        set_queue_label = queue_arg if queue_checks else (self.queue_label or "")

        assert set_queue_label, "Internal queue not set by HandleMsgs class or included as a decorator arg."
        set_queue_label = clean_queue_label(set_queue_label)

        log.info(f"{self} routing on '{set_queue_label}'")
        if (queue_arg and queue_arg != set_queue_label) and isinstance(self, HTTPConsumerLoop):
            print(f"Note: decorator queue '{queue_arg}' arg passed is not the same as class queue '{set_queue_label}' being used.", \
                        "\n (use `override=True` arg on queue decorator to change this)")

        return self._decorator(set_queue_label, *args, **kwargs)

    @abstractmethod
    def _decorator(self, set_queue_label, *args, **kwargs):
        def wrapper(func):
            @wraps(func)
            async def wrapped(*w_args, **w_kwargs):
                '''
                Message processing loop decorator to be added to
                agent's `start` task.
                '''
                while True:
                    async for msg in self.consumer:
                        # Decorated function comes in here
                        await func(msg, *w_args, **w_kwargs)
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
    QUEUE_TYPE = ["Kafka"]

    def __init__(self, queue_name, queues_labels={}, **kwargs):
        super().__init__(queue_name, queues_labels, **kwargs)
        self.topic = self.queue_label
        self.consumer = AIOKafkaConsumer(
            self.topic,
            loop=self.loop, bootstrap_servers=KAFKA_HOST,
            key_deserializer=lambda v: v.decode(ENCODING),
            value_deserializer=lambda v: json.loads(v.decode(ENCODING)),
            #group_id="my-group"
        )

    def _decorator(self, set_queue_label, *args, **kwargs):
        def wrapper(func):
            @wraps(func)
            async def wrapped(*w_args, **w_kwargs):
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
                            await func(msg, *w_args, **w_kwargs)

            return wrapped
        return wrapper


class AsyncIOConsumerLoop(ConsumerABC):
    '''
    Uses an internal AsyncIO queue to pass messages around.
    '''
    QUEUE_TYPE = ["AsyncIO"]

    def __init__(self, queue_name, queues_labels={}, **kwargs):
        super().__init__(queue_name, queues_labels, **kwargs)
        self.consumer = self.queue_ref = AsyncioQueue()

    def _decorator(self, set_queue_label, *args, **kwargs):
        def wrapper(func):
            @wraps(func)
            async def wrapped(*w_args, **w_kwargs):
                """
                Message processing loop decorator to be added to `start` task.
                """
                while True:
                    async for msg in self.consumer:
                        log.info('Got a message')

                        # Decorated function comes in here
                        await func(msg, *w_args, **w_kwargs)
            return wrapped
        return wrapper


class RMQIOConsumerLoop(ConsumerABC):
    '''
    Uses the `RMQIOConsumerLoop` decorator class
    to consumer messages from a RabbitMQ queue.
    '''
    QUEUE_TYPE = ["RabbitMQ"]

    def __init__(self, queue_name, queues_labels={}, **kwargs):
        super().__init__(queue_name, queues_labels, **kwargs)
        self.routing_key = self.queue_label
        self.host = RMQ_HOST

    async def a_init(self):
        log.info(f"Consumer connecting to ip <{self.host}> with topic <{self.routing_key}>")
        tries, MAX_TRIES = 0, 10
        SLEEP = 3
        while tries < MAX_TRIES:
            tries += 1
            try:
                self.connection = self.queue_ref = await aio_pika.connect_robust(
                    f"amqp://{RMQ_USER}:{RMQ_PASS}@{self.host}/", loop=self.loop
                )
                tries = MAX_TRIES
            except ConnectionError as e:
                log.error(e)
                time.sleep(SLEEP)
                print(f"Retrying connect... ({tries} of {MAX_TRIES})")
            except Exception as e:
                log.error(e)
                traceback.print_exc()
                tries = MAX_TRIES

        # Creating channel
        self.channel = await self.connection.channel()    # type: aio_pika.Channel

        # Declaring queue
        self.queue = await self.channel.declare_queue(
            self.routing_key,
            auto_delete=False
        )   # type: aio_pika.Queue
        self.a_init_ran = True

    def add_agent_uuid(self, **kwargs_from_agent):
        self.key = kwargs_from_agent[self.QUEUE_TYPE]
        self.routing_key += f"-{self.key[:5]}"

    def _decorator(self, set_queue_label, *args, **kwargs):
        def wrapper(func):
            @wraps(func)
            async def wrapped(*w_args, **w_kwargs):
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
                            await func(msg, *w_args, **w_kwargs)

            return wrapped
        return wrapper


class HTTPConsumerLoop(ConsumerABC):
    '''
    Uses the `RMQIOConsumerLoop` decorator class
    to consumer messages from a RabbitMQ queue.
    '''
    QUEUE_TYPE = ["HTTP"]

    CLASS_ROUTES = ClassRouteTableDef()
    ROUTES = web.RouteTableDef()

    PORT = get_free_port()


    def __init__(self, queue_name="", queues_labels={}, in_class=True, **kwargs):
        super().__init__(queue_name, queues_labels, **kwargs)

        self.routes = HTTPConsumerLoop.CLASS_ROUTES if in_class \
                        else HTTPConsumerLoop.ROUTES

        self.default_method = "post"

        self.port = kwargs.get("port", self.PORT)
        self.queue_ref = self.port
        log.info(f"{self} routing on port '{self.port}'")


    async def a_init(self):
        self.a_init_ran = True

    def add_agent_uuid(self, **kwargs_from_agent):
        pass

    def _decorator(self, set_queue_label, method="post"):
        routes_methods = {
            "get": self.routes.get,
            "post": self.routes.post,
        }
        route_send = routes_methods.get(method.lower(), routes_methods[self.default_method])
        return route_send(set_queue_label)

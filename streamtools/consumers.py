import asyncio
from functools import wraps
import inspect
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
import boto3

from .libs import log, log_error, TRACEBACK, clean_queue_label, get_free_port, \
                  check_queue_type, AsyncioQueue, ClassRouteTableDef
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
        self.queue_label = clean_queue_label(self.queue_label)

        self.loop = asyncio.get_event_loop()
        self.consumer = None
        self.in_class = None
        self.self_obj = None
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
        set_queue_label = clean_queue_label(set_queue_label)
        # TODO Review how rabbitmq handles empty queue names (and any other services)
        # if not isinstance(self, HTTPConsumerLoop):
        #     assert set_queue_label, \
        #             "Internal queue not set by HandleMsgs class or included as a decorator arg."

        log.info(f"{self} routing on '{set_queue_label}'")
        if (queue_arg and queue_arg not in [set_queue_label, f"/{set_queue_label}"]) \
            and isinstance(self, HTTPConsumerLoop):
            # Setup log msgs
            log_msg1 = f"Note: decorator queue '{queue_arg}' arg passed is not the same as" + \
                       f" 'HTTP Handler' class queue '{set_queue_label}' being used."
            log_msg2 = f"(use `override=True` arg on queue decorator to change this)"

            # Send log messages
            [log.info(msg) for msg in (log_msg1, log_msg2)]

        return self._decorator(set_queue_label, *args, **kwargs)

    def parse_decorated_args(self, func, args_list, kwargs_list):
        args_error_msg = "Please use only one argument (and optional 'self' arg in classes)" + \
                         " for the message to be passed."

        if kwargs_list:
            kw_error_msg = "No keyword arguments allowed. "
            raise ValueError(args_error_msg + kw_error_msg)
        if len(args_list) > 1:
            raise ValueError(args_error_msg)

        self_obj = None
        if args_list:
            self_obj, *_ = args_list

            self_members = dict(inspect.getmembers(self_obj.__class__))
            func_in_class = self_members.get(func.__name__)
            assert func_in_class and \
                   inspect.isroutine(func) and \
                   (func.__qualname__ == func_in_class.__qualname__), \
                "First argument must be either 'self/cls' inside a class object. " + \
                "Only one argument allowed otherwise for unbound functions."

        # Save 'self_obj' to consumer class instance on first run
        if self.in_class is None:
            self.self_obj = self_obj
            self.in_class = True if args_list else False

        return self_obj

    async def call_decorated(self, func, msg, w_args, w_kwargs):
        self_obj = self.parse_decorated_args(func, w_args, w_kwargs)
        if self.in_class:
            try:
                assert self_obj == self.self_obj, \
                    "'self/cls' argument not being consistently passed into class method"
                await func(self_obj, msg)
            except AssertionError as e:
                error_msg = log_error(e, with_traceback=TRACEBACK)
                await func(self.self_obj, msg)
        else:
            await func(msg)

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
                        # Decorated function 'func' comes in here
                        await self.call_decorated(func, msg, w_args, w_kwargs)

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
                    log.error(e)
                    log.info('continuing...')

                while True:
                    async for msg in self.consumer:
                        valid_raw_msg = (msg.key == self.queues_labels[self.queue_name]['uuid'])
                        log.debug(f"Agent uuid matches Kafka msg key received?: {valid_raw_msg}")
                        if valid_raw_msg:
                            msg = msg.value
                            msg = json.dumps(msg) \
                                if not isinstance(msg, (str, bytes, bytearray)) \
                                else msg

                            # Decorated function 'func' comes in here
                            await self.call_decorated(func, msg, w_args, w_kwargs)

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
                        # Decorated function 'func' comes in here
                        await self.call_decorated(func, msg, w_args, w_kwargs)

            return wrapped
        return wrapper

class SQSConsumerLoop(ConsumerABC):
    '''
    Uses AWS SQS to pass messages around.
    '''
    QUEUE_TYPE = ["SQS"]
    LOOP = asyncio.get_event_loop()

    sqs = boto3.resource('sqs')

    def __init__(self, queue_name, queues_labels={}, **kwargs):
        super().__init__(queue_name, queues_labels, **kwargs)
        self.queue = self.sqs.get_queue_by_name(QueueName=self.queue_label)

    def _decorator(self, set_queue_label, *args, **kwargs):
        def wrapper(func):
            @wraps(func)
            def wrapped(*w_args, **w_kwargs):
                """
                Message processing loop decorator to be added to `start` task.
                """
                while True:
                    messages = self.queue.receive_messages(WaitTimeSeconds=5)
                    for msg in messages:
                        # Decorated function 'func' comes in here
                        log.info(f"SQS message received: {msg}")
                        self.LOOP.run_until_complete(self.call_decorated(func, msg.body, w_args, w_kwargs))
                        msg.delete()

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
                rmq_url = f"amqp://{RMQ_USER}:{RMQ_PASS}@{self.host}/"
                log.info(f"Connecting to RabbitMQ host at: {rmq_url}")
                self.connection = self.queue_ref = \
                    await aio_pika.connect_robust(rmq_url, loop=self.loop)
                log.info(f"Successfully connected! {self} {rmq_url}")
                tries = MAX_TRIES
            except ConnectionError as e:
                log.error(e)
                time.sleep(SLEEP)
                log.info(f"Retrying connect... ({tries} of {MAX_TRIES})")
            except Exception as e:
                log.error(e)
                traceback.print_exc()
                tries = MAX_TRIES

        self.a_init_ran = True

    def add_agent_uuid(self, **kwargs_from_agent):
        self.key = kwargs_from_agent[self.QUEUE_TYPE[0]]
        self.routing_key += f"-{self.key[:5]}"

    async def _create_channel_and_queue(self):
        # Creating channel
        self.channel = await self.connection.channel()    # type: aio_pika.Channel

        # Declaring queue
        self.queue = await self.channel.declare_queue(
            self.routing_key,
            auto_delete=False
        )   # type: aio_pika.Queue

    def _decorator(self, set_queue_label, *args, **kwargs):
        def wrapper(func):
            @wraps(func)
            async def wrapped(*w_args, **w_kwargs):
                """
                Message processing loop decorator to be added to `start` task.
                """
                async with self.connection:
                    await self._create_channel_and_queue()
                    async with self.queue.iterator() as consumer:
                        # Cancel consuming after __aexit__
                        async for msg in consumer:
                            async with msg.process():
                                msg = msg.body

                                # Decorated function 'func' comes in here
                                await self.call_decorated(func, msg, w_args, w_kwargs)

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
        send = routes_methods.get(method.lower(), routes_methods[self.default_method])
        return send(f"/{set_queue_label}")

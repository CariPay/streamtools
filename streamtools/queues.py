import asyncio
from aiohttp import web

from .consumers import \
    KafkaConsumerLoop, \
    AsyncIOConsumerLoop, \
    RMQIOConsumerLoop, \
    HTTPConsumerLoop

from .producers import \
    KafkaProducer, \
    HTTPProducer, \
    AsyncIOProducer, \
    RMQIOProducer

from .libs import prepare_aiohttp_app, check_queue_type

CONSUMER_LOOPS = {
    "KafkaHandler": KafkaConsumerLoop,
    "AsyncIOHandler": AsyncIOConsumerLoop,
    "RMQIOHandler": RMQIOConsumerLoop,
    "HTTPHandler": HTTPConsumerLoop
}

PRODUCERS = {
    "KafkaHandler": KafkaProducer,
    "POSTHandler": HTTPProducer,
    "AsyncIOHandler": AsyncIOProducer,
    "RMQIOHandler": RMQIOProducer,
}


class HandleMsgs:
    '''
    An object for setting the message producers and consumers
    for the repo based on strings passed.
    '''

    def __init__(self, queue_name: str, msg_queues: dict, queues_labels: dict, consumer_loops_dict: dict, producers_dict: dict):
        self.msg_queues = msg_queues
        self.queues_labels = queues_labels
        assert queue_name in self.msg_queues, f"'queue_name' arg must be one of {tuple(self.msg_queues.keys())}"
        self.queue_name = queue_name
        self.consumer_loops_dict = consumer_loops_dict
        self.producers_dict = producers_dict
        self.consumer_loop_label = self.msg_queues[queue_name]["consumer_loop"]
        self.producer_label = self.msg_queues[queue_name]["producer"]

        config_check, CONFIG_OPTIONS = self.check_args(self.consumer_loop_label, self.producer_label)
        assert config_check, \
            f"Invalid config passed. Please make sure args are one of: {CONFIG_OPTIONS}"

        cons_type, prod_type = self.types
        type_check = set(cons_type) & set(prod_type)   # compare both collections for common elements
        assert type_check, \
            f"Incompatible queue types passed: {self.types}"

        self.consumer_loop = self.set_consumer_loop()
        self.producer = self.set_producer()
        self.shared_queue = self._update_shared_queue()

    async def a_init(self):
        await self.consumer_loop.a_init()
        self.shared_queue = self._update_shared_queue()
        await self.producer.a_init()

    def _update_shared_queue(self):
        if hasattr(self.consumer_loop, "queue_ref"):
            shared_queue = self.consumer_loop.queue_ref
            self.producer.queue_from_consumer = shared_queue
            return shared_queue
        return None

    def add_agent_uuid(self, **kwargs_from_agent):
        for elem in (self.consumer_loop, self.producer):
            if hasattr(elem, "add_agent_uuid"):
                elem.add_agent_uuid(**kwargs_from_agent)

    def check_args(self, *args):
        CONFIG_OPTIONS = set([*self.producers_dict.keys(), *self.consumer_loops_dict.keys()])
        config_check = all([arg in CONFIG_OPTIONS for arg in args])
        return config_check, CONFIG_OPTIONS

    @property
    def types(self):
        consumer_loop_class = self.consumer_loops_dict[self.consumer_loop_label]
        check_queue_type(consumer_loop_class)
        producer_class = self.producers_dict[self.producer_label]
        check_queue_type(producer_class)
        return consumer_loop_class.QUEUE_TYPE, producer_class.QUEUE_TYPE

    def set_consumer_loop(self):
        ConsumerLoopClass = self.consumer_loops_dict[self.consumer_loop_label]
        consumer_loop = ConsumerLoopClass(self.queue_name, self.queues_labels)
        if hasattr(consumer_loop, "routes"):
            self.handler_routes = consumer_loop.routes
        return consumer_loop

    def set_producer(self):
        ProducerClass = self.producers_dict[self.producer_label]
        return ProducerClass(self.queue_name, self.queues_labels)

    @property
    def handles(self):
        return self.consumer_loop, self.producer


class HandlerFactory:

    def __init__(self, configs: dict):
        self.configs = configs
        self.queues_labels = self._prepare_queues_config()
        self.handlers = self._build()
        self.web_app = web.Application()
        self.web_app_config = self._extract_app_config()


    def _prepare_queues_config(self):
        config_dict = {}
        for label in self.configs:
            config_dict[label] = {
                "queue": self.configs[label]["name"],
                "uuid": ""
            }

        return config_dict

    def _build(self):
        handler_kwargs = {
            "msg_queues": self.configs,
            "consumer_loops_dict": CONSUMER_LOOPS,
            "producers_dict": PRODUCERS
        }
        for queue in self.queues_labels:
            self.queues_labels[queue]["handler"] = HandleMsgs(
                                                    queue_name=queue,
                                                    queues_labels=self.queues_labels,
                                                    **handler_kwargs
                                                   )

        return {queue: self.queues_labels[queue]["handler"] for queue in self.queues_labels}


    def _extract_app_config(self):
        config = None, None, None
        for handler in self.handlers.values():
            if hasattr(handler, "handler_routes"):
                app = self.web_app
                routes = handler.handler_routes
                port = handler.shared_queue

                config = (app, routes, port)
                break

        return config

class ConsumerRunner:

    WEBAPP = web.Application()

    def __init__(self, consumers=None, handlers=None, port=9000, host=None):
        self.consumers = consumers
        self.handlers = handlers
        self.port = port
        self.host = host
        self.webapp = ConsumerRunner.WEBAPP


    async def _extract_routes_from_consumers(self, consumers):
        routes = None
        for consumer in consumers:
            await consumer.a_init()
            if not routes:
                routes = getattr(consumer, "routes", None)
        return routes

    async def app(self):

        routes = await self._extract_routes_from_consumers(self.consumers)
        site = await prepare_aiohttp_app(app=self.webapp, routes=routes, port=self.port, host=self.host)

        if not (self.consumers and self.handlers):
            print("No consumers passed to run function. Exiting...")
            asyncio.get_running_loop().stop()
        elif site:
            print(f"===== Starting HANDLER on: http://localhost:{self.port} =====")
            await site.start()
        else:
            queue_type = f"{self.consumers[0].QUEUE_TYPE} Handler"
            print(f"===== Starting HANDLER on: {queue_type} =====")
            await asyncio.gather(*[handler() for handler in self.handlers])


    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.app())
        loop.run_forever()

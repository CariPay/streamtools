import aiohttp
import asyncio
import json
import logging
import os
import time
import traceback
from abc import ABC, abstractmethod

from kafka import KafkaProducer as ProducerFromKafka
import aio_pika

from .libs import log, clean_queue_label, check_queue_type
from .libs import HTTP_HOST, RMQ_USER, RMQ_PASS, RMQ_HOST, KAFKA_HOST, ENCODING


class ProducerABC(ABC):
    '''
    Abstract base class to be used for all other producers
    classes to interact with Kafka.
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

        self._queue_from_consumer = None
        self.key = ""

    async def a_init(self):
        pass

    def add_agent_uuid(self, **kwargs_from_agent):
        pass

    @property
    def queue_from_consumer(self):
        return self._queue_from_consumer

    @queue_from_consumer.setter
    def queue_from_consumer(self, queue_from_consumer):
        self.producer = \
        self._queue_from_consumer = queue_from_consumer
        log.info(f"Producer was set from consumer's queue object!")
        log.info(f"Producer: {self}")

    @abstractmethod
    async def send(self, msg, *args, **kwargs):
        pass



class KafkaProducer(ProducerABC):
    '''
    Uses the `KafkaProducer` class from the
    aiokafka library to publish messages
    directly to a Kafka broker.
    '''
    QUEUE_TYPE = ["Kafka"]

    def __init__(self, queue_name, queues_labels={}, **kwargs):
        super().__init__(queue_name, queues_labels, **kwargs)
        self.topic = self.queue_label

        start = time.time()
        log.info(f"Starting kafka producer for '{self.topic}' topic")
        self._producer = ProducerFromKafka(
            acks=1,
            batch_size=1,  # @TODO: config-able
            bootstrap_servers=KAFKA_HOST,
            api_version=(1, 0, 0),
            key_serializer=lambda v: str(v).encode(ENCODING),
            value_serializer=lambda v: json.dumps(v).encode(ENCODING)
        )
        end = time.time()
        log.info(f"Got '{self.topic}' kafka producer in {round(end - start, 2)}s!")

    def add_agent_uuid(self, **kwargs_from_agent):
        self.key = kwargs_from_agent[self.QUEUE_TYPE[0]]

    def send(self, message, *args, **kwargs):
        try:
            # @TODO: pack message

            log.info('sending on topic {}'.format(self.topic))
            log.info('sending message {}'.format(self.key))

            future = self._producer.send(
                self.topic,
                key=self.key,
                value=message
            )

            future.get(timeout=5)
            log.info('sent message {}'.format(self.key))

            return True

        except Exception as e:
            log.error('Failed to send: ')
            log.error(e)
            traceback.print_exc()

            return None


class HTTPProducer(ProducerABC):
    '''
    "Uses an external Kafka producer service to
    have messages sent to the Kafka broker to be
    added to the queues. Messages are sent out as
    POST requests to the service's endpoint with
    two required fields:\n
    \"msg_topic\": for the relevant kafka topic queue\n
    \"msg_key\": to link back messages to the agent
    '''
    QUEUE_TYPE = ["HTTP", "Kafka", "RabbitMQ"]

    HEADERS = {
        "JSON": {'content-type': 'application/json'},
        "SSI": {'content-type': 'application/ssi-agent-wire'}
    }

    def __init__(self, queue_name="", queues_labels={}, **kwargs):
        super().__init__(queue_name, queues_labels, **kwargs)
        self.topic = self.queue_label
        self.route = self.queue_label
        self.host = HTTP_HOST

    async def a_init(self):
        self.ip = "localhost"
        self.port = self.queue_from_consumer
        self.host = f"{self.ip}:{self.port}"

    def add_agent_uuid(self, **kwargs_from_agent):
        self.key = kwargs_from_agent[self.QUEUE_TYPE[0]]

    def _add_class_attrs_to_msg(self, msg):
        if isinstance(msg, (str, bytes, bytearray)):
            msg = json.loads(msg)
        msg["msg_key"] = self.key
        msg["msg_topic"] = self.topic
        return json.dumps(msg)

    async def send_message_via_aiohttp(self, url, msg, headers, method="post"):
        async with aiohttp.ClientSession() as session:
            send_request = session.post if method.lower()=="post" else session.get
            async with send_request(url, data=msg, headers=headers) as response:
                resp = response
                if resp.status != 202:
                    log.info(f"Response: ({resp.status}) {await resp.text()}")
        return response

    async def send(self, msg, method="post", endpoint=None, headers_tag="JSON", *args, **kwargs):
        endpoint =  endpoint or f"http://{self.host}/{self.route}"
        headers = self.HEADERS.get(headers_tag, self.HEADERS["JSON"])

        msg = self._add_class_attrs_to_msg(msg)
        return await self.send_message_via_aiohttp(url=endpoint, msg=msg, headers=headers, method=method)


class AsyncIOProducer(ProducerABC):
    '''
    Uses an internal AsyncIO queue to pass messages around.
    '''
    QUEUE_TYPE = ["AsyncIO"]

    def __init__(self, queue_name, queues_labels={}, **kwargs):
        super().__init__(queue_name, queues_labels, **kwargs)
        self.producer = None

    async def send(self, msg, *args, **kwargs):
        assert self.producer != None, \
            "Producer queue object is not set"
        await self.producer.put(msg)


class RMQIOProducer(ProducerABC):
    '''
    Uses an external RabbitMQ broker queue to pass
    messages around.
    '''
    QUEUE_TYPE = ["RabbitMQ"]

    def __init__(self, queue_name, queues_labels={}, **kwargs):
        super().__init__(queue_name, queues_labels, **kwargs)
        self.loop = asyncio.get_event_loop()
        self.routing_key = self.queue_label
        self.host = RMQ_HOST

    async def a_init(self):
        rmq_url = f"amqp://{RMQ_USER}:{RMQ_PASS}@{self.host}/"
        log.info(f"Producer connecting to ip <{self.host}> with topic <{self.routing_key}>")
        self.connection = await aio_pika.connect_robust(rmq_url,loop=self.loop) \
                                    if not self.queue_from_consumer else self.queue_from_consumer
        self.channel = await self.connection.channel()

    def add_agent_uuid(self, **kwargs_from_agent):
        self.key = kwargs_from_agent[self.QUEUE_TYPE[0]]
        self.routing_key += f"-{self.key[:5]}"

    async def send(self, msg, *args, **kwargs):
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=msg.encode()
            ),
            routing_key=self.routing_key
        )

import aiohttp
import asyncio
import json
import os
import requests
import time
import traceback
from abc import ABC, abstractmethod

from kafka import KafkaProducer as ProducerFromKafka
import aio_pika

from logger import log
from message import Message

POST_HOST = os.environ['POST_HOST']
HOST = os.environ['KAFKA_HOST']
ENCODING = os.environ['ENCODING']

HEADERS = {
    "JSON": {'content-type': 'application/json'},
    "SSI": {'content-type': 'application/ssi-agent-wire'}
}
async def send_message_via_post(url, msg, headers):
    async with aiohttp.ClientSession() as session, \
               session.post(url, data=msg, headers=headers) as resp:
        if resp.status != 202:
            print(resp.status)
            print(await resp.text())


class ProducerABC(ABC):
    QUEUE_TYPE = ""
    '''
    Abstract base class to be used for all other producers
    classes to interact with Kafka.
    '''
    def __init__(self, **kwargs):
        assert hasattr(self, 'QUEUE_TYPE'), "'QUEUE_TYPE' not defined in class."
        self.kwargs = kwargs
        self._queue_from_consumer = None

    async def a_init(self):
        pass

    def update_attr(self, **kwargs_from_agent):
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
    def send(self, msg: Message):
        pass



class KafkaProducer(ProducerABC):
    QUEUE_TYPE = "Kafka"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.topic = self.kwargs["topic"]

        start = time.time()
        log.info(f"Starting kafka producer for '{self.topic}' topic")
        self._producer = ProducerFromKafka(
            acks=1,
            batch_size=1,  # @TODO: config-able
            bootstrap_servers=HOST,
            api_version=(1, 0, 0),
            key_serializer=lambda v: str(v).encode(ENCODING),
            value_serializer=lambda v: json.dumps(v).encode(ENCODING)
        )
        end = time.time()
        log.info(f"Got '{self.topic}' kafka producer in {round(end - start, 2)}s!")

    def update_attr(self, **kwargs_from_agent):
        self.key = kwargs_from_agent[self.QUEUE_TYPE]

    def send(self, message):
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

class POSTProducer(ProducerABC):
    QUEUE_TYPE = "Kafka"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.topic = self.kwargs["topic"]

    def update_attr(self, **kwargs_from_agent):
        self.key = kwargs_from_agent[self.QUEUE_TYPE]

    async def send(self, msg: Message):
        endpoint = f"http://{POST_HOST}/send"
        if isinstance(msg, str):
            msg = json.loads(msg)
        msg["msg_key"] = self.key
        msg["msg_topic"] = self.topic
        msg = json.dumps(msg)

        await send_message_via_post(endpoint, msg, HEADERS["JSON"])

class AsyncIOProducer(ProducerABC):
    QUEUE_TYPE = "AsyncIO"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.producer = None

    async def send(self, msg: Message):
        assert self.producer != None, \
            "Producer queue object is not set"
        await self.producer.put(msg)

class RMQIOProducer(ProducerABC):
    QUEUE_TYPE = "RabbitMQ"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.loop = asyncio.get_event_loop()
        self.routing_key = self.kwargs["topic"]

    async def a_init(self):
        self.connection = self.queue_from_consumer
        self.channel = await self.connection.channel()

    def update_attr(self, **kwargs_from_agent):
        self.key = kwargs_from_agent[self.QUEUE_TYPE]
        self.routing_key += f"-{self.key[:5]}"

    async def send(self, msg: Message):
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=msg.encode()
            ),
            routing_key=self.routing_key
        )

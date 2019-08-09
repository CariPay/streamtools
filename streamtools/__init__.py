from consumers import \
    KafkaConsumerLoop, \
    AsyncIOConsumerLoop, \
    RMQIOConsumerLoop

from producers import \
    KafkaProducer, \
    POSTProducer, \
    AsyncIOProducer, \
    RMQIOProducer


CONSUMER_LOOPS = {
    "KafkaHandler": {
        "type": "Kafka",
        "object": KafkaConsumerLoop,
        "description": "Uses the `KafkaConsumerLoop` decorator class " \
                       "from the aiokafka library to consume messages " \
                       "from a Kafka broker."
    },
    "AsyncIOHandler": {
        "type": "AsyncIO",
        "object": AsyncIOConsumerLoop,
        "description": "Uses an internal AsyncIO queue to pass messages \
                        around."
    },
    "RMQIOHandler": {
        "type": "RabbitMQ",
        "object": RMQIOConsumerLoop,
        "description": "Uses the `RMQIOConsumerLoop` decorator class \
                        to consumer messages from a RabbitMQ queue."
    }
}

PRODUCERS = {
    "KafkaHandler": {
        "type": "Kafka",
        "object": KafkaProducer,
        "description": "Uses the `KafkaProducer` class from " \
                       "the aiokafka library to publish messages " \
                       "directly to a Kafka broker."
    },
    "POSTHandler": {
        "type": "Kafka",
        "object": POSTProducer,
        "description": "Uses an external Kafka producer service to " \
                       "have messages sent to the Kafka broker to be " \
                       "added to the queues. Messages are sent out as " \
                       "POST requests to the service's endpoint with " \
                       "two required fields:\n" \
                       "\"msg_topic\": for the relevant kafka topic queue\n" \
                       "\"msg_key\": to link back messages to the agent"
    },
    "AsyncIOHandler": {
        "type": "AsyncIO",
        "object": AsyncIOProducer,
        "description": "Uses an internal AsyncIO queue to pass messages \
                        around."
    },
    "RMQIOHandler": {
        "type": "RabbitMQ",
        "object": RMQIOProducer,
        "description": "Uses an external RabbitMQ broker queue to pass \
                        messages around."
    }
}

class HandleMsgs:
    '''
    An object for setting the message producers and consumers
    for the repo based on strings passed.
    '''

    def __init__(self, consumer_loop: str, producer: str, **kwargs):
        self.consumer_loop_label = consumer_loop
        self.producer_label = producer
        self.kwargs = kwargs

        config_check, CONFIG_OPTIONS = self.check_args(consumer_loop, producer)
        assert config_check, \
            f"Invalid config passed. Please make sure args are one of: {CONFIG_OPTIONS}"

        type_check = len(set(self.types)) == 1
        assert type_check, \
            f"Incompatible queue types passed: {self.types}"

        self.consumer_loop = self.set_consumer_loop()
        self.producer = self.set_producer()

    async def a_init(self):
        await self.consumer_loop.a_init()
        if hasattr(self.consumer_loop, "queue_ref"):
            self.producer.queue_from_consumer = self.consumer_loop.queue_ref
        await self.producer.a_init()

    def update_attr(self, **kwargs_from_agent):
        for elem in (self.consumer_loop, self.producer):
            elem.update_attr(**kwargs_from_agent)

    def check_args(self, *args):
        CONFIG_OPTIONS = set([*PRODUCERS.keys(), *CONSUMER_LOOPS.keys()])
        config_check = all([arg in CONFIG_OPTIONS for arg in args])
        return config_check, CONFIG_OPTIONS

    def set_consumer_loop(self):
        consumer_loop_func = CONSUMER_LOOPS[self.consumer_loop_label]["object"]
        return consumer_loop_func(**self.kwargs)

    def set_producer(self):
        producer_func = PRODUCERS[self.producer_label]["object"]
        return producer_func(**self.kwargs)

    @property
    def handles(self):
        return self.consumer_loop, self.producer

    @property
    def types(self):
        consumer_loop_type = CONSUMER_LOOPS[self.consumer_loop_label]["type"]
        producer_type = PRODUCERS[self.producer_label]["type"]
        return consumer_loop_type, producer_type

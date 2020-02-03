import asyncio
from copy import deepcopy

from .queues import CONSUMER_LOOPS
from .constants import HTTP

LOOP = asyncio.get_event_loop()


class ConsumerFactory:

    def __init__(self, queue_type, port, supports_http=True, in_class=False):
        self.queue_type = queue_type
        self._validate_queue_type()

        self.ConsumerClass = CONSUMER_LOOPS[queue_type]
        self.instances = []
        self.handlers = []
        self.port = port
        self.http_handler = (self.queue_type == HTTP)
        self.supports_http = supports_http
        self.in_class = in_class

    def _validate_queue_type(self):
        valid_queue_types = list(CONSUMER_LOOPS.keys())
        valid = self.queue_type in valid_queue_types
        if not valid:
            raise ValueError(f"'queue_type' arg must be one of {valid_queue_types}")

        return valid


    def _generic_consumer(self, *args, **kwargs):
        '''
        Made to prevent an HTTP instance of a consumer
        handler from breaking when it hits the
        async_serverless decorator.
        '''
        def wrapper(func):
            def wrapped(*w_args, **w_kwargs):
                msg = {}
                return func(msg, *w_args, **w_kwargs)
            return wrapped
        return wrapper

    def route(self, *args, **kwargs):
        # Setup ConsumerClass init kwargs
        if self.http_handler:
            init_kwargs = deepcopy(kwargs)
            init_kwargs.pop('port', None)
            init_kwargs.pop('in_class', None)
            init_kwargs = {
                'port': self.port,
                'in_class': self.in_class,
                **init_kwargs,
            }
        else:
            init_kwargs = kwargs

        # Set the decorator
        decorator = self._generic_consumer
        if self.supports_http or not self.http_handler:
            instance = self.ConsumerClass(*args, **init_kwargs)
            LOOP.run_until_complete(instance.a_init())
            self.instances.append(instance)

            decorator = instance(*args, **kwargs)

        return decorator

    def register(self, func):
        '''
        A decorator to register handler
        functions within this class.
        '''
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        self.handlers.append(wrapper)
        return wrapper

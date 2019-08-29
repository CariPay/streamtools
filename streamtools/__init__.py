from .queues import HandlerFactory, ConsumerRunner, CONSUMER_LOOPS, PRODUCERS
from .libs import prepare_aiohttp_app

# Create agnostic post handler to be used to send POST requests
httpsender = PRODUCERS["POSTHandler"]()

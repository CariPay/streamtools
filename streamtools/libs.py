import asyncio
import inspect
import logging
import os
import re
import socket
from typing import Any

from aiohttp import web

log = logging.getLogger()

ENCODING = os.environ.get("ENCODING", "utf-8")

try:
    IP = socket.gethostbyname(socket.gethostname())
except Exception:
    IP = socket.gethostbyname('localhost')

RMQ_USER = os.environ.get("RMQ_USER", "guest")
RMQ_PASS = os.environ.get("RMQ_PASS", "guest")
RMQ_HOST = os.environ.get("RMQ_HOST", IP)

HTTP_HOST = os.environ.get("HTTP_HOST", f"{IP}:3000")  # includes port for HTTP
KAFKA_HOST = os.environ.get("KAFKA_HOST", f"{IP}:9092")  # includes port for Kafka

TRACEBACK = bool(os.environ.get('TRACEBACK', True))

def log_error(error, with_traceback=TRACEBACK):
    if not with_traceback:
        log.error(f"{type(error).__name__}: {error}")
    else:
        traceback_msg = f"Printing traceback for error: \"{error}\":\n{traceback.format_exc()}"
        log.error(traceback_msg)

    return str(error)

def clean_queue_label(string):
    pattern = re.compile('[^a-zA-Z0-9-_]+', re.UNICODE)
    cleaned_string = pattern.sub('', string)
    return cleaned_string


def get_free_port():
    with socket.socket() as s:
        s.bind(('',0))
        port = s.getsockname()[1]
    return port


async def prepare_aiohttp_app(app=None, routes=None, port=None, handler_group=None, classes=[], host=None):
    if handler_group:
        app, routes, port = getattr(handler_group, "web_app_config", (None, None, None))
    required = [app is not None, routes is not None, port]
    if not all(required):
        return None

    if classes and hasattr(routes, "add_class_routes"):
        routes.add_class_routes(classes)

    app.add_routes(routes)

    runner = web.AppRunner(app)
    await runner.setup()

    return web.TCPSite(runner=runner, host=host, port=port)

def check_queue_type(obj):
    assert hasattr(obj, 'QUEUE_TYPE'), "'QUEUE_TYPE' not defined in class."

    assert all( isinstance(obj.QUEUE_TYPE, (list, tuple)) and \
                isinstance(queue_type, str) for queue_type in obj.QUEUE_TYPE ), \
                "Class's 'QUEUE_TYPE' must be a collection of strings."

class AsyncioQueue(asyncio.Queue):
    def __aiter__(self): return self
    async def __anext__(self): return await self.get()


class ClassRouteTableDef(web.RouteTableDef):

    def __repr__(self) -> str:
        return "<ClassRouteTableDef count={}>".format(len(self._items))

    def route(self,
              method: str,
              path: str,
              **kwargs: Any):
        def inner(handler: Any) -> Any:
            # Is Any rather than _HandlerType because of mypy limitations, see
            # https://github.com/python/mypy/issues/3882 and
            # https://github.com/python/mypy/issues/2087
            handler.route_info = (method, path, kwargs)
            return handler
        return inner

    def add_class_routes(self, instances) -> None:
        def predicate(member: Any) -> bool:
            # alternatively use `inspect.isroutine(object)` below fow wider filter
            return all((inspect.ismethod(member),
                        hasattr(member, "route_info")))
        for instance in instances:
            for _, handler in inspect.getmembers(instance, predicate):
                assert inspect.iscoroutinefunction(handler), \
                    "Please ensure your decorated handler methods are all \"async\""
                method, path, kwargs = handler.route_info
                super().route(method, path, **kwargs)(handler)

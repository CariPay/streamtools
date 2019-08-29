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

HOST = os.environ.get("IP", socket.gethostbyname('localhost'))

RMQ_USER = os.environ.get("RMQ_USER", "guest")
RMQ_PASS = os.environ.get("RMQ_PASS", "guest")
RMQ_HOST = HOST

HTTP_HOST = os.environ.get("HTTP_HOST", f"{HOST}:3000")  # includes port for HTTP
KAFKA_HOST = os.environ.get("KAFKA_HOST", f"{HOST}:9092")  # includes port for Kafka


def clean_route_string(string):
    pattern = re.compile('[^a-zA-Z0-9-_]+', re.UNICODE)
    cleaned_string = pattern.sub('', string)
    return f"/{cleaned_string}"


def get_free_port():
    with socket.socket() as s:
        s.bind(('',0))
        port = s.getsockname()[1]
    return port


async def prepare_aiohttp_app(app=None, routes=None, port=None, handler_group=None, classes=[]):
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

    return web.TCPSite(runner=runner, port=port)


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
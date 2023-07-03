# Description

This library provides a set of tools targeted at the communication layer between microservices within an application’s architecture.

In any microservices implementation, each service instance (typically a process) must communicate with the others using some _inter-process communication protocol_ such as HTTP or AMQP. This communication may be either synchronous, for example via HTTP requests & responses, or asynchronous, for example via a message broker or some other queuing mechanism.

The library provides two high-level abstractions for interfacing with these sorts of protocols:
1. A set of **Producer** classes for addressing and sending messages via a chosen protocol
1. A set of **Consumer** classes for listening on a defined channel and handling any messages sent on that channel.

It also provides a way to write service code that is agnostic of the messaging layer that will be used. To allow for this, the library also includes a switching mechanism for choosing a specific protocol that allows seemless switching between the messaging protocols that this tool supports.

The library also functions as an "async-only" tool where all the methods exposed are async-enabled and must be run within an event loop in some way.

## Interface and Switching

The primary interface for the Producer and Consumer classes is defined in a respective base class for each of these two types. To interact with a new protocol, a child class must be made from this base class to allow communication over that protocol by implementing the interface defined in the base class.

A name constant is defined for each unique protocol and all child classes are collected in two dictionary objects:
1. `PRODUCERS`
1. `CONSUMER_LOOPS`

For services that wish to make use of this tool, they must implement dynamic selection of classes by using syntax such as `MyProducerClass = streamtools.PRODUCERS[QUEUE_NAME]`, where the `QUEUE_NAME` variable can be a hardcoded string or environment variable for the messaging protocol to be used.

# Usage

Services may choose to use either Producers, Consumers. or some combination of the two to interact with the desired messaging protocol. Producers and Consumers support different send flags and options for each messaging protocol implementation.

## General Usage
[Configure for RabbitMQ](#rabbitmq) | [Configure for HTTP](#http)

At a high level, the following is how someone could successfully implement a Producer and a Consumer.

**Producers:**

1. Fetch the Producers collection: `from streamtools import PRODUCERS`

1. Choose a messaging type from the supported types: `MyProducer = PRODUCERS[<type-here>]`

1. Instantiate the producer with an endpoint or queue name to produce to: `producer = MyProducer(my_queue_name)`

1. Initialise the producer: `await producer.a_init()`

1. Send a message: `await producer.send(msg)`

**Consumers:**

1. Fetch the Consumers collection: `from streamtools import CONSUMER_LOOPS`

1. Choose a messaging type from the supported types: `MyConsumer = CONSUMERS[<type-here>]`

1. Instantiate the producer with an endpoint or queue name to produce to: `consumer = MyConsumer(my_queue_name)`

1. Initialise the consumer: `await consumer.a_init()`

1. Decorate the handler functions with the consumer:
    ```
    @consumer()
    async def handler_func(msg):
      pass
    ```

---

### RabbitMQ - Deprecated
Asynchronous messaging via the use of a RabbitMQ message broker and async queues.

Both the Producer and Consumer additionally require 3 environment variables to be present:
- `RMQ_HOST` - the ip address for the RabbitMQ broker (default is the local IP for the service)
- `RMQ_USER` - the RabbitMQ username for connecting (default: `guest`)
- `RMQ_PASS` - the RabbitMQ user password for connecting (default: `guest`)

---

### HTTP
Synchronous messaging using HTTP requests and responses between services.

#### Producer
The HTTP Producer has 3 additional parameters on its send method that may be used to customise HTTP requests made:
- `method` - allows for tagging either "GET" or "POST" requests (default: `"POST"`)
- `endpoint` - allows for defining the ip and endpoint that requests should be sent to (default is the host ip and queue's internal queue name as the endpoint)
- `headers_tag` - allows for setting the header content-type (default: `"JSON"`)

Sample send method:
`await producer.send(msg, method="GET", endpoint="http://127.0.0.1/test", header_tag="SSI")`

#### Consumer
The instantiated Consumer decorator has one additional parameter that can be used to define the types of HTTP methods to listen for:
- `method` - allows for tagging either "GET" or "POST" requests (default: `"POST"`)

Sample Consumer decorator:
```
@consumer(method="GET")
async def handler_func(msg):
  pass
```

---

## Helpers
[Handler Factory](#handler-factory) | [Consumer Factory](#consumer-factory) | [Consumer Runner](#consumer-runner)

To support the usage of Producers and Consumers in more complex types of the situations, additional helper tools are available to help simplify the instantiation, management and syntax of working with Producers and Consumers.

### Handler Factory
In codebases where Consumer and Producers are run alongside each other within the same process, this factory can be used to create Producer/Consumer pairs that are guaranteed to operate on the same queue attributes. It also allows for the easy configuration of different types of queue types within the same codebase.

The `HandlerFactory` class makes use of the `HandleMsgs` class which exposes a Consumer/Producer pair for a `queue_name` and `queue_type` passed in to it.

The primary interfaces to this class is via a "messages queues" object that takes the shape of:
```
{
    "queue_1": {
        "name": "queue_1",
        "consumer_loop": "KafkaHandler",
        "producer": "HTTPHandler"
    },
    "queue_2": {
        "name": "queue_2",
        "consumer_loop": "RMQIOHandler",
        "producer": "RMQIOHandler"
    }
}
```
This factory also supports mixing queue types, for example if there is another service that can produce queue messages for you and it accepts HTTP messages to be produced to a Kafka queue for example.

### Consumer Factory
In codebases where there are a number of handlers to fire off within the same process, this class provides a clean way to abstract the instantiation of each consumer to avoid having to instantiate multiple consumers separately before they can be used to decorate handler functions.

The `ConsumerFactory` class takes a `queue_type` argument to defined what kinds of consumers it will create, and a `port` argument for where it will be listening. It then decorates the handler function using a `route` method on the consumers instance much like the interface for libraries like Flask.

```
consumers = ConsumerFactory(HTTP, port=9000)

@consumers.route("queue_1")
async def id_result_handler(msg):
    pass

@consumers.route("queue_2")
async def id_result_handler(msg):
    pass
```


### Consumer Runner
This class provides the abstraction needed to start the listening for a set of consumer handlers. Its design was inspired by HTTP runners available within frameworks such as Flask that would take a passed app instance that has already been configured, and then trigger it with a run method.

To use it, the `ConsumerRunner` class must first be initialised with the consumer instances and handlers and with the port and ip that it will be running on. The `ConsumerRunner` instance is then started by calling its `run` method. This becomes much simpler when used together with the `ConsumerFactory` class.

```
if __name__ == '__main__':
    app = ConsumerRunner(
        consumers=consumers.instances,
        handlers=consumers.handlers,
        port=PORT,
        host=IP,
    )
    app.run()
```

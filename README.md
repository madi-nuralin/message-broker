# message-broker

A simple wrapper on rabbitmq-c (0.13>=).
See examples and https://www.rabbitmq.com/getstarted.html for quick start.

### some typical example
```cpp
AmqpConnection::Ptr conn = AmqpConnection::createInstance();
conn->open(host, port);
conn->login(vhost, username, password, frame_max);

AmqpChannel::Ptr channel = AmqpChannel::createInstance(conn);
channel->exchangeDeclare("hello", "fanout");

Message msg;
msg.body() = "{\"value":\"hello\"}";
msg.properties().content_type = "application/json";
msg.properties().delivery_mode = 2u;

channel->basicPublish("hello", "", msg);
```
### 1) Basic Messaging pattern

Publisher:
```cpp
MessageBroker broker("amqp://guest:guest@localhost:5672");
MessageBroker::Configuration configuration;

configuration.exchange.name = "hello";
configuration.exchange.type = "fanout";
configuration.exchange.declare = true;

broker.publish(configuration, "hello");
```

Subscriber:
```cpp
MessageBroker broker("localhost", 5672, "guest", "guest", "/");
MessageBroker::Configuration configuration;

configuration.exchange.name = "hello";
configuration.exchange.type = "fanout";
configuration.exchange.declare = true;
configuration.queue.name = "";
configuration.queue.exclusive = false;
configuration.queue.declare = true;
configuration.queue.bind = true;

broker.subscribe(configuration, [](const auto& message) {
	std::cout << "[x] Received b'" << message.getBody() << "'" << std::endl;
});

```


### 2) Request/Response pattern

Publisher:
```cpp
MessageBroker broker("amqp://guest:guest@localhost:5672");
MessageBroker::Configuration c;

c.queue.exclusive = true;
c.queue.declare = true;
c.routing_key = "rpc_queue";

struct timeout tv = {5,0};

auto response = broker.publish(c, "30", &tv);

if (response->ok())
{
	std::cout << "[.] Got  fib(" << 30 << ") = " <<  response->getBody() << std::endl;
}
```

Subscriber:
```cpp
MessageBroker broker("amqp://guest:guest@localhost:5672");
MessageBroker::Configuration c;

c.queue.name = "rpc_queue";
c.queue.declare = true;

broker.subscribe(c, [](const auto& request, auto& response){
	auto number = std::stoi(request.getBody());
	std::cout << "[.] fib(" <<  number << ")" << std::endl;
	response.setBody(std::to_string(fib(number)));
	return true;
});
```

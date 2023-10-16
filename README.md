*message-broker*
*1) Basic Messaging pattern*



*2) Request/Response pattern*:

Publisher:
```cpp
MessageBroker broker;
MessageBroker::Configuration c1;

c1.queue.exclusive = true;
c1.queue.declare = true;
c1.routing_key = "rpc_queue";

struct timeout tv = {5,0};

auto response = broker.publish(c1, "30", &tv);

if (response->ok())
{
	std::cout << "[.] Got  fib(" << 30 << ") = " <<  response->getBody() << std::endl;
}
```

Subscriber:
```cpp
MessageBroker broker;
MessageBroker::Configuration c2;

c2.queue.name = "rpc_queue";
c2.queue.declare = true;

broker.subscribe(c2, [](const auto& request, auto& response){
	auto number = std::stoi(request.getBody());
	std::cout << "[.] fib(" <<  number << ")" << std::endl;
	response.setBody(std::to_string(fib(number)));
	return true;
});
```

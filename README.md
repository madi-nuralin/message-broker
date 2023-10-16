*message-broker*

*Request/Response pattern*:

Publisher:
```cpp
	MessageBroker::Configuration config;
	config.queue.exclusive = true;
	config.queue.declare = true;
	config.routing_key = "rpc_queue";

	struct timeout tv = {5,0};

	auto response = broker.publish(config, "30", &tv);

	if (response->ok())
	{
		std::cout << "[.] Got  fib(" << 30 << ") = " <<  response->getBody() << std::endl;
	}
```

Subscriber:
```cpp
broker.subscribe({
    .queue = {
      .name = "rpc_queue",
      .declare = true
    },
    .on_error = [](const auto &e) {
        std::cerr << error << std::endl;
    }
  }, [](const auto& request, auto& response){
    auto number = std::stoi(request.getBody());
    std::cout << "[.] fib(" <<  number << ")" << std::endl;
    response.setBody(std::to_string(fib(number)));
    return true;
  }
);
```

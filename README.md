*message-broker*

*Request/Response pattern*:

Client:
```cpp
broker.publish({
    .queue = {
        .exclusive = true,
        .declare = true
    },
    .routing_key = "rpc_queue"
  }, number, [&](const auto& response) {
    std::cout << "[.] Got  fib(" << number << ") = " <<  response.getBody() << std::endl;
  }
);
```

Server:
```cpp
broker.subscribe({
    .queue = {
      .name = "rpc_queue",
      .declare = true
    }
  }, [](const auto& request, auto& response){
    auto number = std::stoi(request.getBody());
    std::cout << "[.] fib(" <<  number << ")" << std::endl;
    response.setBody(std::to_string(fib(number)));
    return true;
  }
);
```

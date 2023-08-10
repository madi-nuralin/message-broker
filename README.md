*message-broker*

*Request/Response pattern*:

Client:
```cpp
broker.publish({
    .queue = {.exclusive = true, .declare = true},
    .routing_key = "rpc_queue"
  }, "30", [](const auto& response) {
    g_message("[.] Got  fib(%d) = %s", 30, response.getBody().c_str());
  }
);
```

Server:
```cpp
broker.subscribe({
    .queue = {
      .name = "rpc_queue", 
      .declare = true
    }, [](const auto& request, auto& response){
      auto n = std::stoi(request.getBody());
      g_message("[.] fib('%d')", n);
      response.setBody(std::to_string(fib(n)));
      return true;
    }
  }
);
```

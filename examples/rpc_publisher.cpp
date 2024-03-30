#include <iostream>

#include "../message_broker.hpp"

using namespace gs;

int
main(int argc, char const* argv[])
{
  MessageBroker broker("localhost", 5672, "guest", "guest", "/");
  MessageBroker::Configuration configuration;

  configuration.queue.exclusive = true;
  configuration.queue.declare = true;
  configuration.routing_key = "rpc_queue";

  struct timeval timeout
  {
    35, 0
  };

  MessageBroker::Request req;
  req.body() = "30";

  auto response = broker.publish(configuration, req, &timeout);

  if (response) {
    if (response->ok()) {
      std::cout << "[.] Got  fib(" << 30 << ") = " << response->body()
                << std::endl;
    }
  }

  return 0;
}

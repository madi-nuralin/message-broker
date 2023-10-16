#include <iostream>
#include "../message_broker.hpp"

using namespace gammasoft;

int main(int argc, char const *argv[])
{
	MessageBroker broker("localhost", 5672, "guest", "guest", "/");
	MessageBroker::Configuration configuration;

	configuration.queue.exclusive = true;
	configuration.queue.declare = true;
	configuration.routing_key = "rpc_queue";

	struct timeval timeout{5,0};

	auto response = broker.publish(configuration, "30", &timeout);

	if (response->ok())
	{
		std::cout << "[.] Got  fib(" << 30 << ") = " <<  response->getBody() << std::endl;
	}

  return 0;
}

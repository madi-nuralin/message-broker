#include <iostream>

#include "../message_broker.hpp"

using namespace soft;

int main(int argc, char const *argv[])
{
	MessageBroker broker("amqp://guest:guest@localhost:5672");
	MessageBroker::Configuration configuration;

	configuration.exchange.name = "hello";
	configuration.exchange.type = "fanout";
	configuration.exchange.declare = true;

	broker.publish(configuration, "hello");

	return 0;
}

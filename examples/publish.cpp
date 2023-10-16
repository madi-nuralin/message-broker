#include <iostream>

#include "../message_broker.hpp"

using namespace gammasoft;

int main(int argc, char const *argv[])
{
	MessageBroker broker;
	MessageBroker::Configuration configuration;

	configuration.exchange.name = "hello";
	configuration.exchange.type = "fanout";
	configuration.exchange.declare = true;

	broker.publish(configuration, "hello");

	return 0;
}

#include <iostream>

#include "../message_broker.hpp"

using namespace soft;
using namespace soft::amqp;

int main(int argc, char const *argv[])
{
	MessageBroker broker("amqp://guest:guest@localhost:5672");
	MessageBroker::Configuration configuration;
	configuration.exchange.name = "hello";
	configuration.exchange.type = "fanout";
	configuration.exchange.declare = true;
	broker.publish(configuration, "hello");

	Connection2::Ptr connection = Connection2::createInstance();
	Channel2::Ptr channel = Channel2::createInstance(connection);
	//channel->basicPublish(Message2("nnkn"));

	return 0;
}

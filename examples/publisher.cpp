#include <iostream>

#include "../message_broker.hpp"

using namespace soft;
using namespace soft::amqp;

int main(int argc, char const *argv[])
{
	MessageBroker broker("amqp://guest:guest@localhost:5672");
	MessageBroker::Configuration cfg;
	cfg.exchange.name = "hello";
	cfg.exchange.type = "fanout";
	cfg.exchange.declare = true;
	broker.publish(cfg, "hello");

	return 0;
}

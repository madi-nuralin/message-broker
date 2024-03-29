#include <iostream>

#include "../message_broker.hpp"
#include <unistd.h>
using namespace gs;
using namespace gs::amqp;

int main(int argc, char const *argv[])
{
	MessageBroker broker("amqp://guest:guest@localhost:5672");
	MessageBroker::Configuration cfg;
	cfg.exchange.name = "hello";
	cfg.exchange.type = "fanout";
	cfg.exchange.declare = true;
	for (int i = 0; i < 1000; ++i)
	{
		broker.publish(cfg, "hello");
		usleep(1000);
	}
	

	return 0;
}

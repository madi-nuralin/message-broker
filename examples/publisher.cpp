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
		MessageBroker::Message msg;
		msg.body() = "hello";
		msg.properties().app_id = "my_app";
		msg.properties().message_id = "4";

		broker.publish(cfg, msg);
		usleep(1000);
	}
	/*cfg.exchange.name = "cabinet.notifications_direct";
	cfg.exchange.type = "topic";
	cfg.exchange.durable = true;
	cfg.exchange.declare = true;
	cfg.queue.name = "cabinet.notifications";
	cfg.queue.durable = true;
	cfg.queue.declare = true;
	cfg.queue.bind = true;
	cfg.routing_key = "cabinet.notifications.1";
	cfg.routing_pattern = "cabinet.notifications.*";*/


	return 0;
}

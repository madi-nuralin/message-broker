#include <iostream>

#include "../message_broker.hpp"
#include <unistd.h>
using namespace gs;
using namespace gs::amqp;

int main(int argc, char const *argv[])
{

	MessageBroker broker("amqp://guest:guest@localhost:5672");
	MessageBroker::Configuration cfg;
	MessageBroker::Message msg;

	using Table = AmqpTable;
	using TableEntry = AmqpTableEntry;
	using TableValue = AmqpTableValue;

	Table table_in;
	table_in.insert(TableEntry("x-message-ttl", TableValue(int32_t(3600000))));
	//table_in.insert(TableEntry("bool_key", true));
	//table_in.insert(TableEntry("uint8_key", uint8_t(8)));
	//table_in.insert(TableEntry("int8_key", int8_t(8)));
	//table_in.insert(TableEntry("uint16_key", uint16_t(16)));
	//table_in.insert(TableEntry("int16_key", int16_t(16)));
	//table_in.insert(TableEntry("uint32_key", uint32_t(32)));
	//table_in.insert(TableEntry("int32_key", int32_t(32)));
	//table_in.insert(TableEntry("int64_key", int64_t(64)));
	//table_in.insert(TableEntry("float_key", float(1.5)));
	//table_in.insert(TableEntry("double_key", double(2.25)));
	//table_in.insert(TableEntry("string_key", "A string!"));

	msg.body() = "Bye";
	msg.properties().app_id = "my_app";
	msg.properties().message_id = "r_r";

	cfg.exchange.name = "cabinet.notifications_direct";
	cfg.exchange.type = "direct";
	cfg.exchange.declare = true;
	//cfg.queue.name = "cabinet.notifications";
	//cfg.queue.durable = true;
	//cfg.queue.declare = true;
	//cfg.queue.bind = true;
	//cfg.queue.arguments = table_in;
	cfg.routing_key = "cabinet.notifications.2";
	cfg.routing_pattern = "cabinet.notifications.*";
	broker.publish(cfg, msg);

	return 0;
}

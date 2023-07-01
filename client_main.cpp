#include <iostream>

#include "message-broker.hpp"

int main(int argc, char const *argv[])
{
	MessageBroker msgBroker("localhost", 5672);
	msgBroker.send("amq.direct", "logs", "{QUERY_DATA}");
	msgBroker.send("amq.direct", "logs", "{QUERY_DATA}");

	return 0;
}
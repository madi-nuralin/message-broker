#include "message-broker.hpp"
#include <stdio.h>

int main(int argc, char const *argv[])
{
	MessageBroker msgBroker("localhost", 5672);
	msgBroker.send("logs", "{QUERY_NAME}", "{QUERY_DATA}");

	return 0;
}
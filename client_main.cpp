#include <iostream>

#include "message-broker.hpp"

const char *query1 = "{\"query-name\":\"get-user-by-id\",\"query-data\":{\"id\":1}}";
const char *query2 = "{\"query-name\":\"get-user-by-id\",\"query-data\":{\"id\":2}}";

int main(int argc, char const *argv[])
{
	MessageBroker msgBroker("localhost", 5672);
	msgBroker.send("amq.direct", "logs", query1);
	msgBroker.send("amq.direct", "logs", query2);

	return 0;
}
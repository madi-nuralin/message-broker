#include "message-broker.hpp"

const char *query1 = "{\"query_name\":\"get-user-by-id\",\"query_data\":{\"id\":1}}";
const char *query2 = "{\"query_name\":\"get-user-by-id\",\"query_data\":{\"id\":3}}";

int main(int argc, char const *argv[])
{
	MessageBroker msgBroker("localhost", 5672);

	auto resp = msgBroker.send("amq.direct", "logs", query1);
	if (resp) {
		fprintf(stderr, "Reponse: %s\n", resp->serialize());
	}
	
	resp = msgBroker.send("amq.direct", "logs", query2);
	if (resp) {
		fprintf(stderr, "Reponse: %s\n", resp->serialize());
	}
	return 0;
}
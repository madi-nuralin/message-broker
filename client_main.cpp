#include "message-broker.hpp"

const char *query1 = "{\"query_name\":\"get-user-by-id\",\"query_data\":{\"id\":1}}";
const char *query2 = "{\"query_name\":\"get-user-by-id\",\"query_data\":{\"id\":3}}";

int main(int argc, char const *argv[])
{
	try {
		MessageBroker broker("localhost", 5672);
		MessageBroker::Response::Ptr resp;
		
		broker.publish("amq.direct", "users", query1);
		broker.publish("amq.direct", "users", query1, resp);
		broker.publish("amq.direct", "users", query1, [](const MessageBroker::Response& resp) {
			if (resp.ok()) {
				resp.serializeBody();
			}
		});
	} catch (const std::runtime_error& e) {}
	return 0;
}

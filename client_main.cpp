#include <iostream>
#include "message_broker.hpp"

const char *query1 = "{\"query_name\":\"get-user-by-id\",\"query_data\":{\"id\":1}}";
const char *query2 = "{\"query_name\":\"get-user-by-id\",\"query_data\":{\"id\":3}}";


int main(int argc, char const *argv[])
{
	MessageBroker broker;

	/*MessageBroker::Message m;
	m.setBody(query1);
	std::cout << m.serialize() << std::endl;

	MessageBroker::Request req;
	req.setBody(query1);
	std::cout << req.serialize() << std::endl;

	MessageBroker::Response res(req);
	std::cout << res.serialize() << std::endl;
	res.setReason("failed to find user!");
	if (!res.ok()) {
		std::cout << res.reason() << std::endl;
	}*/

	broker.publish("amq.direct", "test", query1);
	/*broker.publish(
		MessageBroker::Exchange("amq.direct"),
		MessageBroker::Queue(),
		"logs",
		"hello"
	); */

	/*broker.subsribe(
		MessageBroker::Exchange("logs", "direct"),
		MessageBroker::Queue(""),
		[](const VistaBroker::Request &request, VistaBroker::Reponse &response) {

		}
	)*/

}

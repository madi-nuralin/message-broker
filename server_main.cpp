#include "message-broker.hpp"

int main(int argc, char const *argv[])
{
	MessageBroker msgBroker("localhost", 5672);

	msgBroker.listen("amq.direct", "logs", [](const MessageBroker::Request &request, MessageBroker::Response &response){
		//response.body.query.reply = "{iin:12}";
		return true;
	});

	while (1) {}
	return 0;
}
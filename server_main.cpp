#include "message-broker.hpp"

bool callback(const MessageBroker::Request &request, MessageBroker::Response &response)
{
	return true;
}

int main(int argc, char const *argv[])
{
	MessageBroker msgBroker("localhost", 5672);
	msgBroker.listen("logs", callback);

	return 0;
}
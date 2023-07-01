#ifndef MESSAGE_BROKER_HPP
#define MESSAGE_BROKER_HPP

#include <memory>
#include <string>

#include <amqp.h>
#include <amqp_tcp_socket.h>

class MessageBroker
{
	amqp_socket_t *socket;
	amqp_connection_state_t conn;

public:
	MessageBroker(const char* hostname, int port);
	virtual ~MessageBroker();

	struct QueryInterface
	{
		typedef std::shared_ptr<QueryInterface> Ptr;

		static const char * const QUERY_REQUEST;
		static const char * const QUERY_RESPONSE;
		static const char * const QUERY_ERROR;

		int reqid;
		std::string type, body;

		const char* serialize();
		bool parse(const char *json_str);
	};

	struct Request : public QueryInterface {
		Request() {
			type = QueryInterface::QUERY_REQUEST;
		}
	};

	struct Response : public QueryInterface {
		Response() {
			type = QueryInterface::QUERY_RESPONSE;
		}
	};

	class Queue
	{
		amqp_bytes_t queuename;
	public:
		Queue();
		~Queue();
	};

	MessageBroker::Response::Ptr send(
		const char *exchange,
		const char *routingkey,
		const char *query
	);

	int listen(
		const char *exchange,
		const char *bindingkey,
		bool (*callback)(const MessageBroker::Request &request, MessageBroker::Response &response)
	);
};

#endif //MESSAGE_BROKER_HPP
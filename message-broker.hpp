#ifndef MESSAGE_BROKER_HPP
#define MESSAGE_BROKER_HPP

#include <string.h>
#include <memory>

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

		const char * const QUERY_REQUEST = "request";
		const char * const QUERY_RESPONSE = "response";
		const char * const QUERY_ERROR = "error";

		int reqid;
		char* type;
		struct {
			union {
				char* reason;
				struct {
					union {
						char* reply;
						struct {
							char* name;
							char* data;
						};
					};
				} query;
			};
		} body;

		~QueryInterface();

		const char* serialize();
		bool parse(const char *json_str);
	};

	struct Request : public QueryInterface {
		Request() {
			type = strdup(QueryInterface::QUERY_REQUEST);
		}
	};

	struct Response : public QueryInterface {
		Response() {
			type = strdup(QueryInterface::QUERY_RESPONSE);
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
		const char *routingkey,
		const char *queryname,
		const char *querydata
	);

	int listen(
		const char *bindingkey,
		bool (*callback)(const MessageBroker::Request &request, MessageBroker::Response &response)
	);
};

#endif //MESSAGE_BROKER_HPP
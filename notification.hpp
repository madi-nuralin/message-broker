#ifndef NOTIFICATION_HPP
#define NOTIFICATION_HPP

#include <stdlib.h>
#include <memory>

#include <amqp.h>
#include <amqp_tcp_socket.h>

class Notification
{
	amqp_socket_t *socket;
	amqp_connection_state_t conn;

public:
	Notification(const char* hostname, int port);
	virtual ~Notification();

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

	Notification::Response::Ptr send(
		const char *routingkey, 
		const char *queryname, 
		const char *querydata
	);

	/*void listen(const char *bindingkey, void ((int*)callback)(void));*/
};

#endif //NOTIFICATION_HPP
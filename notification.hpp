#ifndef NOTIFICATION_HPP
#define NOTIFICATION_HPP

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
		const char * const QUERY_REQUEST = "request";
		const char * const QUERY_RESPONSE = "response";
		const char * const QUERY_ERROR = "error";

		int reqid;
		const char *type;
		struct {
			union {
				const char *reason;
				struct {
					union {
						const char *reply;
						struct {
							const char *name, *data;
						};
					};
				} query;
			};
		} body;

		const char* serialize();
		bool parse(const char *json_str);
	};

	struct Request : public QueryInterface {};
	struct Response : public QueryInterface {};

	/*class Queue
	{
		amqp_bytes_t queuename;
	public:
		Queue();
		~Queue();
	};

	Notification::Response send(const char *routingkey, const char *queryName, const char *queryData, bool replyCallback);
	void listen(const char *bindingkey, void ((int*)callback)(void));*/
};

#endif //NOTIFICATION_HPP
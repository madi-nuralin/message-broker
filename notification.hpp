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

	struct Request
	{
		int reqid;
		const char *type;
		struct {
			const char *queryName;
			const char *queryData;
		} body;

		char* serialize();
		Request deserialize(const char *json_str);
	};

	struct Response
	{
		int reqid;
		const char *type;
		const char *body;

		struct {
			const char *queryName;
			const char *queryData;
		} body;

		char* serialize();
		Request deserialize(const char *json_str);
	};

	class Queue
	{
		amqp_bytes_t queuename;
	public:
		Queue();
		~Queue();
	};

	Notification::Response send(const char *routingkey, const char *queryName, const char *queryData, bool replyCallback);
	void listen(const char *bindingkey, void ((int*)callback)(void));
};

#endif //NOTIFICATION_HPP
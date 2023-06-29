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
};

#endif //NOTIFICATION_HPP
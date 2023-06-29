#include "notification.hpp"
#include "utils.h"

Notification::Notification(const char* hostname, int port)
{
	int status;

	conn = amqp_new_connection();

	socket = amqp_tcp_socket_new(conn);
	if (!socket) {
		die("creating TCP socket");
	}

	status = amqp_socket_open(socket, hostname, port);
	if (status) {
		die("opening TCP socket");
	}

	die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
			"guest", "guest"),
		"Logging in");
	amqp_channel_open(conn, 1);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");
}

Notification::~Notification()
{
	die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS),
			"Closing channel");
	die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
			"Closing connection");
	die_on_error(amqp_destroy_connection(conn), "Ending connection");
}

/*Notification::Queue Notification::via(const char *queuename)
{
	{
		amqp_queue_declare_ok_t *r = amqp_queue_declare(
			conn, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
		die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
		queuename = amqp_bytes_malloc_dup(r->queue);
		if (queuename.bytes == NULL) {
			fprintf(stderr, "Out of memory while copying queue name");
			return 1;
		}
	}
}*/
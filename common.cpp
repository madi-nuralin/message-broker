#include "common.h"
#include "utils.h"
#include <iostream>
#include <rabbitmq-c/tcp_socket.h>

Connection::Connection(
	const std::string &host, int port,
	const std::string &user,
	const std::string &password,
	const std::string &vhost, int frame_max) {
	if (host.empty()) {
		throw std::runtime_error("host is not specified, it is required");
	}
	if (vhost.empty()) {
		throw std::runtime_error("vhost is not specified, it is required");
	}
	if (port <= 0) {
		throw std::runtime_error("port is not valid, it must be a positive number");
	}

	state = amqp_new_connection();

	socket = amqp_tcp_socket_new(state);
	if (!socket) {
		die("creating TCP socket");
	}

	int status = amqp_socket_open(socket, host.c_str(), port);
	if (status) {
		die("opening TCP socket");
	}

	die_on_amqp_error(amqp_login(state, vhost.c_str(), 0, frame_max, 0, AMQP_SASL_METHOD_PLAIN,
			user.c_str(), password.c_str()),
		"Logging in");

	/*std::thread worker([this]() {
		while(1) {

		}
	});
	worker.detach();*/
}

Connection::~Connection() {
	std::unique_lock<std::mutex> lock(mutex);
	die_on_amqp_error(amqp_connection_close(state, AMQP_REPLY_SUCCESS),
			"Closing connection");
	die_on_error(amqp_destroy_connection(state), "Ending connection");
}

static uint16_t serial = 0;

Channel::Channel(Connection *connection) {
	std::unique_lock<std::mutex> lock(connection->mutex);
	this->connection = connection;
	this->id = ++serial;

	amqp_channel_open(connection->state, id);
	die_on_amqp_error(amqp_get_rpc_reply(connection->state),
			"Opening channel");
}

Channel::~Channel() {
	std::unique_lock<std::mutex> lock(connection->mutex);
	die_on_amqp_error(amqp_channel_close(connection->state, id, AMQP_REPLY_SUCCESS),
			"Closing channel");
}

void Channel::consume(const std::string &queue) {

}

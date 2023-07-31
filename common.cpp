#include "common.h"
#include "utils.h"
#include <iostream>
#include <thread>
#include <rabbitmq-c/tcp_socket.h>

// serial auto-increment for channel id
static uint16_t serial = 0;

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

	std::thread worker([this]() {
		for (;;) {
			std::unique_lock<std::mutex> lock(mutex);

			if (!channels.empty()) {
				amqp_rpc_reply_t res;
				amqp_envelope_t envelope;

				amqp_maybe_release_buffers(state);

				struct timeval tv = {.tv_sec = 0, .tv_usec = 100};

				res = amqp_consume_message(state, &envelope, &tv, 0);

				if (AMQP_RESPONSE_NORMAL != res.reply_type) {
					continue;
				}

				channels[envelope.channel]->envelopes.push(envelope);
				//std::cout << (int)envelope.channel << std::endl;
			}
	    }
	});
	worker.detach();
}

Connection::~Connection() {
	std::unique_lock<std::mutex> lock(mutex);
	die_on_amqp_error(amqp_connection_close(state, AMQP_REPLY_SUCCESS),
			"Closing connection");
	die_on_error(amqp_destroy_connection(state), "Ending connection");
}

Channel::Channel(Connection *connection) {
	std::unique_lock<std::mutex> lock(connection->mutex);
	this->id = ++serial;
	this->connection = connection;
	this->connection->channels[id] = this;

	amqp_channel_open(connection->state, id);
	die_on_amqp_error(amqp_get_rpc_reply(connection->state),
			"Opening channel");
}

Channel::~Channel() {
	std::unique_lock<std::mutex> lock(connection->mutex);
	die_on_amqp_error(amqp_channel_close(connection->state, id, AMQP_REPLY_SUCCESS),
			"Closing channel");
}

std::string Channel::setup_queue(const std::string &queue_name, const std::string &exchange, const std::string &routing_key, bool passive, bool durable, bool auto_delete, bool exclusive)
{
	std::unique_lock<std::mutex> lock(connection->mutex);
	amqp_queue_declare_ok_t *r = amqp_queue_declare(
		connection->state, id, queue_name.empty() 
			? amqp_empty_bytes
			: amqp_cstring_bytes(queue_name.c_str()),
		passive, durable, exclusive, auto_delete, amqp_empty_table
	);
	die_on_amqp_error(amqp_get_rpc_reply(connection->state), "Declaring queue");

	/*amqp_queue_bind(
		connection->state,
		id,
		queue_name.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(queue_name.c_str()),
		exchange.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(exchange.c_str()),
		routing_key.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(routing_key.c_str()),
		amqp_empty_table);
	die_on_amqp_error(amqp_get_rpc_reply(connection->state), "Binding queue");*/
	return std::string((char*)r->queue.bytes, r->queue.len);
}

void Channel::publish(const std::string &exchange, const std::string &routing_key, const Message &message, bool mandatory, bool immediate)
{
	std::unique_lock<std::mutex> lock(connection->mutex);
	die_on_error(amqp_basic_publish(connection->state, id,
		exchange.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(exchange.c_str()),
		routing_key.empty()
			? amqp_empty_bytes
			: amqp_cstring_bytes(routing_key.c_str()),
		mandatory, immediate, &message.properties, message.body),
		"Publishing");
}

void Channel::consume(const std::string &queue_name, void (*callback)(const Envelope &envelope), const std::string &consumer_tag, bool no_local, bool no_ack, bool exclusive)
{
	{
		std::unique_lock<std::mutex> lock(connection->mutex);
		amqp_basic_consume(connection->state, id,
			queue_name.empty()
				? amqp_empty_bytes
				: amqp_cstring_bytes(queue_name.c_str()),
			consumer_tag.empty()
				? amqp_empty_bytes
				: amqp_cstring_bytes(consumer_tag.c_str()),
			no_local, no_ack, exclusive, amqp_empty_table
		);
		die_on_amqp_error(amqp_get_rpc_reply(connection->state), "Consuming");
	}

	std::cout << (int)id << ": listening\n";
	
	for (;;) {
		if (!envelopes.empty()) {
			auto envelope = Envelope(envelopes.front());
			envelopes.pop();
			callback(envelope);
		}
	}
}

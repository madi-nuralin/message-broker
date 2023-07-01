#include <stdio.h>
#include <assert.h>
#include <thread>

#include <glib-object.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#include "message-broker.hpp"
#include "utils.h"

MessageBroker::MessageBroker(const char* hostname, int port)
{
	conn = amqp_new_connection();

	socket = amqp_tcp_socket_new(conn);
	if (!socket) {
		die("creating TCP socket");
	}

	int status = amqp_socket_open(socket, hostname, port);
	if (status) {
		die("opening TCP socket");
	}

	die_on_amqp_error(amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN,
			"guest", "guest"),
		"Logging in");
	amqp_channel_open(conn, 1);
	die_on_amqp_error(amqp_get_rpc_reply(conn), "Opening channel");
}

MessageBroker::~MessageBroker()
{
	die_on_amqp_error(amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS),
			"Closing channel");
	die_on_amqp_error(amqp_connection_close(conn, AMQP_REPLY_SUCCESS),
			"Closing connection");
	die_on_error(amqp_destroy_connection(conn), "Ending connection");
}

MessageBroker::Response::Ptr MessageBroker::send(const char *exchange, const char *routingkey, const char *query)
{
	char const *messagebody;

	MessageBroker::Response::Ptr response;
	MessageBroker::Request request;
	request.reqid = 1;
	request.body = query;
	messagebody = request.serialize();

	amqp_bytes_t reply_to_queue;

	/*
		create private reply_to queue
	*/

	{
		amqp_queue_declare_ok_t *r = amqp_queue_declare(
			conn, 1, amqp_empty_bytes, 0, 0, 0, 1, amqp_empty_table);
		die_on_amqp_error(amqp_get_rpc_reply(conn), "Declaring queue");
		reply_to_queue = amqp_bytes_malloc_dup(r->queue);
		if (reply_to_queue.bytes == NULL) {
			fprintf(stderr, "Out of memory while copying queue name");
			return nullptr;
		}
	}

	/*
		send the message
	*/

	{
		/*
			set properties
		*/
		amqp_basic_properties_t props;
		props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG |
					   AMQP_BASIC_DELIVERY_MODE_FLAG |
					   AMQP_BASIC_REPLY_TO_FLAG |
					   AMQP_BASIC_CORRELATION_ID_FLAG;
		props.content_type = amqp_cstring_bytes("application/json");
		props.delivery_mode = 2; /* persistent delivery mode */
		props.reply_to = amqp_bytes_malloc_dup(reply_to_queue);
		if (props.reply_to.bytes == NULL) {
			fprintf(stderr, "Out of memory while copying queue name");
			return nullptr;
		}
		props.correlation_id = amqp_cstring_bytes("1");

		/*
			publish
		*/
		die_on_error(amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange),
				amqp_cstring_bytes(routingkey), 0, 0,
				&props, amqp_cstring_bytes(messagebody)),
			"Publishing");

		amqp_bytes_free(props.reply_to);
	}

	/*
		wait an answer
	*/

	{
		amqp_basic_consume(conn, 1, reply_to_queue, amqp_empty_bytes, 0, 1, 0,
			amqp_empty_table);
		die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");
		amqp_bytes_free(reply_to_queue);

		{
			amqp_frame_t frame;
			int result;

			amqp_basic_deliver_t *d;
			amqp_basic_properties_t *p;
			size_t body_target;
			size_t body_received;

			for(;;) {
				amqp_maybe_release_buffers(conn);
				result = amqp_simple_wait_frame(conn, &frame);
				printf("Result: %d\n", result);
				if (result < 0) {
					break;
				}

				printf("Frame type: %u channel: %u\n", frame.frame_type, frame.channel);
				if (frame.frame_type != AMQP_FRAME_METHOD) {
					continue;
				}

				printf("Method: %s\n", amqp_method_name(frame.payload.method.id));
				if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) {
					continue;
				}

				d =(amqp_basic_deliver_t *)frame.payload.method.decoded;
				printf("Delivery: %u exchange: %.*s routingkey: %.*s\n",
					(unsigned)d->delivery_tag,(int)d->exchange.len,
					(char *)d->exchange.bytes,(int)d->routing_key.len,
					(char *)d->routing_key.bytes);

				result = amqp_simple_wait_frame(conn, &frame);
				if (result < 0) {
					break;
				}

				if (frame.frame_type != AMQP_FRAME_HEADER) {
					fprintf(stderr, "Expected header!");
					abort();
				}
				p = (amqp_basic_properties_t *)frame.payload.properties.decoded;
				if (p->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
					printf("Content-type: %.*s\n",(int)p->content_type.len,
						(char *)p->content_type.bytes);
				}
				printf("----\n");

				body_target =(size_t)frame.payload.properties.body_size;
				body_received = 0;

				while(body_received < body_target) {
					result = amqp_simple_wait_frame(conn, &frame);
					if (result < 0) {
						break;
					}

					if (frame.frame_type != AMQP_FRAME_BODY) {
						fprintf(stderr, "Expected body!");
						abort();
					}

					body_received += frame.payload.body_fragment.len;
					assert(body_received <= body_target);

					amqp_dump(frame.payload.body_fragment.bytes,
					    frame.payload.body_fragment.len);
			    }

				if (body_received != body_target) {
					/* Can only happen when amqp_simple_wait_frame returns <= 0 */
					/* We break here to close the connection */
					break;
				}

				/* everything was fine, we can quit now because we received the reply */
				break;
			}
		}
	}

	return response;
}

int MessageBroker::listen(const char *exchange, const char *bindingkey, bool (*callback)(const MessageBroker::Request &request, MessageBroker::Response &response))
{
	std::thread t1([this, exchange, bindingkey, callback]()
	{
		amqp_bytes_t queuename;
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

		amqp_queue_bind(conn, 1, queuename, amqp_cstring_bytes(exchange),
			amqp_cstring_bytes(bindingkey), amqp_empty_table);
		die_on_amqp_error(amqp_get_rpc_reply(conn), "Binding queue");

		amqp_basic_consume(conn, 1, queuename, amqp_empty_bytes, 0, 1, 0,
			amqp_empty_table);
		die_on_amqp_error(amqp_get_rpc_reply(conn), "Consuming");

		{
			for(;;) {
				amqp_rpc_reply_t res;
				amqp_envelope_t envelope;

				amqp_maybe_release_buffers(conn);

				res = amqp_consume_message(conn, &envelope, NULL, 0);

				if (AMQP_RESPONSE_NORMAL != res.reply_type) {
					break;
				}

				printf("Delivery %u, exchange %.*s routingkey %.*s\n",
					(unsigned)envelope.delivery_tag,(int)envelope.exchange.len,
					(char *)envelope.exchange.bytes,(int)envelope.routing_key.len,
					(char *)envelope.routing_key.bytes);

				if (envelope.message.properties._flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
					printf("Content-type: %.*s\n",
						(int)envelope.message.properties.content_type.len,
						(char *)envelope.message.properties.content_type.bytes);
				}
				printf("----\n");

				amqp_dump(envelope.message.body.bytes, envelope.message.body.len);

				/*
					send reply
				*/

				{
					/*
						set properties
					*/
					amqp_basic_properties_t props;
					props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG |
								   AMQP_BASIC_DELIVERY_MODE_FLAG |
								   AMQP_BASIC_REPLY_TO_FLAG |
								   AMQP_BASIC_CORRELATION_ID_FLAG;
					props.content_type = amqp_cstring_bytes("application/json");
					props.delivery_mode = 2; /* persistent delivery mode */
					props.reply_to = amqp_bytes_malloc_dup(envelope.message.properties.reply_to);
					if (props.reply_to.bytes == NULL) {
						fprintf(stderr, "Out of memory while copying queue name");
						return 1;
					}
					props.correlation_id = envelope.message.properties.correlation_id;

					MessageBroker::Request request;
					MessageBroker::Response response;
					request.parse((const char*)envelope.message.body.bytes);
					response.reqid = request.reqid;
					callback(request, response);

					/*
						publish
					*/
					die_on_error(amqp_basic_publish(conn, 1, amqp_empty_bytes,
							amqp_cstring_bytes((char *)envelope.message.properties.reply_to.bytes), 0, 0,
							&props, amqp_cstring_bytes((const char*)response.serialize())),
						"Publishing");

					amqp_bytes_free(props.reply_to);
				}

				amqp_destroy_envelope(&envelope);
			}
		}

		amqp_bytes_free(queuename);
	});

	t1.detach();
}

const char* MessageBroker::QueryInterface::serialize()
{
	JsonBuilder *builder = json_builder_new();
	json_builder_begin_object(builder);

	json_builder_set_member_name(builder, "reqid");
	json_builder_add_int_value(builder, reqid);

	json_builder_set_member_name(builder, "type");
	json_builder_add_string_value(builder, type.c_str());

	json_builder_set_member_name(builder, "body");
	json_builder_add_string_value(builder, body.c_str());
	
	json_builder_end_object(builder);

	JsonGenerator *gen = json_generator_new();
	JsonNode *root = json_builder_get_root(builder);
	json_generator_set_root(gen, root);
	gchar *json_str = json_generator_to_data(gen, NULL);

	json_node_free(root);
	g_object_unref(gen);
	g_object_unref(builder);

	return (const char *)json_str;
}

bool MessageBroker::QueryInterface::parse(const char *json_str)
{
	JsonParser *parser = json_parser_new();
	json_parser_load_from_data(parser, json_str, -1, NULL);

	JsonReader *reader = json_reader_new(json_parser_get_root(parser));

	json_reader_read_member(reader, "reqid");
	reqid = json_reader_get_int_value(reader);
	json_reader_end_member(reader);

	json_reader_read_member(reader, "type");
	type = json_reader_get_string_value(reader);
	json_reader_end_member(reader);

	json_reader_read_member(reader, "body");
	body = json_reader_get_string_value(reader);
	json_reader_end_member(reader);

	g_object_unref(reader);
	g_object_unref(parser);

	return true;
}

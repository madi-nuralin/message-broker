#ifndef  __GAMMA__VISTA_FOUNDATION_SERVER__VISTA_MESSAGE_BROKER__CLASS__H__
#define  __GAMMA__VISTA_FOUNDATION_SERVER__VISTA_MESSAGE_BROKER__CLASS__H__

#include <map>
#include <string>
#include <memory>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

namespace gammasoft {

class VistaMessageBroker
{
public:
	VistaMessageBroker(
		const std::string &host = "127.0.0.1", int port = 5672,
		const std::string &username = "guest",
		const std::string &password = "guest",
		const std::string &vhost = "/", int frame_max = 131072);
	~VistaMessageBroker();

	class Statement
	{
	public:
		Statement();
		Statement(const std::string& str);
		virtual ~Statement();

		std::string reqid() const { return m_reqid; }
		bool setBody(const JsonNode *json_node, std::string *error = nullptr);
		bool setBody(const std::string &body, std::string *error = nullptr);
		virtual std::string serialize() const;
		std::string serializeBody() const;

	protected:
		std::string m_reqid, m_type;
		JsonNode *m_body;
	};

	class Request : public Statement
	{
	public:
		Request() : Statement() {}
		Request(const std::string &str) : Statement(str) {}

	protected:
		std::string m_type = "request";
	};

	class Response : public Statement
	{
	public:
		Response(const std::string &str);
		Response(const Request &request) : Statement() {
			m_reqid = request.reqid();
			m_type = "response";
		}

		~Response() {}

		std::string serialize() const override;
		void setReason(const std::string &reason);
		std::string reason() const { return m_reason; }
		bool ok() const { return m_type != "error"; }

	protected:
		std::string m_reason;
	};

	struct Exchange {
		std::string name, type;
		bool passive, durable, auto_delete, internal;
	};

	struct Queue {
		std::string name;
		bool passive, durable, auto_delete, exclusive;
	};

	struct Configuration {
		Exchange exchange;
		Queue queue;
		bool queue_bind;
		std::string routing_key;
		std::string binding_key;
	};

	class Message : public amqp_message_t
	{
	public:
		typedef std::shared_ptr<Message> Ptr;

		Message(const std::string &body) {
			this->properties._flags = 0;
			this->body = amqp_bytes_malloc_dup(amqp_cstring_bytes(body.c_str()));
		}

		~Message() {
			//amqp_destroy_message(this);
		}

		void setProperty(const std::string & key, const char *value) {
			*reinterpret_cast<amqp_bytes_t*>(schema[key].ptr) = amqp_cstring_bytes(value);
			this->properties._flags |= schema[key].flag;
		}

		void setProperty(const std::string & key, uint8_t value) {
			*reinterpret_cast<uint8_t*>(schema[key].ptr) = value;
			this->properties._flags |= schema[key].flag;
		}

		void setProperty(const std::string & key, uint64_t value) {
			*reinterpret_cast<uint64_t*>(schema[key].ptr) = value;
			this->properties._flags |= schema[key].flag;
		}

	protected:
		struct PropertyDescriptor {
			amqp_flags_t flag;
			void *ptr;
		};

		std::map<std::string, PropertyDescriptor> schema {
			{"Content-Type", PropertyDescriptor{AMQP_BASIC_CONTENT_TYPE_FLAG, &properties.content_type}},
			{"Content-Encoding", PropertyDescriptor{AMQP_BASIC_CONTENT_ENCODING_FLAG, &properties.content_encoding}},
			{"Delivery-Mode", PropertyDescriptor{AMQP_BASIC_DELIVERY_MODE_FLAG, &properties.delivery_mode}},
			{"Priority", PropertyDescriptor{AMQP_BASIC_PRIORITY_FLAG, &properties.priority}},
			{"Correlation-Id", PropertyDescriptor{AMQP_BASIC_CORRELATION_ID_FLAG, &properties.correlation_id}},
			{"Reply-To", PropertyDescriptor{AMQP_BASIC_REPLY_TO_FLAG, &properties.reply_to}},
			{"Expiration", PropertyDescriptor{AMQP_BASIC_EXPIRATION_FLAG, &properties.expiration}},
			{"Message-Id", PropertyDescriptor{AMQP_BASIC_MESSAGE_ID_FLAG, &properties.message_id}},
			{"Timestamp", PropertyDescriptor{AMQP_BASIC_TIMESTAMP_FLAG, &properties.timestamp}},
			{"Type", PropertyDescriptor{AMQP_BASIC_TYPE_FLAG, &properties.type}},
			{"User-Id", PropertyDescriptor{AMQP_BASIC_USER_ID_FLAG, &properties.user_id}},
			{"App-Id", PropertyDescriptor{AMQP_BASIC_APP_ID_FLAG, &properties.app_id}},
			{"Cluster-Id", PropertyDescriptor{AMQP_BASIC_CLUSTER_ID_FLAG, &properties.cluster_id}}
		};
	};

	struct Envelope : public amqp_envelope_t
	{
		Envelope(const amqp_envelope_t &envelope) {
			this->message = envelope.message;
			this->routing_key = envelope.routing_key;
			this->exchange = envelope.exchange;
			this->consumer_tag = envelope.consumer_tag;
		}

		~Envelope() {
			// envelope a pointer to a amqp_envelope_t object. Caller
            // should call #amqp_destroy_envelope() when it is done using
            // the fields in the envelope object.
			amqp_destroy_envelope(this);
		}
	};

	class Connection
	{
	public:
		Connection(
			const std::string &host = "127.0.0.1", int port = 5672,
			const std::string &username = "guest",
			const std::string &password = "guest",
			const std::string &vhost = "/", int frame_max = 131072);
		~Connection();

		amqp_socket_t *socket = NULL;
		amqp_connection_state_t conn;
		amqp_channel_t channel;

		void declareExchange(Exchange &exchange);
		
		void declareQueue(Queue &queue);
		
		void bindQueue(const std::string &queuename, const std::string &exchange,
			const std::string &routingkey);

		void basicPublish(const std::string &exchange, const std::string &routingkey,
			const Message &message, bool mandatory = false, bool immediate = false);

		void basicConsume(const std::string &queuename, const std::string &consumer_tag = "",
			bool no_local = true, bool no_ack = true, bool exclusive = true,
			uint16_t message_prefetch_count = 1);

		Envelope consumeMessage(int timeout = 0, int flags = 0);
	};
	
	void publish(const Configuration configration, const std::string &message);
	//void publish(Configuration configration, const std::string &messagebody, void (*callback)(const Response &response));
	//void subscribe(Configuration configration, void (*callback)(const Statement &statement));
	//void subscribe(Configuration configration, void (*callback)(const Request &request, Response &response));

	void publish(const std::string &exchange, const std::string &routingkey, const std::string &messagebody);
	void publish(const std::string &exchange, const std::string &routingkey, const std::string &messagebody, void (*callback)(const Response &response));
	void subscribe(const std::string &exchange, const std::string &bindingkey, void (*callback)(const Statement &statement));
	void subscribe(const std::string &exchange, const std::string &bindingkey, void (*callback)(const Request &request, Response &response));

private:
	std::string m_host;
	std::string m_username;
	std::string m_password;
	std::string m_vhost;
	int m_port;
	int m_frame_max;
};

} // end namespace gammasoft

#endif // __GAMMA__VISTA_FOUNDATION_SERVER__VISTA_MESSAGE_BROKER__CLASS__H__
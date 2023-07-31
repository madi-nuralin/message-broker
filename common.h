#ifndef  __GAMMA__VISTA_FOUNDATION_SERVER__VISTA_MESSAGE_BROKER__COMMON__H__
#define  __GAMMA__VISTA_FOUNDATION_SERVER__VISTA_MESSAGE_BROKER__COMMON__H__

#include <string>
#include <map>
#include <queue>
#include <mutex>
#include <memory>
#include <rabbitmq-c/amqp.h>

class Message : public amqp_message_t {
public:
	typedef std::shared_ptr<Message> Ptr;

	Message(const std::string &body) {
		this->properties._flags = 0;
		this->body = amqp_bytes_malloc_dup(amqp_cstring_bytes(body.c_str()));
	}

	~Message() {
		// Frees memory associated with a amqp_message_t
		// allocated in amqp_read_message
		amqp_destroy_message(this);
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

class Envelope : public amqp_envelope_t {
public:
	Envelope(const amqp_envelope_t &envelope) {
		// a shallow copy of amqp_envelope_t
		this->channel = envelope.channel;
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

class Connection {
public:
	Connection(
		const std::string &host = "127.0.0.1", int port = 5672,
		const std::string &username = "guest",
		const std::string &password = "guest",
		const std::string &vhost = "/", int frame_max = 131072);
	~Connection();

	amqp_socket_t *socket = NULL;
	amqp_connection_state_t state;
	std::map<amqp_channel_t, class Channel*> channels;
	std::mutex mutex;
};

class Channel {
public:
	Channel(Connection *connection);
	~Channel();

	std::string setup_queue(const std::string &queue_name, const std::string &exchange = "", const std::string &routing_key = "", bool passive = false, bool durable = false, bool auto_delete = false, bool exclusive = false);
	void publish(const std::string &exchange, const std::string &routing_key, const Message &message, bool mandatory = false, bool immediate = false);
	void consume(const std::string &queue_name, void (*callback)(const Envelope &envelope), const std::string &consumer_tag = "", bool no_local = false, bool no_ack = true, bool exclusive = false);
	int ack(uint64_t delivery_tag, bool multiple = false);
	int nack(uint64_t delivery_tag, bool multiple = false, bool requeue = false);

	amqp_channel_t id;
	Connection *connection;
	std::queue<amqp_envelope_t> envelopes;
};

#endif // __GAMMA__VISTA_FOUNDATION_SERVER__VISTA_MESSAGE_BROKER__COMMON__H__
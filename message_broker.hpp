#ifndef  MESSAGE_BROKER_H
#define  MESSAGE_BROKER_H

#include <map>
#include <queue>
#include <string>
#include <memory>
#include <mutex>
#include <atomic>
#include <utility>
#include <functional>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>

namespace gammasoft {

class MessageBroker
{
public:
	MessageBroker(
		const std::string &host = "127.0.0.1", int port = 5672,
		const std::string &username = "guest",
		const std::string &password = "guest",
		const std::string &vhost = "/", int frame_max = 131072);
	~MessageBroker();

	/**
	 * @brief Class for specifying the RabbitMQ queue and exchange
	 * parameters, like declaration, binding and etc.
	 * @note While making changes, ensure that the class
	 * must be an aggregate type that has:
	 *   -no user-declared constructors
	 *   -no user-provided, inherited, or explicit constructors
	 *   -no user-declared or inherited constructors
	 *   -no private or protected direct non-static data members no base classes
	 *   -no virtual base classes
	 *   -no private or protected direct base classes
	 *   -no virtual member functions
	 *   -no default member initializers
	 * in order to be compliant with "designated initialization".
	 */
	struct Configuration {
		struct {
			std::string name;
			std::string type;
			bool passive;
			bool durable;
			bool auto_delete;
			bool internal;
			bool declare;
		} exchange;
		struct {
			std::string name;
			bool passive;
			bool durable;
			bool auto_delete;
			bool exclusive;
			bool declare;
			bool bind;
		} queue;
		std::string routing_key;
	};

	class Message : public amqp_message_t {
	public:
		typedef std::shared_ptr<Message> Ptr;

		Message() {
			this->properties._flags = 0;
		}

		Message(const std::string &body) {
			this->properties._flags = 0;
			this->body = amqp_bytes_malloc_dup(amqp_cstring_bytes(body.c_str()));
		}

		~Message() {
			// Frees memory associated with a amqp_message_t
			// allocated in amqp_read_message
			// amqp_destroy_message(this);
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

		void setBody(const std::string & body) {
			this->body = amqp_bytes_malloc_dup(amqp_cstring_bytes(body.c_str()));
		}

		std::string getBody() const {
			return std::string((char *)this->body.bytes, (int)this->body.len);
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

	class Channel;
	class Connection {
	public:
		Connection(
			const std::string &host = "127.0.0.1", int port = 5672,
			const std::string &username = "guest",
			const std::string &password = "guest",
			const std::string &vhost = "/", int frame_max = 131072);
		~Connection();

		// Connection state object
		amqp_connection_state_t state;

		// The pool of consuming channels, id -> Channel(id) map
		std::map<amqp_channel_t, Channel*> pool;

		// Consumer event loop indication flag
		std::atomic<bool> run{true};

		// Mutex lock for synchronizing amqp_connection_state_t object
		std::mutex mt_lock;

	protected:
		// TCP socket object
		amqp_socket_t *socket = NULL;
	};

	class Channel {
	public:
		Channel(Connection *connection);
		~Channel();

		std::tuple<std::string, std::string> setup(const Configuration &configration);
		
		void publish(const std::string &exchange, const std::string &routing_key, const Message &message, bool mandatory = false, bool immediate = false);

		void consume(const std::string &queue_name, std::function<void(Channel &, const Envelope &)>, const std::string &consumer_tag = "", bool no_local = false, bool no_ack = true, bool exclusive = false);

		void cancel(const std::string &consumer_tag);

		void qos(uint32_t prefetch_size, uint16_t prefetch_count, bool global);

		void acknowledge(uint64_t delivery_tag, bool multiple = false);

		void reject(uint64_t delivery_tag, bool multiple = false, bool requeue = false);

		void push_envelope(const amqp_envelope_t& envelope) {
			m_envelope_queue.push(envelope);
		}

		amqp_envelope_t pop_envelope() {
			auto envelope = m_envelope_queue.front();
			m_envelope_queue.pop();
			return envelope;
		}

		bool empty_envelope() {
			return m_envelope_queue.empty();
		}

		// Channel id.
		amqp_channel_t id;

		// The binding amqp_connection_state_t object.
		Connection *connection;

	protected:
		bool receive = false;

		// Queue of consumed envelopes.
		std::queue<amqp_envelope_t> m_envelope_queue;
	};

	class Request : public Message {
		using Message::Message;
	};

	class Response : public Message {
		using Message::Message;
	public:
		bool ok() const {
			return std::string((char*)this->properties.type.bytes,
				this->properties.type.len) != "error";
		}
	};
	
	void publish(const Configuration configration, const std::string &messagebody);
	void publish(const Configuration configration, const std::string &messagebody, std::function<void (const Response&)> callback);
	void subscribe(const Configuration configration, std::function<void (const Message&)> callback);
	void subscribe(const Configuration configration, std::function<bool (const Request&, Response&)> callback);

private:
	/*std::string m_host;
	std::string m_username;
	std::string m_password;
	std::string m_vhost;
	int m_port;
	int m_frame_max;*/
	Connection *connection;
};

} // end namespace gammasoft

#endif // MESSAGE_BROKER_H
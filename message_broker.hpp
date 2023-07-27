#ifndef MESSAGE_BROKER_HPP
#define MESSAGE_BROKER_HPP

#include <map>
#include <string>
#include <memory>
#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

namespace gammasoft {

class VistaMessageBroker {
public:
    VistaMessageBroker(const std::string &host = "127.0.0.1", int port = 5672, const std::string &username = "guest", const std::string &password = "guest", const std::string &vhost = "/", int frame_max = 131072);
    ~VistaMessageBroker();

    class Statement,
    class Request,
    class Response;
    class Statement {
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
		Request() : Statement() {
			m_type = "request";
		}
		Request(const std::string &str) : Statement(str) {
			m_type = "request";
		}
		~Request();
	};

	class Response : public Statement
	{
	public:
		Response(const std::string &str);
		Response(const Request &request) : Statement() {
			m_reqid = request.reqid();
			m_type = "response";
		}
		~Response();

		std::string serialize() const override;
		void setReason(const std::string &reason);
		std::string reason() const { return m_reason; }
		bool ok() const { return m_type != "error"; }

	protected:
		std::string m_reason;

	private:
		Response();
	};

	void publish(amqp::exchange &exchange, amqp::queue &queue, const std::string &routingkey, const std::string &message);
	void publish(const std::string &exchange, const std::string &routingkey, const std::string &messagebody);
	void publish(amqp::exchange &exchange, amqp::queue &queue, const std::string &routingkey, const std::string &message, void (*callback)(const Response &response));
	void publish(const std::string &exchange, const std::string &routingkey, const std::string &messagebody, void (*callback)(const Response &response));
	void subscribe(amqp::exchange &exchange, amqp::queue &queue, const std::string &bindingkey, void (*callback)(const Statement &statement));
	void subscribe(const std::string &exchange, const std::string &bindingkey, void (*callback)(const Statement &statement));
	void subscribe(amqp::exchange &exchange, amqp::queue &queue, const std::string &bindingkey, void (*callback)(const Request &request, Response &response));
	void subscribe(const std::string &exchange, const std::string &bindingkey, void (*callback)(const Request &request, Response &response));

private:
	std::string m_host;
	std::string m_username;
	std::string m_password;
	std::string m_vhost;
	int m_port;
	int m_frame_max;
};

namespace amqp {
class envelope : public amqp_envelope_t {

};

class message : public amqp_message_t {
public:
	message(const std::string &body) {
		this->properties._flags = 0;
		this->body = amqp_bytes_malloc_dup(amqp_cstring_bytes(body.c_str()));
	}

	void property(const std::string & key, const char *value) {
		*reinterpret_cast<amqp_bytes_t*>(schema[key].ptr) = amqp_cstring_bytes(value);
		this->properties._flags |= schema[key].flag;
	}

	void property(const std::string & key, uint8_t value) {
		*reinterpret_cast<uint8_t*>(schema[key].ptr) = value;
		this->properties._flags |= schema[key].flag;
	}

	void property(const std::string & key, uint64_t value) {
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

struct exchange {
	exchange(const std::string &exchange_name, const std::string &exchange_type = "direct", bool passive = false, bool durable = false, bool auto_delete = false, bool internal = false)
		: name(exchange_name), type(exchange_type), passive(passive), durable(durable), auto_delete(auto_delete), internal(internal) {}

	std::string name, type;
	bool passive, durable, auto_delete, internal;
};

struct queue {
	queue(const std::string &queue_name = "", bool passive = false, bool durable = false, bool exclusive = false, bool auto_delete = true)
		: name(queue_name), passive(passive), durable(durable), exclusive(exclusive), auto_delete(auto_delete) {}

	std::string name;
	bool passive, durable, auto_delete, exclusive
};

class connection {
public:
	connection(const std::string &host = "127.0.0.1", int port = 5672, const std::string &username = "guest", const std::string &password = "guest", const std::string &vhost = "/", int frame_max = 131072);
	~connection();

	amqp_socket_t *socket = NULL;
	amqp_connection_state_t conn;

	void declare_exchange(amqp::exchange &exchange);
	void declare_queue(amqp::queue &queue);
	void bind_queue(const std::string &queuename, const std::string &exchange, const std::string &routingkey);
	void basic_publish(const std::string &exchange, const std::string &routingkey, const amq::message &message, bool mandatory = false, bool immediate = false);
	void basic_consume(const std::string &queuename, const std::string &consumer_tag = "", bool no_local = true, bool no_ack = true, bool exclusive = true, uint16_t message_prefetch_count = 1);
};
};
};

class MessageBroker
{
public:
	/**
	 * @brief      Constructs a new instance.
	 *
	 * @param[in]  host       The host
	 * @param[in]  port       The port
	 * @param[in]  username   The username
	 * @param[in]  password   The password
	 * @param[in]  vhost      The vhost
	 * @param[in]  frame_max  The frame maximum
	 */
	MessageBroker(const std::string &host = "127.0.0.1", int port = 5672,
                  const std::string &username = "guest",
                  const std::string &password = "guest",
                  const std::string &vhost = "/", int frame_max = 131072);

	/**
	 * @brief      Destroys the object.
	 */
	~MessageBroker();

	class Statement
	{
	public:
		Statement();
		Statement(const std::string& str);

		~Statement();

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
		Request() : Statement() {
			m_type = "request";
		}

		Request(const std::string &str) : Statement(str) {
			m_type = "request";
		}

		~Request() {}
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

	private:
		Response();
	};

	struct Exchange
	{
		Exchange(const std::string &exchange_name,
                 const std::string &exchange_type = "direct",
                 bool passive = false,
                 bool durable = false,
                 bool auto_delete = false,
                 bool internal = false)
		: name(exchange_name),
		  type(exchange_type),
		  passive(passive),
		  durable(durable),
		  auto_delete(auto_delete),
		  internal(internal) {}

		std::string name, type;
		bool passive, durable, auto_delete, internal;
	};

	struct Queue
	{
		Queue(const std::string &queue_name = "",
              bool passive = false,
              bool durable = false,
              bool exclusive = false,
              bool auto_delete = true)
		: name(queue_name),
		  passive(passive),
		  durable(durable),
		  exclusive(exclusive),
		  auto_delete(auto_delete) {}

		std::string name;
		bool passive, durable, auto_delete, exclusive;
	};

	struct Message : public amqp_message_t
	{
		typedef std::shared_ptr<Message> Ptr;

		Message(const std::string &body) {
			this->properties._flags = 0;
			this->body = amqp_bytes_malloc_dup(
				amqp_cstring_bytes(body.c_str())
			);
		}

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
	};

	struct Envelope : public amqp_envelope_t
	{
		typedef std::shared_ptr<Envelope> Ptr;
	};

	class Connection
	{
	public:
		/**
		 * @brief      Constructs a new instance.
		 *
		 * @param[in]  host       The host
		 * @param[in]  port       The port
		 * @param[in]  username   The username
		 * @param[in]  password   The password
		 * @param[in]  vhost      The vhost
		 * @param[in]  frame_max  The frame maximum
		 */
		Connection(const std::string &host = "127.0.0.1", int port = 5672,
                   const std::string &username = "guest",
                   const std::string &password = "guest",
                   const std::string &vhost = "/", int frame_max = 131072);

		/**
		 * @brief      Destroys the object.
		 */
		~Connection();

		amqp_socket_t *socket = NULL;
		amqp_connection_state_t conn;

		void declareExchange(Exchange &exchange);
		
		void declareQueue(Queue &queue);
		
		void bindQueue(const std::string &queuename,
                       const std::string &exchange,
                       const std::string &routingkey);

		void basicPublish(const std::string &exchange,
                          const std::string &routingkey,
                          const Message &message,
                          bool mandatory = false,
                          bool immediate = false);

		void basicConsume(const std::string &queuename,
                          const std::string &consumer_tag = "",
                          bool no_local = true, bool no_ack = true,
                          bool exclusive = true,
                          uint16_t message_prefetch_count = 1);
	};
	
	//void publish(Exchange &exchange, Queue &queue, const std::string &routingkey, const std::string &message);

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

#endif //MESSAGE_BROKER_HPP

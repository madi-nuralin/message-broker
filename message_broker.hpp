#ifndef  MESSAGE_BROKER_H
#define  MESSAGE_BROKER_H

#include <map>
#include <string>
#include <memory>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <json-glib/json-glib.h>
#include <json-glib/json-gobject.h>

#include "common.h"

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

	class Message
	{
	public:
		Message();
		Message(const std::string& str);
		virtual ~Message();

		std::string reqid() const { return m_reqid; }
		bool setBody(const JsonNode *json_node, std::string *error = nullptr);
		bool setBody(const std::string &body, std::string *error = nullptr);
		virtual std::string serialize() const;
		std::string serializeBody() const;

	protected:
		std::string m_reqid, m_type;
		JsonNode *m_body;
	};

	class Request : public Message
	{
	public:
		Request() : Message() {}
		Request(const std::string &str) : Message(str) {}

	protected:
		std::string m_type = "request";
	};

	class Response : public Message
	{
	public:
		Response(const std::string &str);
		Response(const Request &request) : Message() {
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
		} exchange;
		struct {
			std::string name;
			bool passive;
			bool durable;
			bool auto_delete;
			bool exclusive;
		} queue;
		bool queue_bind;
		std::string routing_key;
		std::string binding_key;
	};
	
	void publish(const Configuration configration, const std::string &messagebody);
	void publish(const Configuration configration, const std::string &messagebody, std::function<void (const Response&)> callback);
	void subscribe(const Configuration configration, std::function<void (const Message&)> callback);
	void subscribe(const Configuration configration, std::function<void (const Request&, Response&)> callback);

//	void publish(const std::string &exchange, const std::string &routingkey, const std::string &messagebody);
//	void publish(const std::string &exchange, const std::string &routingkey, const std::string &messagebody, void (*callback)(const Response &response));
//	void subscribe(const std::string &exchange, const std::string &bindingkey, void (*callback)(const Message &Message));
//	void subscribe(const std::string &exchange, const std::string &bindingkey, void (*callback)(const Request &request, Response &response));

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
#ifndef MESSAGE_BROKER_HPP
#define MESSAGE_BROKER_HPP

#include <memory>
#include <string>

#include <json-glib/json-glib.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>

class MessageBroker
{
	amqp_socket_t *socket;
	amqp_connection_state_t conn;

public:
	MessageBroker(const char* hostname, int port);
	virtual ~MessageBroker();

	class QueryInterface
	{
	public:
		typedef std::shared_ptr<QueryInterface> Ptr;

		static const char * const QUERY_REQUEST;
		static const char * const QUERY_RESPONSE;
		static const char * const QUERY_ERROR;

		QueryInterface();
		QueryInterface(const std::string& json_str);
		//virtual ~QueryInterface();

		std::string getType() const { return m_type; }
		JsonNode* getBody() const { return m_body; }
		bool setBody(const std::string& json_str);
		bool setBody(const JsonNode *node);
		bool setType(const std::string& type);
		std::string serialize() const;
		std::string serializeBody() const;

	protected:
		std::string m_reqid, m_type;
		JsonNode* m_body;
	};

	class Request : public QueryInterface {
	public:
		Request() : QueryInterface() {
			setType(QueryInterface::QUERY_REQUEST);
		}
		Request(const std::string& json_str) : QueryInterface(json_str) {}
	};

	class Response : public QueryInterface {
	public:
		Response() : QueryInterface() {
			setType(QueryInterface::QUERY_RESPONSE);
		}
		Response(const std::string& json_str) : QueryInterface(json_str) {}
	};

	class Queue
	{
		amqp_bytes_t queuename;
	public:
		Queue();
		~Queue();
	};

	MessageBroker::Response::Ptr send(
		const char *exchange,
		const char *routingkey,
		const char *message
	);

	void listen(
		const char *exchange,
		const char *bindingkey,
		bool (*callback)(const MessageBroker::Request &request, MessageBroker::Response &response)
	);
};

#endif //MESSAGE_BROKER_HPP

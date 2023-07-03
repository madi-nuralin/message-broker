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
	protected:
		int _id;
		std::string _type;
		JsonNode *_body;

	public:
		typedef std::shared_ptr<QueryInterface> Ptr;

		static const char * const QUERY_REQUEST;
		static const char * const QUERY_RESPONSE;
		static const char * const QUERY_ERROR;

	//public:
	//       QueryInterface(const char *json_str);
	//       virtual ~QueryInterface();

	//       int getId() const;
	//       char* getType() const;
	//       JsonNode* getBody() const;
	//       bool setBody(const char *json_str) const;
	//       bool setBody(const JsonNode *node) const;
	//       char* toJsonString() const;
	//       char* bodyToJsonString() const;
	    
	//protected:
	//       bool setType(const char *type);

		QueryInterface();
		QueryInterface(const char* json_str);
		//~QueryInterface();

		int id() const {return _id;}
		std::string type() const {return _type;}
		JsonNode* body() const {return _body;}
		bool set_type(const char* type);
		bool set_body(const char* json_str);
		bool set_body(const JsonNode* node);
		char* json_str() const;
		char* json_str_body() const;

	};

	class Request : public QueryInterface {
	public:
		Request() : QueryInterface() {}
		Request(const char* json_str) : QueryInterface(json_str) {
			_type = QueryInterface::QUERY_REQUEST;
		}
	};

	class Response : public QueryInterface {
	public:
		Response() : QueryInterface() {}
		Response(const char* json_str) : QueryInterface(json_str) {
			_type = QueryInterface::QUERY_RESPONSE;
		}
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
		const char *query
	);

	int listen(
		const char *exchange,
		const char *bindingkey,
		bool (*callback)(const MessageBroker::Request &request, MessageBroker::Response &response)
	);
};

#endif //MESSAGE_BROKER_HPP

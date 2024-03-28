#include "message_broker.hpp"
#include "utils.h"
#include <assert.h>
#include <chrono>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <set>
#include <stdexcept>
#include <stdlib.h>
#include <string.h>
#include <thread>

namespace soft {

namespace amqp {

static inline std::string&
btos(const amqp_bytes_t& x)
{
  return std::string(x.bytes, x.len);
}

static inline AmqpProperties
ptop(const amqp_basic_properties_t& props)
{
  AmqpProperties properties;
  if (props._flags & AMQP_BASIC_CONTENT_TYPE_FLAG)
    properties.content_type = btos(props.content_type);
  if (props._flags & AMQP_BASIC_CONTENT_ENCODING_FLAG)
    properties.content_encoding = btos(props.content_encoding);
  if (props._flags & AMQP_BASIC_DELIVERY_MODE_FLAG)
    properties.delivery_mode = props.delivery_mode;
  if (props._flags & AMQP_BASIC_PRIORITY_FLAG)
    properties.priority = props.priority;
  if (props._flags & AMQP_BASIC_CORRELATION_ID_FLAG)
    properties.correlation_id = btos(props.correlation_id);
  if (props._flags & AMQP_BASIC_REPLY_TO_FLAG)
    properties.reply_to = btos(props.reply_to);
  if (props._flags & AMQP_BASIC_EXPIRATION_FLAG)
    properties.expiration = btos(props.expiration);
  if (props._flags & AMQP_BASIC_MESSAGE_ID_FLAG)
    properties.message_id = btos(props.message_id);
  if (props._flags & AMQP_BASIC_TIMESTAMP_FLAG)
    properties.timestamp = props.timestamp;
  if (props._flags & AMQP_BASIC_USER_ID_FLAG)
    properties.user_id = btos(props.user_id);
  if (props._flags & AMQP_BASIC_APP_ID_FLAG)
    properties.app_id = btos(props.app_id);
  if (props._flags & AMQP_BASIC_CLUSTER_ID_FLAG)
    properties.cluster_id = btos(props.cluster_id);
  return properties;
}

static inline amqp_basic_properties_t
ptop(const AmqpProperties& properties)
{
  amqp_basic_properties_t props;
  props._flags = 0;
  if (properties.content_type.has_value()) {
    props._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
    props.content_type =
      amqp_cstring_bytes(properties.content_type.value().c_str());
  }
  if (properties.content_encoding.has_value()) {
    props._flags |= AMQP_BASIC_CONTENT_ENCODING_FLAG;
    props.content_encoding =
      amqp_cstring_bytes(properties.content_encoding.value().c_str());
  }
  if (properties.delivery_mode.has_value()) {
    props._flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.delivery_mode = properties.delivery_mode.value();
  }
  if (properties.priority.has_value()) {
    props._flags |= AMQP_BASIC_PRIORITY_FLAG;
    props.priority = properties.priority.value();
  }
  if (properties.correlation_id.has_value()) {
    props._flags |= AMQP_BASIC_CORRELATION_ID_FLAG;
    props.correlation_id =
      amqp_cstring_bytes(properties.correlation_id.value().c_str());
  }
  if (properties.reply_to.has_value()) {
    props._flags |= AMQP_BASIC_REPLY_TO_FLAG;
    props.reply_to = amqp_cstring_bytes(properties.reply_to.value().c_str());
  }
  if (properties.expiration.has_value()) {
    props._flags |= AMQP_BASIC_EXPIRATION_FLAG;
    props.expiration =
      amqp_cstring_bytes(properties.expiration.value().c_str());
  }
  if (properties.message_id.has_value()) {
    props._flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
    props.message_id =
      amqp_cstring_bytes(properties.message_id.value().c_str());
  }
  if (properties.timestamp.has_value()) {
    props._flags |= AMQP_BASIC_TIMESTAMP_FLAG;
    props.timestamp = properties.timestamp.value();
  }
  if (properties.type.has_value()) {
    props._flags |= AMQP_BASIC_TYPE_FLAG;
    props.type = amqp_cstring_bytes(properties.type.value().c_str());
  }
  if (properties.user_id.has_value()) {
    props._flags |= AMQP_BASIC_USER_ID_FLAG;
    props.user_id = amqp_cstring_bytes(properties.user_id.value().c_str());
  }
  if (properties.app_id.has_value()) {
    props._flags |= AMQP_BASIC_APP_ID_FLAG;
    props.app_id = amqp_cstring_bytes(properties.app_id.value().c_str());
  }
  if (properties.cluster_id.has_value()) {
    props._flags |= AMQP_BASIC_CLUSTER_ID_FLAG;
    props.cluster_id =
      amqp_cstring_bytes(properties.cluster_id.value().c_str());
  }
  return props;
}

static bool
checkConsumeMessageLibErr(int library_error,
                          amqp_connection_state_t& connection,
                          std::string& errorText)
{
  switch (library_error) {
    case AMQP_STATUS_OK:
    case AMQP_STATUS_TIMEOUT:
    case _AMQP_STATUS_NEXT_VALUE:
    case _AMQP_STATUS_TCP_NEXT_VALUE:
    case _AMQP_STATUS_SSL_NEXT_VALUE:
      return true;
    case AMQP_STATUS_UNEXPECTED_STATE: {
      amqp_frame_t frame;
      auto result = amqp_simple_wait_frame(connection.get_state(), &frame);
      // getLogger()->warn("wait frame res: code: {} error text: {}",
      //                   result,
      //                   amqp_error_string2(result));
    }
      return true;
    default: {
      errorText = amqp_error_string2(library_error);
      // getLogger()->error("AMQP consume message error: code: {} error text:
      // {}",
      //                    library_error,
      //                    errorText);
    }
      return false;
  }
}

class AmqpMessageImpl : public AmqpMessage
{
public:
  AmqpMessageImpl() {}
  virtual ~AmqpMessageImpl() {}

  std::string& body() noexcept { return m_body; }

  void body(const std::string& b) noexcept { m_body = b; }

  AmqpProperties& properties() noexcept { return m_properties; }

  void properties(const AmqpProperties& p) noexcept { m_properties = p; }

private:
  std::string m_body;
  AmqpProperties m_properties;
};

AmqpMessage::AmqpMessage() {}

AmqpMessage::~AmqpMessage() {}

AmqpMessage::Ptr
AmqpMessage::createInstance()
{
  return AmqpMessage::Ptr(new AmqpMessageImpl());
}

class AmqpEnvelopeImpl : public AmqpEnvelope
{
public:
  AmqpEnvelopeImpl() {}
  virtual ~AmqpEnvelopeImpl() {}

  virtual AmqpMessage::Ptr message() const noexcept { return m_message; }

  virtual std::string consumerTag() const noexcept { return m_consumerTag; }

  virtual std::uint64_t deliveryTag() const noexcept { return m_deliveryTag; }

  virtual std::string exchange() const noexcept { return m_exchange; }

  virtual bool redelivered() const noexcept { return m_redelivered; }

  virtual std::string routingKey() const noexcept { return m_routingKey; }

private:
  const AmqpMessage::Ptr m_message;
  const std::string m_consumerTag;
  const std::uint64_t m_deliveryTag;
  const std::string m_exchange;
  const bool m_redelivered;
  const std::string m_routingKey;
};

AmqpEnvelope::AmqpEnvelope() {}

AmqpEnvelope::~AmqpEnvelope() {}

AmqpEnvelope::Ptr
AmqpEnvelope::createInstance()
{
  return AmqpEnvelope::Ptr(new AmqpEnvelopeImpl());
}

class AmqpConnectionImpl : public AmqpConnection
{
public:
  using Ptr = std::shared_ptr<AmqpConnectionImpl>;

  AmqpConnectionImpl()
    : m_state(amqp_new_connection())
  {
  }

  virtual ~AmqpConnectionImpl()
  {
    if (m_state) {
      die_on_amqp_error(amqp_connection_close(m_state, AMQP_REPLY_SUCCESS),
                        "Closing connection");
      die_on_error(amqp_destroy_connection(m_state), "Ending connection");
    }
  }

  void open(const std::string& host, int port)
  {
    amqp_socket_t* socket = amqp_tcp_socket_new(m_state);
    if (!socket) {
      die("AMQP creating TCP socket failed");
    }
    int status = amqp_socket_open(socket, host.c_str(), port);
    if (status) {
      die("AMQP opening TCP socket on %s:%d failed", host.c_str(), port);
    }
  }

  void login(const std::string& vhost,
             const std::string& username,
             const std::string& password,
             int frame_max) const
  {
    die_on_amqp_error(amqp_login(m_state,
                                 vhost.c_str(),
                                 0,
                                 frame_max,
                                 0,
                                 AMQP_SASL_METHOD_PLAIN,
                                 username.c_str(),
                                 password.c_str()),
                      "Logging in");
  }

  amqp_connection_state_t& state() noexcept { return m_state; }

private:
  amqp_connection_state_t m_state;
};

AmqpConnection::AmqpConnection() {}

AmqpConnection::~AmqpConnection() {}

AmqpConnection::Ptr
AmqpConnection::createInstance()
{
  return AmqpConnection::Ptr(new AmqpConnectionImpl());
}

const char* AmqpChannel::EXCHANGE_TYPE_DIRECT = "direct";

const char* AmqpChannel::EXCHANGE_TYPE_FANOUT = "fanout";

const char* AmqpChannel::EXCHANGE_TYPE_TOPIC = "topic";

class AmqpChannelImpl : public AmqpChannel
{
public:
  AmqpChannelImpl(AmqpConnection::Ptr connection)
  {
    AmqpConnectionImpl::Ptr impl =
      std::dynamic_pointer_cast<AmqpConnectionImpl>(connection);
    m_state = impl->state();
    m_channel = 1;

    amqp_channel_open(m_state, m_channel);
    die_on_amqp_error(amqp_get_rpc_reply(m_state), "Opening channel");
  }

  virtual ~AmqpChannelImpl()
  {
    die_on_amqp_error(
      amqp_channel_close(m_state, m_channel, AMQP_REPLY_SUCCESS),
      "Closing connection");
  }

  void exchangeDeclare(const std::string& exchange_name,
                       const std::string& exchange_type,
                       bool passive,
                       bool durable,
                       bool auto_delete,
                       bool internal)
  {
    amqp_exchange_declare(m_state,
                          m_channel,
                          amqp_cstring_bytes(exchange_name.c_str()),
                          amqp_cstring_bytes(exchange_type.c_str()),
                          passive,
                          durable,
                          auto_delete,
                          internal,
                          amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(m_state), "exchange.declare");
  }

  void exchangeBind(const std::string& destination,
                    const std::string& source,
                    const std::string& routing_key)
  {
  }

  void exchangeUnbind(const std::string& destination,
                      const std::string& source,
                      const std::string& routing_key)
  {
  }

  std::string queueDeclare(const std::string& queue_name,
                           bool passive,
                           bool durable,
                           bool exclusive,
                           bool auto_delete)
  {
    amqp_queue_declare_ok_t* r =
      amqp_queue_declare(m_state,
                         m_channel,
                         amqp_cstring_bytes(queue_name.c_str()),
                         passive,
                         durable,
                         exclusive,
                         auto_delete,
                         amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(m_state), "queue.declare");
    return std::string((char*)r->queue.bytes, r->queue.len);
  }

  void queueBind(const std::string& queue_name,
                 const std::string& exchange_name,
                 const std::string& routing_key)
  {
    amqp_queue_bind(m_state,
                    m_channel,
                    amqp_cstring_bytes(queue_name.c_str()),
                    amqp_cstring_bytes(exchange_name.c_str()),
                    amqp_cstring_bytes(routing_key.c_str()),
                    amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(m_state), "queue.bind");
  }

  void queueUnbind(const std::string& queue_name,
                   const std::string& exchange_name,
                   const std::string& routing_key = "")
  {
  }

  void basicPublish(const std::string& exchange,
                    const std::string& routing_key,
                    const AmqpMessage::Ptr message,
                    bool mandatory,
                    bool immediate)
  {
    amqp_basic_properties_t props = ptop(message->properties());
    die_on_error(
      amqp_basic_publish(m_state,
                         m_channel,
                         amqp_cstring_bytes(exchange.c_str()),
                         amqp_cstring_bytes(routing_key.c_str()),
                         mandatory,
                         immediate,
                         &props,
                         amqp_cstring_bytes(message->body().c_str())),
      "basic.publish");
  }

  void basicConsume(const std::string& queue_name,
                    const std::string& consumer_tag,
                    bool no_local,
                    bool no_ack,
                    bool exclusive)
  {
    amqp_basic_consume(m_state,
                       m_channel,
                       amqp_cstring_bytes(queue_name.c_str()),
                       amqp_cstring_bytes(consumer_tag.c_str()),
                       no_local,
                       no_ack,
                       exclusive,
                       amqp_empty_table);
    die_on_amqp_error(amqp_get_rpc_reply(m_state), "basic.consume");
  }

  void basicCancel(const std::string& consumer_tag)
  {
    if (!consumer_tag.empty()) {
      amqp_basic_cancel(
        m_state, m_channel, amqp_cstring_bytes(consumer_tag.c_str()));
      die_on_amqp_error(amqp_get_rpc_reply(m_state), "basic.cancel");
    }
  }

  void basicQos(uint32_t prefetch_size, uint16_t prefetch_count, bool global)
  {
    if (!amqp_basic_qos(
          m_state, m_channel, prefetch_count, prefetch_size, global)) {
      die_on_amqp_error(amqp_get_rpc_reply(m_state), "basic.qos");
    }
  }

  void basicAck(uint64_t delivery_tag, bool multiple)
  {
    die_on_error(amqp_basic_ack(m_state, m_channel, delivery_tag, multiple),
                 "basic.ack");
  }

  void basicNack(uint64_t delivery_tag, bool multiple, bool requeue)
  {
    die_on_error(
      amqp_basic_nack(m_state, m_channel, delivery_tag, multiple, requeue),
      "basic.nack");
  }

  AmqpEnvelope::Ptr basicConsumeMessage(const struct timeval* timeout)
  {
    amqp_rpc_reply_t res;
    amqp_envelope_t envelope;

    amqp_maybe_release_buffers(m_state);
    res = amqp_consume_message(m_state, &envelope, &timeout, 0);
    std::string errorText{ "unknown" };

    if (AMQP_RESPONSE_NORMAL != res.reply_type) {
      if (!checkConsumeMessageLibErr(res.library_error, m_state, errorText))
        throw std::runtime_error(errorText);
      continue;
    }

    auto msg = AmqpMessage::createInstance();
    msg->body(btos(envelope.message.body));
    msg->properties(ptop(envelope.message.properties));

    auto env = AmqpEnvelope::createInstance();
    env->message(msg);
    env->deliveryTag(envelope.delivery_tag);
    env->redelivered(envelope.redelivered);
    env->exchange(btos(envelope.exchange));
    env->consumerTag(btos(envelope.consumer_tag));
    env->routingKey(btos(envelope.routing_key));

    amqp_destroy_envelope(envelope);
    return env;
  }

private:
  amqp_connection_state_t m_state;
  amqp_channel_t m_channel;
};

AmqpChannel::AmqpChannel() {}

AmqpChannel::~AmqpChannel() {}

AmqpChannel::Ptr
AmqpChannel::createInstance(AmqpConnection::Ptr connection)
{
  return AmqpChannel::Ptr(new AmqpChannelImpl(connection));
}

using namespace soft::amqp;

const char ALPHABET[] = { "0123456789"
                          "abcdefgjhiklmnopqrstvwxyz"
                          "ABCDEFGJHIKLMNOPQRSTVWXYZ" };

static std::string
generateReqId()
{
  std::string result;
  for (std::size_t i = 0; i < 16; i++) {
    result += ALPHABET[rand() % (sizeof(ALPHABET) - 1)];
  }
  return result;
}

MessageBroker::MessageBroker(const std::string& host,
                             int port,
                             const std::string& username,
                             const std::string& password,
                             const std::string& vhost,
                             int frame_max)
{
  std::srand(std::time(NULL));

  if (host.empty()) {
    throw std::runtime_error("host is not specified, it is required");
  }
  if (vhost.empty()) {
    throw std::runtime_error("vhost is not specified, it is required");
  }
  if (port <= 0) {
    throw std::runtime_error(
      "port is not valm_id, it must be a positive number");
  }

  m_host = host;
  m_port = port;
  m_username = username;
  m_password = password;
  m_vhost = vhost;
  m_frame_max = frame_max;
}

MessageBroker::MessageBroker(const std::string& url, int frame_max)
{
  std::srand(std::time(NULL));

  struct amqp_connection_info ci;
  char* p = strdup(url.c_str());

  die_on_error(amqp_parse_url(p, &ci), "Parsing URL");

  m_host = ci.host;
  m_port = ci.port;
  m_username = ci.user;
  m_password = ci.password;
  m_vhost = ci.vhost;
  m_frame_max = frame_max;

  free(p);
}

MessageBroker::~MessageBroker()
{
  m_close = true;
  for (auto it = m_threads.begin(); it != m_threads.end(); it++) {
    it->join();
  }
}

void
MessageBroker::publish(const Configuration& cfg, const std::string& messagebody)
{
  auto conn = AmqpConnection::createInstance();
  conn->open(m_host, m_port);
  conn->login(m_vhost, m_username, m_password, m_frame_max);

  auto channel = AmqpChannel::createInstance(conn);
  auto [exchange, queue] = setup(channel, cfg);

  auto msg = Message::createInstance();
  msg->body(messagebody);
  msg->properties(cfg.properties);

  if (!msg->properties().content_type.has_value())
    msg->properties().content_type = "application/json";
  if (!msg->properties().delivery_mode.has_value())
    msg->properties().delivery_mode = 2u;
  if (!msg->properties().message_id.has_value())
    msg->properties().message_id = generateReqId();

  channel->basicPublish(exchange, cfg.routing_key, msg);
}

MessageBroker::Response::Ptr
MessageBroker::publish(const Configuration& cfg,
                       const std::string& messagebody,
                       struct timeval* timeout)
{
  auto conn = AmqpConnection::createInstance();
  conn->open(m_host, m_port);
  conn->login(m_vhost, m_username, m_password, m_frame_max);

  auto channel = AmqpChannel::createInstance(conn);
  auto [exchange, reply_to] = setup(channel, cfg);

  auto req = Request::createInstance();
  req->body(messagebody);
  req->properties(cfg.properties);

  if (!req->properties().content_type.has_value())
    req->properties().content_type = "application/json";
  if (!req->properties().delivery_mode.has_value())
    req->properties().delivery_mode = 2u;
  if (!req->properties().message_id.has_value())
    req->properties().message_id = generateReqId();
  if (!req->properties().correlation_id.has_value())
    req->properties().correlation_id = generateReqId();
  if (!req->properties().type.has_value())
    req->properties().type = "request";

  channel->basicPublish(exchange, cfg.routing_key, req);
  channel->basicConsume(reply_to);

  struct timeval tv = { 30, 0 };
  Response::Ptr response;

  for (;;) {
    auto envelope = channel->basicConsumeMessage(timeout ? timeout : &tv);
    response = std::make_shared<Response>(envelope->message);
    break;
  }

  return response;
}

void
MessageBroker::subscribe(const Configuration& cfg,
                         std::function<void(const Message::Ptr)> callback)
{
  std::thread worker([this, cfg, callback]() {
    struct timeval tv = { 1, 0 };
    auto conn = AmqpConnection::createInstance();
    conn->open(m_host, m_port);
    conn->login(m_vhost, m_username, m_password, m_frame_max);

    auto channel = AmqpChannel::createInstance(conn);
    auto [exchange, queue] = setup(channel, cfg);
    channel->basicConsume(queue);

    while (!m_close) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      auto envelope = channel->basicConsumeMessage(&tv);
      callback(envelope->message);
    }
  });

  m_threads.push_back(std::move(worker));
}

void
MessageBroker::subscribe(
  const Configuration& cfg,
  std::function<bool(const Request::Ptr, Response::Ptr)> callback)
{
  std::thread worker([this, cfg, callback]() {
    struct timeval tv = { 1, 0 };
    auto conn = AmqpConnection::createInstance();
    conn->open(m_host, m_port);
    conn->login(m_vhost, m_username, m_password, m_frame_max);

    auto channel = AmqpChannel::createInstance(conn);
    auto [exchange, queue] = setup(channel, cfg);
    channel->basicConsume(queue);

    while (!m_close) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      auto envelope = channel->basicConsumeMessage(&tv);

      auto request = Request::Ptr(envelope->message);
      Response::Ptr response;

      auto ok = callback(request, response);

      std::string reply_to(envelope.message.properties.reply_to);
      std::string correlation_id(envelope.message.properties.correlation_id);

      if (!req->properties().content_type.has_value())
        req->properties().content_type = "application/json";
      if (!req->properties().delivery_mode.has_value())
        req->properties().delivery_mode = 2u;
      if (!req->properties().message_id.has_value())
        req->properties().message_id = generateReqId();
      if (!req->properties().correlation_id.has_value())
        req->properties().correlation_id = correlation_id;
      if (!req->properties().type.has_value())
        req->properties().type = ok ? "response" : "error";

      channel->basicPublish("", reply_to, response);
    }
  });

  m_threads.push_back(std::move(worker));
}

void
MessageBroker::close()
{
  m_close = true;
}

std::tuple<std::string, std::string>
MessageBroker::setup(AmqpChannel::Ptr channel, const Configuration& cfg)
{
  std::string exchange, queue;

  if (cfg.exchange.name == "amq") {
    exchange = "amq." + cfg.exchange.type;
  }

  if (cfg.exchange.declare) {
    channel->exchangeDeclare(cfg.exchange.name,
                             cfg.exchange.type,
                             cfg.exchange.passive,
                             cfg.exchange.durable,
                             cfg.exchange.auto_delete,
                             cfg.exchange.internal);

    exchange = cfg.exchange.name;
  }

  if (cfg.queue.declare) {
    queue = channel->queueDeclare(cfg.queue.name,
                                  cfg.queue.passive,
                                  cfg.queue.durable,
                                  cfg.queue.exclusive,
                                  cfg.queue.auto_delete);
  }

  if (cfg.queue.bind) {
    channel->queueBind(
      queue,
      exchange,
      cfg.routing_key); /// @todo replace with routing_pattern field
  }

  return std::make_tuple(exchange, queue);
}

} // end namespace soft

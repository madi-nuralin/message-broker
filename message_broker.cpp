#include "message_broker.hpp"
#include <assert.h>
#include <atomic>
#include <chrono>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <set>
#include <stdexcept>
#include <stdlib.h>
#include <string.h>
#include <thread>
#include <vector>

#include "utils.h"

namespace gs {

namespace amqp {

static inline std::string
bytesToString(const amqp_bytes_t& x)
{
  return std::string((char*)x.bytes, x.len);
}

static inline AmqpProperties
getMessageProperties(const amqp_basic_properties_t& props)
{
  AmqpProperties properties;
  if (props._flags & AMQP_BASIC_CONTENT_TYPE_FLAG)
    properties.content_type = bytesToString(props.content_type);
  if (props._flags & AMQP_BASIC_CONTENT_ENCODING_FLAG)
    properties.content_encoding = bytesToString(props.content_encoding);
  if (props._flags & AMQP_BASIC_DELIVERY_MODE_FLAG)
    properties.delivery_mode = props.delivery_mode;
  if (props._flags & AMQP_BASIC_PRIORITY_FLAG)
    properties.priority = props.priority;
  if (props._flags & AMQP_BASIC_CORRELATION_ID_FLAG)
    properties.correlation_id = bytesToString(props.correlation_id);
  if (props._flags & AMQP_BASIC_REPLY_TO_FLAG)
    properties.reply_to = bytesToString(props.reply_to);
  if (props._flags & AMQP_BASIC_EXPIRATION_FLAG)
    properties.expiration = bytesToString(props.expiration);
  if (props._flags & AMQP_BASIC_MESSAGE_ID_FLAG)
    properties.message_id = bytesToString(props.message_id);
  if (props._flags & AMQP_BASIC_TIMESTAMP_FLAG)
    properties.timestamp = props.timestamp;
  if (props._flags & AMQP_BASIC_USER_ID_FLAG)
    properties.user_id = bytesToString(props.user_id);
  if (props._flags & AMQP_BASIC_APP_ID_FLAG)
    properties.app_id = bytesToString(props.app_id);
  if (props._flags & AMQP_BASIC_CLUSTER_ID_FLAG)
    properties.cluster_id = bytesToString(props.cluster_id);
  return properties;
}

static inline amqp_basic_properties_t
getMessageProperties(const AmqpProperties& properties)
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
      auto result = amqp_simple_wait_frame(connection, &frame);
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

struct AmqpMessage::Impl
{
  std::string body;
  AmqpProperties properties;
};

AmqpMessage::AmqpMessage()
  : m_impl(new Impl)
{
}

AmqpMessage::~AmqpMessage() {}

const std::string&
AmqpMessage::body() const noexcept
{
  return m_impl->body;
}

std::string&
AmqpMessage::body() noexcept
{
  return m_impl->body;
}

const AmqpProperties&
AmqpMessage::properties() const noexcept
{
  return m_impl->properties;
}

AmqpProperties&
AmqpMessage::properties() noexcept
{
  return m_impl->properties;
}

AmqpEnvelope::AmqpEnvelope(const AmqpMessage::Ptr message,
                           const std::string& consumer_tag,
                           const std::uint64_t delivery_tag,
                           const std::string& exchange,
                           bool redelivered,
                           const std::string& routing_key)
  : m_message(message)
  , m_consumerTag(consumer_tag)
  , m_deliveryTag(delivery_tag)
  , m_exchange(exchange)
  , m_redelivered(redelivered)
  , m_routingKey(routing_key)
{
}

AmqpEnvelope::~AmqpEnvelope() {}

struct AmqpConnection::Impl
{
  amqp_connection_state_t state;
};

AmqpConnection::AmqpConnection()
  : m_impl(new Impl)
{
  m_impl->state = amqp_new_connection();
}

AmqpConnection::~AmqpConnection()
{
  if (m_impl->state) {
    die_on_amqp_error(amqp_connection_close(m_impl->state, AMQP_REPLY_SUCCESS),
                      "connection.close");
    die_on_error(amqp_destroy_connection(m_impl->state), "connection.destroy");
  }
}

void
AmqpConnection::open(const std::string& host, int port)
{
  amqp_socket_t* socket = amqp_tcp_socket_new(m_impl->state);
  if (!socket) {
    die("AMQP creating TCP socket failed");
  }
  int status = amqp_socket_open(socket, host.c_str(), port);
  if (status) {
    die("AMQP opening TCP socket on %s:%d failed", host.c_str(), port);
  }
}

void
AmqpConnection::login(const std::string& vhost,
                      const std::string& username,
                      const std::string& password,
                      int frame_max) const
{
  die_on_amqp_error(amqp_login(m_impl->state,
                               vhost.c_str(),
                               0,
                               frame_max,
                               0,
                               AMQP_SASL_METHOD_PLAIN,
                               username.c_str(),
                               password.c_str()),
                    "Logging in");
}

struct AmqpChannel::Impl
{
  amqp_connection_state_t state;
  amqp_channel_t channel;
};

const char* AmqpChannel::EXCHANGE_TYPE_DIRECT = "direct";

const char* AmqpChannel::EXCHANGE_TYPE_FANOUT = "fanout";

const char* AmqpChannel::EXCHANGE_TYPE_TOPIC = "topic";

AmqpChannel::AmqpChannel(const AmqpConnection::Ptr conn)
  : m_impl(new Impl)
{
  m_impl->state = conn->m_impl->state;
  // m_impl->state = *state_map[channel_pool[m_impl->channel]];

  m_impl->channel = 1;

  amqp_channel_open(m_impl->state, m_impl->channel);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "channel.open");
}

AmqpChannel::~AmqpChannel()
{
  die_on_amqp_error(
    amqp_channel_close(m_impl->state, m_impl->channel, AMQP_REPLY_SUCCESS),
    "channel.close");
}

void
AmqpChannel::exchangeDeclare(const std::string& exchange_name,
                             const std::string& exchange_type,
                             bool passive,
                             bool durable,
                             bool auto_delete,
                             bool internal)
{
  amqp_exchange_declare(m_impl->state,
                        m_impl->channel,
                        amqp_cstring_bytes(exchange_name.c_str()),
                        amqp_cstring_bytes(exchange_type.c_str()),
                        passive,
                        durable,
                        auto_delete,
                        internal,
                        amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "exchange.declare");
}

void
AmqpChannel::exchangeBind(const std::string& destination,
                          const std::string& source,
                          const std::string& routing_key)
{
}

void
AmqpChannel::exchangeUnbind(const std::string& destination,
                            const std::string& source,
                            const std::string& routing_key)
{
}

std::string
AmqpChannel::queueDeclare(const std::string& queue_name,
                          bool passive,
                          bool durable,
                          bool exclusive,
                          bool auto_delete)
{
  amqp_queue_declare_ok_t* r =
    amqp_queue_declare(m_impl->state,
                       m_impl->channel,
                       amqp_cstring_bytes(queue_name.c_str()),
                       passive,
                       durable,
                       exclusive,
                       auto_delete,
                       amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "queue.declare");
  return std::string((char*)r->queue.bytes, r->queue.len);
}

void
AmqpChannel::queueBind(const std::string& queue_name,
                       const std::string& exchange_name,
                       const std::string& routing_key)
{
  amqp_queue_bind(m_impl->state,
                  m_impl->channel,
                  amqp_cstring_bytes(queue_name.c_str()),
                  amqp_cstring_bytes(exchange_name.c_str()),
                  amqp_cstring_bytes(routing_key.c_str()),
                  amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "queue.bind");
}

void
AmqpChannel::queueUnbind(const std::string& queue_name,
                         const std::string& exchange_name,
                         const std::string& routing_key)
{
}

void
AmqpChannel::basicPublish(const std::string& exchange,
                          const std::string& routing_key,
                          const AmqpMessage::Ptr message,
                          bool mandatory,
                          bool immediate)
{
  amqp_basic_properties_t props = getMessageProperties(message->properties());
  die_on_error(amqp_basic_publish(m_impl->state,
                                  m_impl->channel,
                                  amqp_cstring_bytes(exchange.c_str()),
                                  amqp_cstring_bytes(routing_key.c_str()),
                                  mandatory,
                                  immediate,
                                  &props,
                                  amqp_cstring_bytes(message->body().c_str())),
               "basic.publish");
}

std::string
AmqpChannel::basicConsume(const std::string& queue_name,
                          const std::string& consumer_tag,
                          bool no_local,
                          bool no_ack,
                          bool exclusive)
{
  amqp_basic_consume_ok_t* r =
    amqp_basic_consume(m_impl->state,
                       m_impl->channel,
                       amqp_cstring_bytes(queue_name.c_str()),
                       amqp_cstring_bytes(consumer_tag.c_str()),
                       no_local,
                       no_ack,
                       exclusive,
                       amqp_empty_table);
  die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "basic.consume");
  return std::string((char*)r->consumer_tag.bytes, r->consumer_tag.len);
}

void
AmqpChannel::basicCancel(const std::string& consumer_tag)
{
  if (!consumer_tag.empty()) {
    amqp_basic_cancel(
      m_impl->state, m_impl->channel, amqp_cstring_bytes(consumer_tag.c_str()));
    die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "basic.cancel");
  }
}

void
AmqpChannel::basicQos(uint32_t prefetch_size,
                      uint16_t prefetch_count,
                      bool global)
{
  if (!amqp_basic_qos(m_impl->state,
                      m_impl->channel,
                      prefetch_count,
                      prefetch_size,
                      global)) {
    die_on_amqp_error(amqp_get_rpc_reply(m_impl->state), "basic.qos");
  }
}

void
AmqpChannel::basicAck(uint64_t delivery_tag, bool multiple)
{
  die_on_error(
    amqp_basic_ack(m_impl->state, m_impl->channel, delivery_tag, multiple),
    "basic.ack");
}

void
AmqpChannel::basicNack(uint64_t delivery_tag, bool multiple, bool requeue)
{
  die_on_error(
    amqp_basic_nack(
      m_impl->state, m_impl->channel, delivery_tag, multiple, requeue),
    "basic.nack");
}

AmqpEnvelope::Ptr
AmqpChannel::basicConsumeMessage(const struct timeval* timeout)
{
  amqp_rpc_reply_t res;
  amqp_envelope_t envelope;

  amqp_maybe_release_buffers(m_impl->state);
  res = amqp_consume_message(m_impl->state, &envelope, timeout, 0);
  std::string errorText{ "unknown" };

  if (AMQP_RESPONSE_NORMAL != res.reply_type) {
    if (!checkConsumeMessageLibErr(res.library_error, m_impl->state, errorText))
      throw std::runtime_error(errorText);
    return nullptr;
  }

  auto message = AmqpMessage::createInstance();
  message->body() = bytesToString(envelope.message.body);
  message->properties() = getMessageProperties(envelope.message.properties);

  auto env = AmqpEnvelope::createInstance(message,
                                          bytesToString(envelope.consumer_tag),
                                          envelope.delivery_tag,
                                          bytesToString(envelope.exchange),
                                          envelope.redelivered,
                                          bytesToString(envelope.routing_key));

  amqp_destroy_envelope(&envelope);
  return env;
}

} // end namespace amqp

using namespace gs::amqp;

struct MessageBroker::Impl
{
  std::string host;
  int port;
  std::string username;
  std::string password;
  std::string vhost;
  int frame_max;
  std::vector<std::thread> threads;
  std::atomic<bool> close{ false };
};

MessageBroker::MessageBroker(const std::string& host,
                             int port,
                             const std::string& username,
                             const std::string& password,
                             const std::string& vhost,
                             int frame_max)
  : m_impl(new Impl)
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

  m_impl->host = host;
  m_impl->port = port;
  m_impl->username = username;
  m_impl->password = password;
  m_impl->vhost = vhost;
  m_impl->frame_max = frame_max;
}

MessageBroker::MessageBroker(const std::string& url, int frame_max) : m_impl(new Impl)
{
  std::srand(std::time(NULL));

  struct amqp_connection_info ci;
  char* p = strdup(url.c_str());

  die_on_error(amqp_parse_url(p, &ci), "Parsing URL");

  m_impl->host = ci.host;
  m_impl->port = ci.port;
  m_impl->username = ci.user;
  m_impl->password = ci.password;
  m_impl->vhost = ci.vhost;
  m_impl->frame_max = frame_max;

  free(p);
}

MessageBroker::~MessageBroker()
{
  m_impl->close = true;
  for (auto it = m_impl->threads.begin(); it != m_impl->threads.end(); it++) {
    it->join();
  }
}

const std::string
MessageBroker::generateReqId()
{
  static const char ALPHABET[] = { "0123456789"
                                   "abcdefgjhiklmnopqrstvwxyz"
                                   "ABCDEFGJHIKLMNOPQRSTVWXYZ" };
  std::string result;
  for (std::size_t i = 0; i < 16; i++) {
    result += ALPHABET[rand() % (sizeof(ALPHABET) - 1)];
  }
  return result;
}

const char* MessageBroker::MESSAGE_TYPE_REQUEST = "request";

const char* MessageBroker::MESSAGE_TYPE_RESPONSE = "response";

const char* MessageBroker::MESSAGE_TYPE_ERROR = "error";

void
MessageBroker::publish(const Configuration& cfg, const std::string& messagebody)
{
  AmqpConnection::Ptr conn = AmqpConnection::createInstance();
  conn->open(m_impl->host, m_impl->port);
  conn->login(
    m_impl->vhost, m_impl->username, m_impl->password, m_impl->frame_max);

  AmqpChannel::Ptr channel = AmqpChannel::createInstance(conn);
  auto [exchange, queue] = setupBroker(cfg, channel);

  Message::Ptr msg = Message::createInstance();
  msg->body() = messagebody;
  msg->properties() = cfg.properties;

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
  conn->open(m_impl->host, m_impl->port);
  conn->login(
    m_impl->vhost, m_impl->username, m_impl->password, m_impl->frame_max);

  auto channel = AmqpChannel::createInstance(conn);
  auto [exchange, reply_to] = setupBroker(cfg, channel);

  auto req = Request::createInstance();
  req->body() = messagebody;
  req->properties() = cfg.properties;

  if (!req->properties().content_type.has_value())
    req->properties().content_type = "application/json";
  if (!req->properties().delivery_mode.has_value())
    req->properties().delivery_mode = 2u;
  if (!req->properties().message_id.has_value())
    req->properties().message_id = generateReqId();
  if (!req->properties().correlation_id.has_value())
    req->properties().correlation_id = generateReqId();
  if (!req->properties().type.has_value())
    req->properties().type = MESSAGE_TYPE_REQUEST;

  channel->basicPublish(exchange, cfg.routing_key, req);
  channel->basicConsume(reply_to);

  struct timeval tv = { 30, 0 };
  Response::Ptr response;

  for (;;) {
    auto envelope = channel->basicConsumeMessage(timeout ? timeout : &tv);
    if (!envelope)
      continue;
    response = std::dynamic_pointer_cast<Response>(envelope->message());
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
    conn->open(m_impl->host, m_impl->port);
    conn->login(
      m_impl->vhost, m_impl->username, m_impl->password, m_impl->frame_max);

    auto channel = AmqpChannel::createInstance(conn);
    auto [exchange, queue] = setupBroker(cfg, channel);
    channel->basicConsume(queue);

    while (!m_impl->close) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      auto envelope = channel->basicConsumeMessage(&tv);
      if (!envelope)
        continue;
      callback(envelope->message());
    }
  });

  m_impl->threads.push_back(std::move(worker));
}

void
MessageBroker::subscribe(
  const Configuration& cfg,
  std::function<bool(const Request::Ptr, Response::Ptr)> callback)
{
  std::thread worker([this, cfg, callback]() {
    struct timeval tv = { 1, 0 };
    auto conn = AmqpConnection::createInstance();
    conn->open(m_impl->host, m_impl->port);
    conn->login(
      m_impl->vhost, m_impl->username, m_impl->password, m_impl->frame_max);

    auto channel = AmqpChannel::createInstance(conn);
    auto [exchange, queue] = setupBroker(cfg, channel);
    channel->basicConsume(queue);

    while (!m_impl->close) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      auto envelope = channel->basicConsumeMessage(&tv);
      if (!envelope)
        continue;

      Request::Ptr req = std::dynamic_pointer_cast<Request>(envelope->message());
      Response::Ptr res;

      auto ok = callback(req, res);

      std::string reply_to(envelope->message()->properties().reply_to.value());
      std::string correlation_id(
        envelope->message()->properties().correlation_id.value());

      if (!res->properties().content_type.has_value())
        res->properties().content_type = "application/json";
      if (!res->properties().delivery_mode.has_value())
        res->properties().delivery_mode = 2u;
      if (!res->properties().message_id.has_value())
        res->properties().message_id = generateReqId();
      if (!res->properties().correlation_id.has_value())
        res->properties().correlation_id = correlation_id;
      if (!res->properties().type.has_value())
        res->properties().type = ok ? MESSAGE_TYPE_RESPONSE : MESSAGE_TYPE_ERROR;

      channel->basicPublish("", reply_to, res);
    }
  });

  m_impl->threads.push_back(std::move(worker));
}

void
MessageBroker::close()
{
  m_impl->close = true;
}

std::tuple<std::string, std::string>
MessageBroker::setupBroker(const Configuration& cfg, AmqpChannel::Ptr channel)
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

} // end namespace gs

#include <iostream>
#include <condition_variable>
#include <mutex>
#include <iostream>
#include <signal.h>

#include "../message_broker.hpp"

static std::condition_variable _condition;
static std::mutex _mutex;
    class InterruptHandler {
    public:
        static void hookSIGINT() {
            signal(SIGINT, handleUserInterrupt);        
            signal(SIGTERM, handleUserInterrupt);        
        }

        static void handleUserInterrupt(int signal) {
            switch (signal) {
                case SIGINT:
                    std::cout << "SIGINT trapped" << std::endl;
                    _condition.notify_one();
                    break;
                case SIGTERM:
                    std::cout << "SIGTERM trapped" << std::endl;
                    _condition.notify_one();
                    break;
                default:
                    std::cout << "Unknown signal {} trapped " << signal << std::endl;
            }
        }

        static void waitForUserInterrupt() {
            std::unique_lock<std::mutex> lock { _mutex };
            _condition.wait(lock);
            lock.unlock();
            std::cout << "Signal requests to exit" << std::endl;
        }
    };

using namespace soft;

int main(int argc, char const *argv[])
{
	InterruptHandler::hookSIGINT();

	MessageBroker broker("amqp://guest:guest@localhost:5672");
	MessageBroker::Configuration configuration;

	configuration.exchange.name = "hello";
	configuration.exchange.type = "fanout";
	configuration.exchange.declare = true;
	configuration.queue.name = "";
	configuration.queue.exclusive = false;
	configuration.queue.declare = true;
	configuration.queue.bind = true;

	broker.subscribe(configuration, [](const auto& message) {
		std::cout << "[x] Received b'" << message.getBody() << "'" << std::endl;
	});

	InterruptHandler::waitForUserInterrupt();

	return 0;
}

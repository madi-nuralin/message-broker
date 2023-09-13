#include <iostream>
#include "../message_broker.hpp"

/*#include <condition_variable>
#include <mutex>
#include <iostream>
#include <signal.h>

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
*/
using namespace gammasoft;

int fib(int n)
{
    switch (n)
    {
    case 0:
        return 0;
    case 1:
        return 1;
    default:
        return fib(n - 1) + fib(n - 2);
    }
}

int main(int argc, char const *argv[])
{
//	InterruptHandler::hookSIGINT();
	MessageBroker broker("localhost", 5672, "guest", "guest", "/");

	/*MessageBroker::Configuration configuration;
	configuration.exchange.name = "hello";
	configuration.exchange.type = "fanout";
	configuration.exchange.declare = true;
	configuration.queue.name = "";
	configuration.queue.exclusive = false;
	configuration.queue.declare = true;
	configuration.queue.bind = true;

	broker.subscribe(configuration, [](const auto& message) {
		std::cout << "[x] Received b'" << message.getBody() << "'" << std::endl;
	});*/

	MessageBroker::Configuration configuration;
	configuration.queue.name = "rpc_queue";
	configuration.queue.declare = true;
	broker.subscribe(configuration, [](const auto& request, auto& response){
		auto number = std::stoi(request.getBody());
		std::cout << "[.] fib(" <<  number << ")" << std::endl;
		response.setBody(std::to_string(fib(number)));
		return true;
	});

//	InterruptHandler::waitForUserInterrupt();
	while(1){}
	return 0;
}
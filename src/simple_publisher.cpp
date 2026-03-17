#include <iostream>
#include <thread>

#include "rix/core/node.hpp"
#include "rix/ipc/signal.hpp"
#include "rix/msg/standard/Header.hpp"

class SimplePublisher : public rix::core::Node {
   public:
    SimplePublisher() : Node("simple_publisher", rix::ipc::Endpoint("127.0.0.1", rix::core::RIXHUB_PORT)) {
        // If the Node failed to initialize, then ok() will return false
        if (!ok()) {
            rix::util::Log::error << "Failed to create node." << std::endl;
            shutdown();
            return;
        }

        // Create a publisher on topic /chatter with message type Header
        pub = create_publisher<rix::msg::standard::Header>("/chatter");
        if (!pub->ok()) {
            rix::util::Log::error << "Failed to create publisher." << std::endl;
            shutdown();
            return;
        }

        // Initialize our message parameters
        message.frame_id = "Hello, world!";
        message.seq = 0;

        // Create a timer to run at 1.0 Hz
        timer = create_timer(rix::util::Duration(1.0),
                             std::bind(&SimplePublisher::timer_callback, this, std::placeholders::_1));
    }

   private:
    std::shared_ptr<rix::core::Publisher> pub;
    std::shared_ptr<rix::core::Timer> timer;
    rix::msg::standard::Header message;

    /**
     * @brief Timer callback that is invoked by the Node at 1.0 Hz during spin
     * 
     */
    void timer_callback(const rix::core::Timer::Event &event) {
        message.frame_id = "Hello, world!";
        message.seq += 1;
        message.stamp = rix::util::Time::now().to_msg();
        pub->publish(message);
    }
};

int main() {
    auto simple_publisher = std::make_shared<SimplePublisher>();
    if (!simple_publisher->ok()) {
        rix::util::Log::error << "Failed to create simple_publisher." << std::endl;
        return 1;
    }

    auto notif = std::make_shared<rix::ipc::Signal>(SIGINT);
    simple_publisher->spin(notif);

    return 0;
}
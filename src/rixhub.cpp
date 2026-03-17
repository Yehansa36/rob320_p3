#include <iostream>

#include "rix/core/mediator.hpp"
#include "rix/ipc/signal.hpp"

int main() {
    rix::ipc::Endpoint rixhub_endpoint("127.0.0.1", rix::core::RIXHUB_PORT);
    
    auto mediator = std::make_shared<rix::core::Mediator>(rixhub_endpoint);
    if (!mediator->ok()) {
        rix::util::Log::error << "Failed to create mediator." << std::endl;
        return 1;
    }

    auto notif = std::make_shared<rix::ipc::Signal>(SIGINT);
    mediator->spin(notif);

    return 0;
}
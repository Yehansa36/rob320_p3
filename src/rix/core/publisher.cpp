#include "rix/core/publisher.hpp"

namespace rix {
namespace core {

Publisher::Publisher(const rix::msg::mediator::PubInfo &info, std::shared_ptr<rix::ipc::interfaces::Server> server,
                     ClientFactory factory, rix::ipc::Endpoint rixhub_endpoint)
    : info_(info), server_(server), shutdown_flag_(false), factory_(factory), rixhub_endpoint_(rixhub_endpoint) {
    // Ensure server was intitialized properly
    if (!server_->ok()) {
        rix::util::Log::error << "Server invalid!" << std::endl;
        shutdown();
        return;
    }
    if (!send_message_with_opcode(factory_(), info_, PUB_REGISTER, rixhub_endpoint_)) {
        shutdown();
        return;
    }


    /**< TODO: Register the publisher with the mediator */
}

Publisher::~Publisher() {
    send_message_with_opcode_no_response(factory_(), info_, PUB_DEREGISTER, rixhub_endpoint_);
    shutdown();

    /**< TODO: Deregister the publisher with the mediator */
}

bool Publisher::ok() const { return !shutdown_flag_; }

void Publisher::shutdown() { shutdown_flag_ = true; }

/**< TODO: Implement the publish method */
void Publisher::publish(const rix::msg::Message &msg) {
    if (msg.hash() != info_.topic_info.message_hash) {
       return;
    } //check hashes match

    //create size prefix
    rix::msg::standard::UInt32 msg_size;
    msg_size.data = msg.size();

    //create buffer
    std::vector<uint8_t> buffer(msg_size.size() + msg.size());
    size_t offset = 0;

    //serialize both into buffer
    msg_size.serialize(buffer.data(), offset);
    msg.serialize(buffer.data(), offset);

    //send to all connections
    std::lock_guard<std::mutex> gaurd(connections_mutex_);
    auto it = connections_.begin();
    while (it != connections_.end()) {
        auto c = it->lock();
        if (!c || !c->is_writable()) {
            it = connections_.erase(it);
            continue;
        }
        c->write(buffer.data(), buffer.size());
        it++;
    }
}


size_t Publisher::get_subscriber_count() const {
    std::lock_guard<std::mutex> guard(connections_mutex_);
    return connections_.size();
}

/**< TODO: Implement the spin_once method */
void Publisher::spin_once() {
    // Check to see if a subscriber has made a connection
    if (!server_->wait_for_accept(rix::util::Duration(0.0))) {
        return;
    }

    // Accept a connection from a subscriber
    std::weak_ptr<rix::ipc::interfaces::Connection> conn;
    if (!server_->accept(conn)) {
        return;
    }

    // Store the connection
    std::lock_guard<std::mutex> guard(connections_mutex_);
    connections_.insert(conn);
}

}  // namespace core
}  // namespace rix
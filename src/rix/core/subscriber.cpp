#include "rix/core/subscriber.hpp"

namespace rix {
namespace core {

Subscriber::Subscriber(const rix::msg::mediator::SubInfo &info, std::shared_ptr<rix::ipc::interfaces::Server> server,
                       ClientFactory factory, const rix::ipc::Endpoint &rixhub_endpoint)
    : info_(info), server_(server), factory_(factory), callback_(nullptr), rixhub_endpoint_(rixhub_endpoint) {
    // Ensure server was intitialized properly
    if (!server_->ok()) {
        shutdown();
        return;
    }
    if (!send_message_with_opcode(factory_(), info_, SUB_REGISTER, rixhub_endpoint_)) {
        shutdown();
        return;
    }

    /**< TODO: Register the subscriber with the mediator */
}

Subscriber::~Subscriber() {
    send_message_with_opcode_no_response(factory_(), info_, SUB_DEREGISTER, rixhub_endpoint_);

    shutdown();
    

    /**< TODO: Deregister the subscriber with the mediator */
}


bool Subscriber::ok() const { return !shutdown_flag_; }

void Subscriber::shutdown() { shutdown_flag_ = true; }

Subscriber::SerializedCallback Subscriber::get_callback() const { return callback_; }

size_t Subscriber::get_publisher_count() const {
    std::lock_guard<std::mutex> guard(callback_mutex_);
    return clients_.size();
}

/**< TODO: Implement the spin_once method */
void Subscriber::spin_once() {
    if(server_ -> wait_for_accept(rix::util::Duration(0.0))) {
     
     //accept connection from mediator
    std::weak_ptr<rix::ipc::interfaces::Connection> conn;
    if (server_->accept(conn)) {
        auto c = conn.lock();

    //read operation msg
    rix::msg::mediator::Operation message;
    std::vector<uint8_t> buffer(message.size());
    c->read(buffer.data(), buffer.size());
    size_t offset = 0;
    message.deserialize(buffer.data(), buffer.size(), offset);

    //check opcode is sub_notify
    if (message.opcode == SUB_NOTIFY) {
        //read rest of msg 
        std::vector<uint8_t> buffer(message.len);
        c->read(buffer.data(), buffer.size());

        //deserialize into subnotify
        rix::msg::mediator::SubNotify notify;
        size_t offsetN = 0;
        notify.deserialize(buffer.data(), buffer.size(), offsetN);

        //connect to each publisher
        for (auto &pub : notify.publishers) {
            rix::ipc::Endpoint ep(pub.endpoint.address, pub.endpoint.port);
            auto client = factory_();
            client->set_nonblocking(true);
            client->connect(ep);
            clients_[pub.id] = client;
        }
    }
}
    }


        //check existing publisher connection for messages 
        auto it = clients_.begin();
        while (it != clients_.end()) {
            auto client = it->second;
            if (!client->ok()) {
                it = clients_.erase(it);
                continue;
            }

        if (!client->is_connected() || !client->is_readable()) {
            it++;
            continue;
        }

        //ead the size prefix 
        rix::msg::standard::UInt32 msg_size;
        std::vector<uint8_t> size_buffer(msg_size.size());
        client->read(size_buffer.data(), size_buffer.size());
        size_t offset = 0;
        msg_size.deserialize(size_buffer.data(), size_buffer.size(), offset);

        //read mesage
        std::vector<uint8_t> msg_buffer(msg_size.data);
        client->read(msg_buffer.data(), msg_buffer.size());

        //invoke callback
        std::lock_guard<std::mutex> guard(callback_mutex_);
        if (callback_) {
            callback_(msg_buffer.data(), msg_buffer.size());
        }
        it++;
    }
}

}// namespace core
}// namespace rix
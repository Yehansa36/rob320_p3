#include "rix/core/mediator.hpp"

namespace rix {
namespace core {

Mediator::~Mediator() {}

bool Mediator::ok() const { return !shutdown_flag_; }

void Mediator::shutdown() { shutdown_flag_ = true; }

/**< TODO: Implement the spin_once method */
void Mediator::spin_once() {
    if(!server_ -> wait_for_accept(rix::util::Duration(0.0))); {
     return;
    }
     //accept connection from mediator
    std::weak_ptr<rix::ipc::interfaces::Connection> conn;
    if (!server_->accept(conn)) {
        return;
    }
    auto c = conn.lock();

    //read operation msg
    rix::msg::mediator::Operation message;
    std::vector<uint8_t> buffer(message.size());
    c->read(buffer.data(), buffer.size());
    size_t offset = 0;
    message.deserialize(buffer.data(), buffer.size(), offset);
    
    //read msg bytes
    std::vector<uint8_t> buffer1(message.len);
    c->read(buffer1.data(), buffer1.size());
//NODE_REGISTER****
    if (message.opcode == NODE_REGISTER) {
        rix::msg::mediator::NodeInfo nodeInfo;
        size_t offset1 = 0;
        nodeInfo.deserialize(buffer1.data(), buffer1.size(), offset1);
        nodes_[nodeInfo.id] = nodeInfo;
        
        rix::msg::mediator::Status status;
        status.error = 0;
        send_status_message(c, status);

    }
//SUB_REGISTER******
    if (message.opcode == SUB_REGISTER) {
        rix::msg::mediator::SubInfo subInfo;
        size_t offset2 = 0;
        if (!subInfo.deserialize(buffer1.data(), buffer1.size(), offset2)) {
            rix::msg::mediator::Status status;
            status.error = 1;
            send_status_message(c, status);
            return;
        }

    if (!validate_topic_info(subInfo.topic_info)) {
        rix::msg::mediator::Status status;
        status.error = 1;
        send_status_message(c, status);
        return;
    }

    subscribers_[subInfo.id] = subInfo;
    rix::msg::mediator::Status status;
    status.error = 0;
    send_status_message(c, status);

    //collect matcing lublihsers and notify
    std::vector<rix::msg::mediator::PubInfo> matching_p;
    for (auto &pair : publishers_) {
        if(pair.second.topic_info.name == subInfo.topic_info.name) {
            matching_p.push_back(pair.second);

        }
    }
    notify_subscribers(subInfo, matching_p);

        
    } else if (message.opcode == PUB_REGISTER) {
        rix::msg::mediator::PubInfo pInfo;
        size_t offset2 = 0;

        if (!pInfo.deserialize(buffer1.data(), buffer1.size(), offset2)) {
            rix::msg::mediator::Status status;
            status.error = 1;
            send_status_message(c, status);
            return;
        } 

        if (!validate_topic_info(pInfo.topic_info)) {
            rix::msg::mediator::Status status;
            status.error = 1;
            send_status_message(c, status);
            return;
            
        }
        publishers_[pInfo.id] = pInfo;
        rix::msg::mediator::Status status;
        status.error = 0;
        send_status_message(c, status);

        //collect matching subscibers and notify
        std::vector<rix::msg::mediator::SubInfo> matching_s;
        for (auto &pair : subscribers_) {
            if (pair.second.topic_info.name == pInfo.topic_info.name) {
                matching_s.push_back(pair.second);
            }
        }
        notify_subscribers(matching_s, pInfo);
    } else if (message.opcode == NODE_DEREGISTER) {
        rix::msg::mediator::NodeInfo nodeInfo;
        size_t offset2 = 0;
        nodeInfo.deserialize(buffer1.data(), buffer1.size(), offset2);
        nodes_.erase(nodeInfo.id);

    } else if (message.opcode == SUB_DEREGISTER) {
        rix::msg::mediator::SubInfo subInfo;
        size_t offset2 = 0;
        subInfo.deserialize(buffer1.data(), buffer1.size(), offset2);
        subscribers_.erase(subInfo.id);

    } else if (message.opcode == PUB_DEREGISTER) {
        rix::msg::mediator::PubInfo pubInfo;
        size_t offset2 = 0;
        pubInfo.deserialize(buffer1.data(), buffer1.size(), offset2);
        publishers_.erase(pubInfo.id);
    }


}

/**< TODO: Implement the notify_subscribers method. */
void Mediator::notify_subscribers(const std::vector<rix::msg::mediator::SubInfo> &subscribers,
                                  const rix::msg::mediator::PubInfo &publisher) {
    
    //for each subscriber in the list
    for (auto &sub : subscribers) {
    // create a subNoitfy msg
    rix::msg::mediator::SubNotify subN;
    // set publishers field to contain just this one publisher
    subN.publishers.push_back(publisher);
    // sent it to the subscribers endpoint using send_message_with_opcose_no_response
    rix::ipc::Endpoint ep(sub.endpoint.address, sub.endpoint.port);
    send_message_with_opcode_no_response(client_factory_(), subN, SUB_NOTIFY, ep); 
    }

}

/**< TODO: Implement the notify_subscribers method. */
void Mediator::notify_subscribers(const rix::msg::mediator::SubInfo &subscriber,
                                  const std::vector<rix::msg::mediator::PubInfo> &publishers) {
    
    //create a subNotify msg
    rix::msg::mediator::SubNotify subN;
    // set the publishers field to the whole publishers vector
    subN.publishers = publishers;
    //send it to subscribers endpoint 
    rix::ipc::Endpoint ep (subscriber.endpoint.address, subscriber.endpoint.port);
    send_message_with_opcode_no_response(client_factory_(), subN, SUB_NOTIFY, ep);

}

/**< TODO: Implement the validate_topic_info method. */
bool Mediator::validate_topic_info(const rix::msg::mediator::TopicInfo &info) {
    
    auto it = topic_hashes_.find(info.name);
    if (it == topic_hashes_.end()) {
        topic_hashes_[info.name] = info.message_hash;
        return true;
    }
    //topic exists check if hash matches
    return it->second == info.message_hash; 
}

void Mediator::send_status_message(std::shared_ptr<rix::ipc::interfaces::Connection> conn,
                                   rix::msg::mediator::Status status) {
    std::vector<uint8_t> buffer(status.size());
    size_t offset = 0;
    status.serialize(buffer.data(), offset);

    ssize_t bytes = conn->write(buffer.data(), buffer.size());
    if (bytes != buffer.size()) {
        rix::util::Log::warn << "Failed to write to rixhub." << std::endl;
    }
}

Mediator::Mediator(const rix::ipc::Endpoint &rixhub_endpoint, ServerFactory server_factory,
                   ClientFactory client_factory)
    : server_(server_factory(rixhub_endpoint)), client_factory_(client_factory), shutdown_flag_(false) {
    if (!server_->ok()) {
        shutdown();
    }
    rix::util::Log::init("rixhub");
}

}  // namespace core
}  // namespace rix
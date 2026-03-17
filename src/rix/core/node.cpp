#include "rix/core/node.hpp"

namespace rix {
namespace core {

Node::~Node() {
    send_message_with_opcode_no_response(client_factory_(), info_, NODE_DEREGISTER, rixhub_endpoint_);
    shutdown();
    
    /**< TODO: Register the node with the mediator */
}

bool Node::ok() const { return !shutdown_flag_; }

void Node::shutdown() { shutdown_flag_ = true; }

void Node::spin_once() {
    // Spin all components, remove ones that are not 'ok'
    auto it = components_.begin();
    while (it != components_.end()) {
        auto component = *it;
        if (!component->ok()) {
            it = components_.erase(it);
            continue;
        }
        component->spin_once();
        it++;
    }
}

std::shared_ptr<Timer> Node::create_timer(const rix::util::Duration &d, Timer::Callback callback) {
    auto timer = std::make_shared<rix::core::Timer>(d, callback);
    components_.push_back(timer);
    return timer;
}

uint64_t Node::generate_id() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;
    return dis(gen);
}

/**< TODO: Implement the create_publisher method */
std::shared_ptr<Publisher> Node::create_publisher(const rix::msg::mediator::TopicInfo &topic_info,
                                                  const rix::ipc::Endpoint &endpoint) {
        
    rix::msg::mediator::PubInfo p_info;
     //create pub info
     p_info.id = generate_id();
     p_info.node_id = info_.id;
     p_info.protocol = 0;
     p_info.topic_info = topic_info;
     p_info.endpoint.address = endpoint.address;
     p_info.endpoint.port = endpoint.port;

     //create server
     auto server = server_factory_(endpoint);

     //create publisher using new 
     std::shared_ptr<Publisher> pub(new Publisher(p_info, server, client_factory_, rixhub_endpoint_));

     //track it - add to components_ so loops manages it 
     components_.push_back(pub);

     //return shared pointer
     return pub;
}

/**< TODO: Implement the create_subscriber method */
std::shared_ptr<Subscriber> Node::create_subscriber(const rix::msg::mediator::TopicInfo &topic_info,
                                                    const rix::ipc::Endpoint &endpoint) {
                                                       
    
    //build subINfo
    rix::msg::mediator::SubInfo s_info;
    s_info.id = generate_id();
    s_info.node_id = info_.id;
    s_info.protocol = 0;
    s_info.topic_info = topic_info;
    s_info.endpoint.address = endpoint.address;
    s_info.endpoint.port = endpoint.port;

    //create a server
    auto server = server_factory_(endpoint);

    //create subricber object
    std::shared_ptr<Subscriber> sub(new Subscriber(s_info, server, client_factory_, rixhub_endpoint_));

    //add to components
    components_.push_back(sub);
    //retunr pointer
    return sub;
}

Node::Node(const std::string &name, const rix::ipc::Endpoint &rixhub_endpoint, ServerFactory server_factory,
           ClientFactory client_factory)
    : rixhub_endpoint_(rixhub_endpoint),
      server_factory_(server_factory),
      client_factory_(client_factory),
      shutdown_flag_(false) {
    info_.id = generate_id();
    info_.name = name;
    
    send_message_with_opcode(client_factory_(), info_, NODE_REGISTER, rixhub_endpoint_);
    /**< TODO: Deregister the node with the mediator */
}

}  // namespace core
}  // namespace rix
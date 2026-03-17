#include <gmock/gmock.h>

#include <thread>

#include "mocks/mock_client.hpp"
#include "mocks/mock_server.hpp"
#include "rix/core/mediator.hpp"
#include "rix/core/node.hpp"
#include "rix/msg/standard/Header.hpp"
#include "rix/msg/standard/UInt32.hpp"

using ::testing::NiceMock;

std::shared_ptr<rix::ipc::interfaces::Server> server_factory(const rix::ipc::Endpoint &endpoint) {
    return std::make_shared<NiceMock<MockServer>>(endpoint);
}

std::shared_ptr<rix::ipc::interfaces::Client> client_factory() { return std::make_shared<NiceMock<MockClient>>(); }

TEST(RIXTest, Callback) {
    auto server_map = std::make_shared<std::map<rix::ipc::Endpoint, NiceMock<MockServer> *>>();
    auto server_mutex = std::make_shared<std::mutex>();
    NiceMock<MockClient>::address = "127.0.0.1";
    NiceMock<MockClient>::server_map = server_map;
    NiceMock<MockServer>::server_map = server_map;
    NiceMock<MockClient>::server_mutex = server_mutex;
    NiceMock<MockServer>::server_mutex = server_mutex;

    {
        rix::ipc::Endpoint rixhub_endpoint("127.0.0.1", rix::core::RIXHUB_PORT);
        auto mediator = std::make_shared<rix::core::Mediator>(rixhub_endpoint, server_factory, client_factory);
        ASSERT_TRUE(mediator->ok());
        std::thread rixhub_thread([&]() { mediator->spin(); });

        {
            auto node = std::make_shared<rix::core::Node>("test", rixhub_endpoint, server_factory, client_factory);
            EXPECT_TRUE(node->ok());

            rix::msg::standard::Header msg_received{};
            rix::ipc::Endpoint sub_endpoint("127.0.0.1", 2);
            auto sub = node->create_subscriber<rix::msg::standard::Header>(
                "/test_topic", [&](const rix::msg::standard::Header &msg) { msg_received = msg; }, sub_endpoint);
            EXPECT_TRUE(sub->ok());

            rix::ipc::Endpoint pub_endpoint("127.0.0.1", 3);
            auto pub = node->create_publisher<rix::msg::standard::Header>("/test_topic", pub_endpoint);
            EXPECT_TRUE(pub->ok());

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            node->spin_once();  // Subscriber calls connect
            node->spin_once();  // Publisher calls accept

            EXPECT_EQ(pub->get_subscriber_count(), 1);
            EXPECT_EQ(sub->get_publisher_count(), 1);

            rix::msg::standard::Header msg_publish{};
            msg_publish.seq = 1234;
            msg_publish.frame_id = "hello, world!";
            msg_publish.stamp.sec = 456;
            msg_publish.stamp.nsec = 789;
            pub->publish(msg_publish);

            node->spin_once();  // Subscriber calls read and invokes callback

            EXPECT_EQ(msg_publish.seq, msg_received.seq);
            EXPECT_EQ(msg_publish.frame_id, msg_received.frame_id);
            EXPECT_EQ(msg_publish.stamp.sec, msg_received.stamp.sec);
            EXPECT_EQ(msg_publish.stamp.nsec, msg_received.stamp.nsec);
        }

        mediator->shutdown();
        rixhub_thread.join();
    }

    NiceMock<MockClient>::server_map = nullptr;
    NiceMock<MockServer>::server_map = nullptr;
    NiceMock<MockClient>::server_mutex = nullptr;
    NiceMock<MockServer>::server_mutex = nullptr;
}

TEST(RIXTest, WrongMessage) {
    auto server_map = std::make_shared<std::map<rix::ipc::Endpoint, NiceMock<MockServer> *>>();
    auto server_mutex = std::make_shared<std::mutex>();
    NiceMock<MockClient>::address = "127.0.0.1";
    NiceMock<MockClient>::server_map = server_map;
    NiceMock<MockServer>::server_map = server_map;
    NiceMock<MockClient>::server_mutex = server_mutex;
    NiceMock<MockServer>::server_mutex = server_mutex;

    {
        rix::ipc::Endpoint rixhub_endpoint("127.0.0.1", rix::core::RIXHUB_PORT);
        auto mediator = std::make_shared<rix::core::Mediator>(rixhub_endpoint, server_factory, client_factory);
        ASSERT_TRUE(mediator->ok());
        std::thread rixhub_thread([&]() { mediator->spin(); });

        {
            auto node = std::make_shared<rix::core::Node>("test", rixhub_endpoint, server_factory, client_factory);
            EXPECT_TRUE(node->ok());

            rix::msg::standard::Header msg1_received{};
            rix::ipc::Endpoint sub1_endpoint("127.0.0.1", 2);
            auto sub1 = node->create_subscriber<rix::msg::standard::Header>(
                "/test_topic", [&](const rix::msg::standard::Header &msg) { msg1_received = msg; }, sub1_endpoint);
            EXPECT_TRUE(sub1->ok());

            rix::msg::standard::Time msg2_received{};
            rix::ipc::Endpoint sub2_endpoint("127.0.0.1", 3);
            auto sub2 = node->create_subscriber<rix::msg::standard::Time>(
                "/test_topic", [&](const rix::msg::standard::Time &msg) { msg2_received = msg; }, sub2_endpoint);
            EXPECT_FALSE(sub2->ok());

            rix::ipc::Endpoint pub_endpoint("127.0.0.1", 4);
            auto pub = node->create_publisher<rix::msg::standard::Time>("/test_topic", pub_endpoint);
            EXPECT_FALSE(pub->ok());
        }

        mediator->shutdown();
        rixhub_thread.join();
    }

    NiceMock<MockClient>::server_map = nullptr;
    NiceMock<MockServer>::server_map = nullptr;
    NiceMock<MockClient>::server_mutex = nullptr;
    NiceMock<MockServer>::server_mutex = nullptr;
}

TEST(RIXTest, MultiplePublishers) {
    auto server_map = std::make_shared<std::map<rix::ipc::Endpoint, NiceMock<MockServer> *>>();
    auto server_mutex = std::make_shared<std::mutex>();
    NiceMock<MockClient>::address = "127.0.0.1";
    NiceMock<MockClient>::server_map = server_map;
    NiceMock<MockServer>::server_map = server_map;
    NiceMock<MockClient>::server_mutex = server_mutex;
    NiceMock<MockServer>::server_mutex = server_mutex;

    {
        rix::ipc::Endpoint rixhub_endpoint("127.0.0.1", rix::core::RIXHUB_PORT);
        auto mediator = std::make_shared<rix::core::Mediator>(rixhub_endpoint, server_factory, client_factory);
        ASSERT_TRUE(mediator->ok());
        std::thread rixhub_thread([&]() { mediator->spin(); });

        {
            auto node = std::make_shared<rix::core::Node>("test", rixhub_endpoint, server_factory, client_factory);
            EXPECT_TRUE(node->ok());

            rix::msg::standard::Header msg_received{};
            rix::ipc::Endpoint sub_endpoint("127.0.0.1", 2);
            auto sub = node->create_subscriber<rix::msg::standard::Header>(
                "/test_topic", [&](const rix::msg::standard::Header &msg) { msg_received = msg; }, sub_endpoint);
            EXPECT_TRUE(sub->ok());

            rix::ipc::Endpoint pub1_endpoint("127.0.0.1", 3);
            auto pub1 = node->create_publisher<rix::msg::standard::Header>("/test_topic", pub1_endpoint);
            EXPECT_TRUE(pub1->ok());

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // Subscriber will accept a connection from the mediator and read a
            // SubNotify message containing a single Publisher. It will connect
            // to that publisher.
            node->spin_once();

            // Publisher will accept the connection from the subscriber
            node->spin_once();

            EXPECT_EQ(pub1->get_subscriber_count(), 1);
            EXPECT_EQ(sub->get_publisher_count(), 1);

            rix::ipc::Endpoint pub2_endpoint("127.0.0.1", 4);
            auto pub2 = node->create_publisher<rix::msg::standard::Header>("/test_topic", pub2_endpoint);
            EXPECT_TRUE(pub2->ok());

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // Subscriber will accept a connection from the mediator and read a
            // SubNotify message containing a single Publisher. It will connect
            // to that publisher.
            node->spin_once();

            // Publisher will accept the connection from the subscriber
            node->spin_once();

            EXPECT_EQ(pub1->get_subscriber_count(), 1);
            EXPECT_EQ(pub2->get_subscriber_count(), 1);
            EXPECT_EQ(sub->get_publisher_count(), 2);

            rix::msg::standard::Header msg_publish{};
            msg_publish.seq = 1234;
            msg_publish.frame_id = "hello, world!";
            msg_publish.stamp.sec = 456;
            msg_publish.stamp.nsec = 789;
            pub1->publish(msg_publish);

            node->spin_once();  // Subscriber calls read and invokes callback

            EXPECT_EQ(msg_publish.seq, msg_received.seq);
            EXPECT_EQ(msg_publish.frame_id, msg_received.frame_id);
            EXPECT_EQ(msg_publish.stamp.sec, msg_received.stamp.sec);
            EXPECT_EQ(msg_publish.stamp.nsec, msg_received.stamp.nsec);

            msg_publish.seq = 5678;
            msg_publish.frame_id = "robot operating systems!";
            msg_publish.stamp.sec = 789;
            msg_publish.stamp.nsec = 456;
            pub2->publish(msg_publish);

            node->spin_once();  // Subscriber calls read and invokes callback

            EXPECT_EQ(msg_publish.seq, msg_received.seq);
            EXPECT_EQ(msg_publish.frame_id, msg_received.frame_id);
            EXPECT_EQ(msg_publish.stamp.sec, msg_received.stamp.sec);
            EXPECT_EQ(msg_publish.stamp.nsec, msg_received.stamp.nsec);
        }

        mediator->shutdown();
        rixhub_thread.join();
    }

    NiceMock<MockClient>::server_map = nullptr;
    NiceMock<MockServer>::server_map = nullptr;
    NiceMock<MockClient>::server_mutex = nullptr;
    NiceMock<MockServer>::server_mutex = nullptr;
}

TEST(RIXTest, MultipleSubscribers) {
    auto server_map = std::make_shared<std::map<rix::ipc::Endpoint, NiceMock<MockServer> *>>();
    auto server_mutex = std::make_shared<std::mutex>();
    NiceMock<MockClient>::address = "127.0.0.1";
    NiceMock<MockClient>::server_map = server_map;
    NiceMock<MockServer>::server_map = server_map;
    NiceMock<MockClient>::server_mutex = server_mutex;
    NiceMock<MockServer>::server_mutex = server_mutex;

    {
        rix::ipc::Endpoint rixhub_endpoint("127.0.0.1", rix::core::RIXHUB_PORT);
        auto mediator = std::make_shared<rix::core::Mediator>(rixhub_endpoint, server_factory, client_factory);
        ASSERT_TRUE(mediator->ok());
        std::thread rixhub_thread([&]() { mediator->spin(); });

        {
            auto node = std::make_shared<rix::core::Node>("test", rixhub_endpoint, server_factory, client_factory);
            EXPECT_TRUE(node->ok());

            rix::msg::standard::Header msg1_received{};
            rix::ipc::Endpoint sub1_endpoint("127.0.0.1", 2);
            auto sub1 = node->create_subscriber<rix::msg::standard::Header>(
                "/test_topic", [&](const rix::msg::standard::Header &msg) { msg1_received = msg; }, sub1_endpoint);
            EXPECT_TRUE(sub1->ok());

            rix::msg::standard::Header msg2_received{};
            rix::ipc::Endpoint sub2_endpoint("127.0.0.1", 3);
            auto sub2 = node->create_subscriber<rix::msg::standard::Header>(
                "/test_topic", [&](const rix::msg::standard::Header &msg) { msg2_received = msg; }, sub2_endpoint);
            EXPECT_TRUE(sub2->ok());

            rix::ipc::Endpoint pub_endpoint("127.0.0.1", 4);
            auto pub = node->create_publisher<rix::msg::standard::Header>("/test_topic", pub_endpoint);
            EXPECT_TRUE(pub->ok());

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // The first subscriber will accept a connection from the mediator
            // and read a SubNotify message containing a single Publisher. It
            // will connect to that publisher.
            node->spin_once();

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // The second subscriber will accept a connection from the mediator
            // and read a SubNotify message containing a single Publisher. It
            // will connect to that publisher.
            // Publisher will accept the connection from the first subscriber
            node->spin_once();

            // Publisher will accept the connection from the second subscriber
            node->spin_once();

            EXPECT_EQ(pub->get_subscriber_count(), 2);
            EXPECT_EQ(sub1->get_publisher_count(), 1);
            EXPECT_EQ(sub2->get_publisher_count(), 1);

            rix::msg::standard::Header msg_publish{};
            msg_publish.seq = 1234;
            msg_publish.frame_id = "hello, world!";
            msg_publish.stamp.sec = 456;
            msg_publish.stamp.nsec = 789;
            pub->publish(msg_publish);

            node->spin_once();  // Subscribers call read and invoke callbacks

            EXPECT_EQ(msg_publish.seq, msg1_received.seq);
            EXPECT_EQ(msg_publish.frame_id, msg1_received.frame_id);
            EXPECT_EQ(msg_publish.stamp.sec, msg1_received.stamp.sec);
            EXPECT_EQ(msg_publish.stamp.nsec, msg1_received.stamp.nsec);

            EXPECT_EQ(msg_publish.seq, msg2_received.seq);
            EXPECT_EQ(msg_publish.frame_id, msg2_received.frame_id);
            EXPECT_EQ(msg_publish.stamp.sec, msg2_received.stamp.sec);
            EXPECT_EQ(msg_publish.stamp.nsec, msg2_received.stamp.nsec);
        }

        mediator->shutdown();
        rixhub_thread.join();
    }

    NiceMock<MockClient>::server_map = nullptr;
    NiceMock<MockServer>::server_map = nullptr;
    NiceMock<MockClient>::server_mutex = nullptr;
    NiceMock<MockServer>::server_mutex = nullptr;
}

TEST(RIXTest, MultipleBoth) {
    auto server_map = std::make_shared<std::map<rix::ipc::Endpoint, NiceMock<MockServer> *>>();
    auto server_mutex = std::make_shared<std::mutex>();
    NiceMock<MockClient>::address = "127.0.0.1";
    NiceMock<MockClient>::server_map = server_map;
    NiceMock<MockServer>::server_map = server_map;
    NiceMock<MockClient>::server_mutex = server_mutex;
    NiceMock<MockServer>::server_mutex = server_mutex;

    {
        rix::ipc::Endpoint rixhub_endpoint("127.0.0.1", rix::core::RIXHUB_PORT);
        auto mediator = std::make_shared<rix::core::Mediator>(rixhub_endpoint, server_factory, client_factory);
        ASSERT_TRUE(mediator->ok());
        std::thread rixhub_thread([&]() { mediator->spin(); });

        {
            auto node = std::make_shared<rix::core::Node>("test", rixhub_endpoint, server_factory, client_factory);
            EXPECT_TRUE(node->ok());

            rix::msg::standard::Header msg1_received{};
            rix::ipc::Endpoint sub1_endpoint("127.0.0.1", 2);
            auto sub1 = node->create_subscriber<rix::msg::standard::Header>(
                "/test_topic", [&](const rix::msg::standard::Header &msg) { msg1_received = msg; }, sub1_endpoint);
            EXPECT_TRUE(sub1->ok());

            rix::msg::standard::Header msg2_received{};
            rix::ipc::Endpoint sub2_endpoint("127.0.0.1", 3);
            auto sub2 = node->create_subscriber<rix::msg::standard::Header>(
                "/test_topic", [&](const rix::msg::standard::Header &msg) { msg2_received = msg; }, sub2_endpoint);
            EXPECT_TRUE(sub2->ok());

            rix::ipc::Endpoint pub1_endpoint("127.0.0.1", 4);
            auto pub1 = node->create_publisher<rix::msg::standard::Header>("/test_topic", pub1_endpoint);
            EXPECT_TRUE(pub1->ok());

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // The first subscriber will accept a connection from the mediator
            // and read a SubNotify message containing a single Publisher. It
            // will connect to that publisher.
            node->spin_once();

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // The second subscriber will accept a connection from the mediator
            // and read a SubNotify message containing a single Publisher. It
            // will connect to that publisher.
            // Publisher will accept the connection from the first subscriber
            node->spin_once();

            // Publisher will accept the connection from the second subscriber
            node->spin_once();

            EXPECT_EQ(pub1->get_subscriber_count(), 2);
            EXPECT_EQ(sub1->get_publisher_count(), 1);
            EXPECT_EQ(sub2->get_publisher_count(), 1);

            rix::ipc::Endpoint pub2_endpoint("127.0.0.1", 5);
            auto pub2 = node->create_publisher<rix::msg::standard::Header>("/test_topic", pub2_endpoint);
            EXPECT_TRUE(pub2->ok());

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // The first subscriber will accept a connection from the mediator
            // and read a SubNotify message containing a single Publisher. It
            // will connect to that publisher.
            node->spin_once();

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // The second subscriber will accept a connection from the mediator
            // and read a SubNotify message containing a single Publisher. It
            // will connect to that publisher.
            // Publisher will accept the connection from the first subscriber
            node->spin_once();

            // Publisher will accept the connection from the second subscriber
            node->spin_once();

            EXPECT_EQ(pub1->get_subscriber_count(), 2);
            EXPECT_EQ(pub2->get_subscriber_count(), 2);
            EXPECT_EQ(sub1->get_publisher_count(), 2);
            EXPECT_EQ(sub2->get_publisher_count(), 2);

            rix::msg::standard::Header msg_publish{};
            msg_publish.seq = 1234;
            msg_publish.frame_id = "hello, world!";
            msg_publish.stamp.sec = 456;
            msg_publish.stamp.nsec = 789;
            pub1->publish(msg_publish);

            node->spin_once();  // Subscribers call read and invoke callbacks

            EXPECT_EQ(msg_publish.seq, msg1_received.seq);
            EXPECT_EQ(msg_publish.frame_id, msg1_received.frame_id);
            EXPECT_EQ(msg_publish.stamp.sec, msg1_received.stamp.sec);
            EXPECT_EQ(msg_publish.stamp.nsec, msg1_received.stamp.nsec);
            EXPECT_EQ(msg_publish.seq, msg2_received.seq);
            EXPECT_EQ(msg_publish.frame_id, msg2_received.frame_id);
            EXPECT_EQ(msg_publish.stamp.sec, msg2_received.stamp.sec);
            EXPECT_EQ(msg_publish.stamp.nsec, msg2_received.stamp.nsec);

            msg_publish.seq = 5678;
            msg_publish.frame_id = "robot operating systems!";
            msg_publish.stamp.sec = 789;
            msg_publish.stamp.nsec = 456;
            pub2->publish(msg_publish);

            node->spin_once();  // Subscribers call read and invoke callbacks

            EXPECT_EQ(msg_publish.seq, msg1_received.seq);
            EXPECT_EQ(msg_publish.frame_id, msg1_received.frame_id);
            EXPECT_EQ(msg_publish.stamp.sec, msg1_received.stamp.sec);
            EXPECT_EQ(msg_publish.stamp.nsec, msg1_received.stamp.nsec);
            EXPECT_EQ(msg_publish.seq, msg2_received.seq);
            EXPECT_EQ(msg_publish.frame_id, msg2_received.frame_id);
            EXPECT_EQ(msg_publish.stamp.sec, msg2_received.stamp.sec);
            EXPECT_EQ(msg_publish.stamp.nsec, msg2_received.stamp.nsec);
        }

        mediator->shutdown();
        rixhub_thread.join();
    }

    NiceMock<MockClient>::server_map = nullptr;
    NiceMock<MockServer>::server_map = nullptr;
    NiceMock<MockClient>::server_mutex = nullptr;
    NiceMock<MockServer>::server_mutex = nullptr;
}

TEST(RIXTest, DifferentTopics) {
    auto server_map = std::make_shared<std::map<rix::ipc::Endpoint, NiceMock<MockServer> *>>();
    auto server_mutex = std::make_shared<std::mutex>();
    NiceMock<MockClient>::address = "127.0.0.1";
    NiceMock<MockClient>::server_map = server_map;
    NiceMock<MockServer>::server_map = server_map;
    NiceMock<MockClient>::server_mutex = server_mutex;
    NiceMock<MockServer>::server_mutex = server_mutex;

    {
        rix::ipc::Endpoint rixhub_endpoint("127.0.0.1", rix::core::RIXHUB_PORT);
        auto mediator = std::make_shared<rix::core::Mediator>(rixhub_endpoint, server_factory, client_factory);
        ASSERT_TRUE(mediator->ok());
        std::thread rixhub_thread([&]() { mediator->spin(); });

        {
            auto node = std::make_shared<rix::core::Node>("test", rixhub_endpoint, server_factory, client_factory);
            EXPECT_TRUE(node->ok());

            rix::msg::standard::Header msg1_received{};
            rix::ipc::Endpoint sub1_endpoint("127.0.0.1", 2);
            auto sub1 = node->create_subscriber<rix::msg::standard::Header>(
                "/test_topic", [&](const rix::msg::standard::Header &msg) { msg1_received = msg; }, sub1_endpoint);
            EXPECT_TRUE(sub1->ok());

            rix::ipc::Endpoint pub2_endpoint("127.0.0.1", 3);
            auto pub2 = node->create_publisher<rix::msg::standard::Time>("/other_topic", pub2_endpoint);
            EXPECT_TRUE(pub2->ok());

            rix::msg::standard::Time msg2_received{};
            rix::ipc::Endpoint sub2_endpoint("127.0.0.1", 4);
            auto sub2 = node->create_subscriber<rix::msg::standard::Time>(
                "/other_topic", [&](const rix::msg::standard::Time &msg) { msg2_received = msg; }, sub2_endpoint);
            EXPECT_TRUE(sub2->ok());

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // The second subscriber will accept a connection from the mediator
            // and read a SubNotify message containing a single Publisher. It
            // will connect to that publisher.
            node->spin_once();

            rix::ipc::Endpoint pub1_endpoint("127.0.0.1", 5);
            auto pub1 = node->create_publisher<rix::msg::standard::Header>("/test_topic", pub1_endpoint);
            EXPECT_TRUE(pub1->ok());

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // The first subscriber will accept a connection from the mediator
            // and read a SubNotify message containing a single Publisher. It
            // will connect to that publisher.
            // The second publisher will accept the connection.
            node->spin_once();

            // The first publisher will accept the connection.
            node->spin_once();

            EXPECT_EQ(pub1->get_subscriber_count(), 1);
            EXPECT_EQ(pub2->get_subscriber_count(), 1);
            EXPECT_EQ(sub1->get_publisher_count(), 1);
            EXPECT_EQ(sub2->get_publisher_count(), 1);

            rix::msg::standard::Header msg1_publish{};
            msg1_publish.seq = 1234;
            msg1_publish.frame_id = "hello, world!";
            msg1_publish.stamp.sec = 456;
            msg1_publish.stamp.nsec = 789;
            pub1->publish(msg1_publish);

            rix::msg::standard::Time msg2_publish{};
            msg2_publish.sec = 456;
            msg2_publish.nsec = 789;
            pub2->publish(msg2_publish);

            node->spin_once();  // Subscribers call read and invoke callbacks

            EXPECT_EQ(msg1_publish.seq, msg1_received.seq);
            EXPECT_EQ(msg1_publish.frame_id, msg1_received.frame_id);
            EXPECT_EQ(msg1_publish.stamp.sec, msg1_received.stamp.sec);
            EXPECT_EQ(msg1_publish.stamp.nsec, msg1_received.stamp.nsec);
            EXPECT_EQ(msg2_publish.sec, msg2_received.sec);
            EXPECT_EQ(msg2_publish.nsec, msg2_received.nsec);
        }

        mediator->shutdown();
        rixhub_thread.join();
    }

    NiceMock<MockClient>::server_map = nullptr;
    NiceMock<MockServer>::server_map = nullptr;
    NiceMock<MockClient>::server_mutex = nullptr;
    NiceMock<MockServer>::server_mutex = nullptr;
}

TEST(RIXTest, DifferentNodesAndTopics) {
    auto server_map = std::make_shared<std::map<rix::ipc::Endpoint, NiceMock<MockServer> *>>();
    auto server_mutex = std::make_shared<std::mutex>();
    NiceMock<MockClient>::address = "127.0.0.1";
    NiceMock<MockClient>::server_map = server_map;
    NiceMock<MockServer>::server_map = server_map;
    NiceMock<MockClient>::server_mutex = server_mutex;
    NiceMock<MockServer>::server_mutex = server_mutex;

    {
        rix::ipc::Endpoint rixhub_endpoint("127.0.0.1", rix::core::RIXHUB_PORT);
        auto mediator = std::make_shared<rix::core::Mediator>(rixhub_endpoint, server_factory, client_factory);
        ASSERT_TRUE(mediator->ok());
        std::thread rixhub_thread([&]() { mediator->spin(); });

        {
            auto node1 = std::make_shared<rix::core::Node>("test", rixhub_endpoint, server_factory, client_factory);
            EXPECT_TRUE(node1->ok());

            auto node2 = std::make_shared<rix::core::Node>("test", rixhub_endpoint, server_factory, client_factory);
            EXPECT_TRUE(node2->ok());

            rix::msg::standard::Header msg1_received{};
            rix::ipc::Endpoint sub1_endpoint("127.0.0.1", 3);
            auto sub1 = node1->create_subscriber<rix::msg::standard::Header>(
                "/test_topic", [&](const rix::msg::standard::Header &msg) { msg1_received = msg; }, sub1_endpoint);
            EXPECT_TRUE(sub1->ok());

            rix::ipc::Endpoint pub2_endpoint("127.0.0.1", 4);
            auto pub2 = node1->create_publisher<rix::msg::standard::Time>("/other_topic", pub2_endpoint);
            EXPECT_TRUE(pub2->ok());

            rix::msg::standard::Time msg2_received{};
            rix::ipc::Endpoint sub2_endpoint("127.0.0.1", 5);
            auto sub2 = node2->create_subscriber<rix::msg::standard::Time>(
                "/other_topic", [&](const rix::msg::standard::Time &msg) { msg2_received = msg; }, sub2_endpoint);
            EXPECT_TRUE(sub2->ok());

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // The second subscriber will accept a connection from the mediator
            // and read a SubNotify message containing a single Publisher. It
            // will connect to that publisher.
            node2->spin_once();

            rix::ipc::Endpoint pub1_endpoint("127.0.0.1", 6);
            auto pub1 = node2->create_publisher<rix::msg::standard::Header>("/test_topic", pub1_endpoint);
            EXPECT_TRUE(pub1->ok());

            // Need to ensure that the mediator has attempted to connect to the
            // subscriber before spinning the node. Not sure how else to do it
            // other than wait. (probably a structural issue in the design,
            // needs to be more testable ...)
            rix::util::sleep_for(rix::util::Duration(0.25));

            // The first subscriber will accept a connection from the mediator
            // and read a SubNotify message containing a single Publisher. It
            // will connect to that publisher.
            node1->spin_once();

            // The second publisher will accept the connection.
            node2->spin_once();

            // The first publisher will accept the connection.
            node1->spin_once();

            EXPECT_EQ(pub1->get_subscriber_count(), 1);
            EXPECT_EQ(pub2->get_subscriber_count(), 1);
            EXPECT_EQ(sub1->get_publisher_count(), 1);
            EXPECT_EQ(sub2->get_publisher_count(), 1);

            rix::msg::standard::Header msg1_publish{};
            msg1_publish.seq = 1234;
            msg1_publish.frame_id = "hello, world!";
            msg1_publish.stamp.sec = 456;
            msg1_publish.stamp.nsec = 789;
            pub1->publish(msg1_publish);

            rix::msg::standard::Time msg2_publish{};
            msg2_publish.sec = 456;
            msg2_publish.nsec = 789;
            pub2->publish(msg2_publish);

            node1->spin_once();  // Subscribers call read and invoke callbacks
            node2->spin_once();  // Subscribers call read and invoke callbacks

            EXPECT_EQ(msg1_publish.seq, msg1_received.seq);
            EXPECT_EQ(msg1_publish.frame_id, msg1_received.frame_id);
            EXPECT_EQ(msg1_publish.stamp.sec, msg1_received.stamp.sec);
            EXPECT_EQ(msg1_publish.stamp.nsec, msg1_received.stamp.nsec);
            EXPECT_EQ(msg2_publish.sec, msg2_received.sec);
            EXPECT_EQ(msg2_publish.nsec, msg2_received.nsec);
        }

        mediator->shutdown();
        rixhub_thread.join();
    }

    NiceMock<MockClient>::server_map = nullptr;
    NiceMock<MockServer>::server_map = nullptr;
    NiceMock<MockClient>::server_mutex = nullptr;
    NiceMock<MockServer>::server_mutex = nullptr;
}
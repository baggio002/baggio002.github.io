---
title: System Architecture of Kafka
date: 2026-06-09T16:45:58-04:00
author: Hu Zhao
description: >-
    Kafka is often considered the most popular Message Queue in the world. Instead of copy-pasting the official guide from apache.org or generating a generic AI response, my goal here is to explain it in the simplest way possible for complete beginners."
categories: [Data&AIML, Big Data]
tags: [kafka, architecture, message queue]
---

Kafka is often considered the most popular Message Queue in the world. Instead of copy-pasting the official guide from apache.org or generating a generic AI response, my goal here is to explain it in the simplest way possible for complete beginners.

## What is a Message Queue?

At its core, a message queue is a software component that allows different parts of a system to communicate asynchronously by sending and temporarily holding messages. It acts as an intermediate buffer, decoupling the senders from the receivers. This means neither side has to wait for the other to process data immediately.

Based on this definition, let's draw a very simple system architecture diagram:

![basic message queue](./assets/posts/system-architecutre-of-kafka/basic_message_queue.jpg)
[open in excalidraw](https://excalidraw.com/#json=xbrcXUcTvXlXSuBTb_6HP,pmlG8-qp6DzC1Bi8zEbmBQ)

It seems very simple, right? In software engineering, we use industry-standard terms to describe these components:

* Producer (or Publisher):
This is "Application 1", which generates and sends data.

* Publish:
The action of “sending messages”.

* Topic:
Think of this as a specific "channel" or "category" inside the “Message Queue” system where messages are stored.

* Subscribe:
The action of actively listening for new messages(“read messages”).

* Consumer (or Subscriber):
This is "Application 2", which reads and processes messages from the topic.

## Why Use a Message Queue?

Imagine a massive e-commerce platform like Amazon, eBay, or Taobao. During peak hours—such as Black Friday or a flash sale—the system might experience tens of thousands of concurrent requests. If all these requests attempt to hit a single database server simultaneously, the database will become overwhelmed, inevitably leading to a complete system outage.

(Side note: You might be wondering—can't we simply horizontally scale the database? Can we just build a massive database cluster to support the online shop and handle the load? That is an excellent architectural question, and I will dive into why databases alone aren't the silver bullet for this issue at the end of this post.)

To solve this, we need to decouple the application layer that receives traffic from the heavy lifting of database writes. The goal is to absorb the massive spike of requests during busy periods and process them smoothly during the system's idle time. In system architecture, we often refer to this as "load leveling" (or vividly, "peak shaving and valley filling").

Instead of making the user wait for the database to respond, the application instantly sends a message to the Message Queue and returns a successful response. The backend consumers will then read messages from the queue at their own pace and execute the actual business logic when they are available.
In this scenario, the Message Queue is exactly that silver bullet!

## The Kafka Architecture

Now that we understand the core concept of a Message Queue and the problems it solves, let's dive into Kafka.

As we discussed earlier, a single server (like our database) can easily be overwhelmed by massive traffic. If our Message Queue ran on just one machine, it would become the new bottleneck and crash under pressure!

To prevent this, Kafka is designed differently. It is fundamentally a Distributed Event Streaming Platform. Instead of running on a single machine, Kafka operates as a cluster of multiple servers working together seamlessly.

Based on my understanding, I created the following architecture diagram:

![kafka architecture1](./assets/posts/system-architecutre-of-kafka/kafka_architecture1.jpg)
[open in excalidraw](https://excalidraw.com/#json=V1xNcu3prCy0vYQ_ZvwMm,kOgd3LV2SkkwufEw24upKg)

Let's break down the key components we saw in the architecture diagram:

* Producer:
As introduced earlier, this is the application that creates and sends messages to Kafka.

* Topic:
Think of it as a logical container. It groups and collects messages of the same subject (e.g., "world_cup_event" or "italy_break_news").

* Broker:
A broker is a single Kafka server node responsible for managing and storing messages. Typically, one physical server or virtual machine runs one broker. A Kafka cluster is simply made up of multiple brokers working together.

* Partition:
A topic is not stored in one giant file; instead, it is divided into smaller, physical data shards called "partitions." This division allows Kafka to distribute data across multiple brokers, enabling massive concurrent reads and writes.

* Replica: This is the backup mechanism for partitions to ensure high availability. For example, if the “replication factor” is set to 2, it means one partition will have 1 Leader (handling reads/writes) and 1 Replica (acting as backups) spread across different brokers. Our architecture graph is set to 2.

* Consumer:
An application that subscribes to a topic and reads messages from it.

* Consumer Group:
A group of consumers working together to share the load of processing messages from a topic. Crucially, within the same group, a single partition can only be consumed by one consumer at a time. (This is exactly why Consumer 3 in our diagram is marked as "idle"—there are 4 consumers but only 3 partitions available)

Moreover, in Kafka, we call the working partition as “Leader” and replicas/backups as “Followers”.

## Disaster Management & High Availability

To truly understand the power of Kafka, let's stress-test our architecture. Imagine a worst-case scenario where multiple system failures occur simultaneously. Here is how the Kafka ecosystem automatically heals itself:

![kafka architecture12](./assets/posts/system-architecutre-of-kafka/kafka_architecture2.jpg)
[open in excalidraw](https://excalidraw.com/#json=YtRopcvhGy1nhyQLX9XVQ,ObwVo9ZmQO7qP7sE9UELsw)

### Scenario 1

Broker 2 Goes Down When Broker 2 crashes, the original Leader for Partition 1 (on both Topic 1 and Topic 2) becomes unavailable. To prevent data loss and downtime, the Kafka cluster immediately triggers a Leader Election. It automatically promotes the healthy Follower Replica located on Broker 3 to be the new Leader for Partition 1. The system seamlessly resumes reading and writing without any human intervention.

### Scenario 2

Consumer Failures and Rebalance At the exact same time, things go wrong in the application layer:

* In Consumer Group 1:
Consumer 2 breaks down. Since there are active partitions that need pulling, Kafka triggers a Rebalance. The previously idle Consumer 3 immediately steps up and takes over the responsibility of consuming from the newly elected Partition 1.

* In Consumer Group 3:
Consumer 1 goes offline for some reason. This leaves only two healthy consumers to handle three partitions. As a result, Consumer 2 heroically takes on a double load, consuming messages from both Partition 1 and Partition 2.
This is the beauty of Kafka: even in the face of infrastructure disasters, the data pipeline keeps flowing.

## Conclusion

In this post, we explored what a Message Queue is, and why we use it. We discussed the concept, architecture of Kafka, and how Kafka keeps Availability and Partition Tolerance.

In my next Kafka post, I plan to discuss the Consistency and some other details, hope you continue to follow my posts.

### ** Why Not Just Scale the Database? **

Remember the question we asked at the very beginning: During a Black Friday traffic spike, why can't we simply horizontally scale our database to handle the massive concurrent requests?

While database clustering and horizontal scaling (like sharding or adding read replicas) are essential practices in system design, they are not the silver bullet for sudden traffic spikes. Here is why from an architectural perspective:

Writes are Harder than Reads: Horizontal scaling easily solves high concurrent reads, but handling tens of thousands of concurrent writes (like creating new orders) in a relational database introduces massive complexities, such as distributed transactions and lock contention. If it happens, the system lock will cause the entire database unavailable.

Tight Coupling: Even if your database survives, your application layer is still tightly coupled. If the backend order-processing logic takes too long, the user's web request will simply time out.

This is exactly where Kafka shines. By introducing a Distributed Event Streaming Platform, we transform a synchronous, heavy-duty transaction into an asynchronous, lightweight message. We protect our expensive databases, decouple our services, and ensure that our ecosystem remains highly available (Availability) and partition-tolerant (Partition Tolerance)—gracefully handling whatever disaster or traffic spike comes its way.

Message Queues don't just move data; they protect your entire architecture.













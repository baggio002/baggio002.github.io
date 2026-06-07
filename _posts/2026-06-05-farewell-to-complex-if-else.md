---
title: Farewell to Complex If-Else - Leveraging Agents for Business Data Processing
description: >-
    Explore how to seamlessly integrate an LLM Agent into enterprise business logic. 
    This post demonstrates building an event-driven streaming pipeline using Kafka, Spark, and MySQL to asynchronously process and categorize real-time data using AI.
date: 2026-06-06 18:58:58 -0400
author: Hu Zhao
categories: [Data&AIML]
tags: [llm agent, kafka, spark, event-driven architecture, adk, data pipeline]
pin: true
---

## Preamble

Nowadays, when the term “LLM Agent” appears, we might immediately associate it with a rigid support bot that ultimately prompts us to ask, “Please transfer me to a human.” However, when we use an LLM as a programming assistant, a daily life counselor, or a scholar with extensive knowledge, we are often amazed by its capabilities, as it genuinely helps us improve our coding efficiency and information retrieval. 

Therefore, I constantly ponder this question: “Beyond just acting as a simple support bot, can an LLM Agent become an integral part of the business logic in our software projects?” 

## Overall Design

With this in mind, I envisioned a scenario: suppose we have a massive amount of text data, but it lacks specific labels and categorization attributes. In this case, we want to enrich this data with tags such as "relevant to certain topics" or "positive news." It seems exceptionally difficult to process this through traditional program logic without manually reading and labeling the texts. This is where an AI agent might be the silver bullet to solve the problem. 

Driven by this idea, I decided to develop a system that accomplishes the following: 

* Collect news data and publish messages to a message queue. 
* Build a streaming pipeline to process these messages and store them in a dataset. 
* Utilize an LLM Agent to handle these messages, identifying whether the news is related to soccer and whether it is positive news. 
* Update the dataset with these analytical findings. 

![architecutre](./assets/posts/farewell-to-complex-if-else/architecture.jpg)
[open in excalidraw](https://excalidraw.com/#json=xZKDABpkLqq0G2VINsiSR,5o4JFsN12KxOFWG5nK9aew)

Finally, I got the entire process running smoothly. Below, I will share the specific architectural design and related code. 

## Technical Details

### Data Source (Publisher)

I chose the News API as the data source. News API is a straightforward HTTP REST API for searching and retrieving live articles from across the web. More information can be found here: [https://newsapi.org/docs](https://newsapi.org/docs) 

Although this data source naturally provides batch data, I am using it to mock a real-time messaging scenario. The process is simple: fetch the data and then split it into multiple messages representing single news items. In this instance, I just used a small batch of data for presentation purposes. 

The source code is available here: [https://github.com/baggio002/news_api_subscriber](https://github.com/baggio002/news_api_subscriber). 

The `news-publisher.py` script is used to fetch data and publish the messages to the message queue. 

BTW, even we query news with key words "soccer", "football" and "足球", we can see the data is actually including lots of news unrelated to "soccer"

![not soccer](./assets/posts/farewell-to-complex-if-else/mark_not_soccer.jpg)

### Message Queue

I use Kafka as the Message Queue service. Since Kafka is the most popular message queue in the world, it does not require a detailed introduction here. In the future, I plan to post some of my Kafka study notes on this blog. 

Please refer to the official documentation for setup: [https://kafka.apache.org/quickstart/](https://kafka.apache.org/quickstart/) 

In my practice, the easiest way to deploy Kafka is by using the JVM-based Docker image: [https://kafka.apache.org/quickstart/#using-jvm-based-apache-kafka-docker-image](https://kafka.apache.org/quickstart/#using-jvm-based-apache-kafka-docker-image) 

In this architecture, Kafka manages a topic named “newsapi.topic”. The `news-publisher.py` script will publish news items directly into this topic. 

### Pipeline (Subscriber)

Currently, the messages are sitting in the queue, meaning we need a subscriber to consume them and handle the data. Since I want to simulate a real-time system, I created a streaming pipeline. I chose Spark as the pipeline component; it subscribes to the Kafka message queue, processes the data, and saves it into a dataset. 

The source code (Scala) is here: [https://github.com/baggio002/kafka-spark-pipeline](https://github.com/baggio002/kafka-spark-pipeline) 

To study and use Spark, refer to: [https://spark.apache.org/](https://spark.apache.org/) 

**Tips:** If you are interested in practicing with Spark, the official guide is a great starting point. However, I want to share a specific issue that blocked me for several hours. 

When submitting a Spark job to the Spark cluster, I encountered a 3-hour roadblock regarding “packages.” Please note that even if you have declared dependencies (such as MySQL or BigQuery connectors) in your `build.sbt` file, submitting a job without the `--packages` flag will result in a failure. You must explicitly add a flag like `--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2`. 

Also, ensure that in the package declaration above, “2.13” matches your Scala version, and “4.0.2” matches your exact Spark version. Normally, when declaring a dependency in `build.sbt`, sbt will smoothly download the library tailored to your Scala version. However, for some dependencies, there is no Scala-specific version. In those cases, if you define it as `"com.mysql" %% "mysql-connector-j" % "8.4.0"`, you will encounter a “Not found” and “Error downloading com.mysql:mysql-connector-j_2.13:8.4.0” error. This happens because the `%%` operator tells sbt to automatically append the Scala version info to the library name.  To solve this, simply replace the `%%` with a single `%`. The definition should be: `"com.mysql" % "mysql-connector-j" % "8.4.0"`. 

### Dataset

Imagine if this were a production real-time data system: every second, hundreds of messages would need to be handled and saved for data analysts. In that scenario, utilizing a data warehouse (like BigQuery) would be the correct architectural choice. However, I did not want to mock massive data throughput or use BigQuery, as doing so would incur significant cloud costs. 

Therefore, for this demonstration, I simply used a local MySQL database to store thousands of messages.  The schema from the News API is as follows:

`{source: {id: int, name: str}, author: str, title: str, description: str, url: str, urlToImage: str, publishAt: str, content: str}` 

Based on this schema, I built a table in the MySQL database:

```sql
CREATE TABLE IF NOT EXISTS `<table name>` (
    `news_id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'PRIMARY KEY',
    `source_id` VARCHAR(100),
    `source_name` VARCHAR(100),
    `author` VARCHAR(100),
    `title` TINYTEXT,
    `description` TEXT,
    `url` TEXT,
    `url_to_image` TEXT,
    `publish_at` TINYTEXT,
    `content` MEDIUMTEXT,
 
    `is_soccer` BOOLEAN,
    `is_good_news` BOOLEAN,
    PRIMARY KEY (`news_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='news api table';
```

The `is_soccer` column indicates whether the news is related to soccer, and the `is_good_news` column indicates whether the news carries a positive sentiment. Undeniably, determining the values for these two columns programmatically using traditional if-else logic is incredibly difficult. 

## LLM Agent

Finally, we arrive at the most crucial part of this blog. In China, we have a proverb: *"For a dish of vinegar, one might make a whole meal of dumplings."* In this project, the LLM Agent is the “Dumplings.”

As I mentioned earlier, identifying `is_soccer` and `is_good_news` is beyond the scope of traditional programming. Therefore, I embedded an LLM agent directly into the business logic to handle the data transformation. The source code is here: [https://github.com/baggio002/llm_agent](https://github.com/baggio002/llm_agent)

I developed the Agent using ADK ([https://adk.dev/](https://adk.dev/)), an open-source framework that lets you build, debug, and deploy reliable AI agents at an enterprise scale. I defined two methods within the Agent as “in-model” tools: `read_mysql` and `update`. I instructed the Agent with the following logic: upon receiving a `news_id`, use it to call the `read_mysql` function to retrieve the record. Next, analyze the text to determine `is_soccer` and `is_good_news`. Finally, call the `update` function to save the results back into the database.

To execute this, follow the instructions at [https://adk.dev/runtime/api-server/](https://adk.dev/runtime/api-server/). Run it as an API server, make an HTTP request, and pass the `news_id` inside the "text" field of the “newMessage” body.

You might have noticed that I did not embed the LLM Agent directly *inside* the Spark streaming pipeline. This is because an LLM Agent takes several seconds to process a single record. In a real-time streaming system, this level of latency cannot be tolerated. 

Therefore, decoupling this component to run asynchronously is the optimal architectural design. My approach is to create a secondary Kafka topic. Once the Spark Streaming pipeline successfully inserts the raw message into the database, it publishes an event to this new topic, prompting the LLM Agent to handle the data asynchronously, one by one. Alternatively, if `is_soccer` and `is_good_news` do not require real-time updates, scheduling the LLM agent to batch-process these records during off-peak hours (via a separate Spark batch pipeline) is another viable choice. 

As with any LLM application, the prompt is paramount. Please review the instructions within the code; I believe the logic is straightforward and easy to grasp. By the way, I used Gemini Pro to generate the system prompt, and it works flawlessly.

## Summary

I spent almost two weeks designing, developing, and testing this entire process, utilizing a tech stack that includes Kafka, Spark, MySQL, Python, Scala, and the ADK/LLM Agent—a true full-stack intersection of Big Data and AI.

My primary goal was to validate my hypothesis regarding enterprise agent applications and to build a strong, hands-on portfolio piece for potential future interviews. I recognize that while this project demonstrates a broad architectural scope, it intentionally trades off some depth—partly because I was unwilling to incur high cloud costs just for a demo. 

I welcome any valuable feedback to help me build a better blog. 


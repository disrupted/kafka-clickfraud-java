# kafka-clickfraud-java
> Kafka App calculating a "click fraud" score based on an incoming stream of click events indicating fake clicks by bots

Built in Java using the official [Kafka Streams](https://kafka.apache.org/documentation/streams/) client library.

## Example input topic

```json
{
  "cookie": "879e134ede5a46798187e655b5435c2d", 
  "campId": "foo", 
  "isFake": 0, 
  "timestamp": "2020-09-11T10:53:09.810150Z"
}
{
  "cookie": "10cb2ef94b9641aeb901976a1b4817da", 
  "campId": "bar", 
  "isFake": 1, 
  "timestamp": "2020-09-11T10:53:15.823862Z"
}
{
  "cookie": "0b4e5785c82740ef987bc985f2c9196c", 
  "campId": "bar", 
  "isFake": 0, 
  "timestamp": "2020-09-11T10:53:33.843298Z"
}
```

## Example output topic

```json
{
  "campaign": "foo",
  "clickFraud": 0.15
}
{
  "campaign": "bar",
  "clickFraud": 0.32
}
{
  "campaign": "bar",
  "clickFraud": 0.32
}
```

## Installation

Clone the repository, navigate inside it and install dependencies using Maven.

requires Java 8 or later

## Usage

Start the Zookeeper & Kafka server stack

```sh
docker-compose up
```

Open a separate shell and create the input topic

```sh
docker exec -it kafka-clickfraud-java_kafka_1 kafka-topics --bootstrap-server localhost:9092 --create --topic kafka-clickcounter-input
```

Import the Project in your IDE.
Run `ClickProducer.java` & `ClickCounter.java`.

Use `kafka-console-consumer` or another client to subscribe to the Kafka topics `streams-clickcounter-input` and `streams-clickcounter-output` to monitor the randomly generated Click events and the calculated Click fraud score as messages from the application.

package myapps;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class ClickProducer {

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
//    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-producer");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//    props.put("bootstrap.servers", "localhost:9092");

    final StreamsBuilder builder = new StreamsBuilder();

//    KStream<String, String> source = builder.stream("streams-clickcounter-input");

//    final Topology topology = builder.build();
//    final KafkaStreams streams = new KafkaStreams(topology, props);
    final CountDownLatch latch = new CountDownLatch(1);

    Producer<String, String> producer = new KafkaProducer<>(props);
    for (int i = 0; i < 1000; i++) {
      Message message = new Message();
      String messageString = message.toString();
      System.out.println(messageString);
      ProducerRecord<String, String> record = new ProducerRecord<>("streams-clickcounter-input", messageString);
      producer.send(record);
      producer.flush();
      TimeUnit.SECONDS.sleep(1);
    }
//    producer.close();

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        producer.close();
//        streams.close();
        latch.countDown();
      }
    });

    try {
//      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }
}
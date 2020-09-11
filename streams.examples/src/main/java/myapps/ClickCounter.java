package myapps;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class ClickCounter {

  private static final String KAFKA_APP_ID = "streams-clickfraud";
  private static final String KAFKA_SERVER_HOST = "localhost:9092";
  private static final String KAFKA_INPUT_TOPIC = KAFKA_APP_ID + "-input";
  private static final String KAFKA_OUTPUT_TOPIC = KAFKA_APP_ID + "-output";

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_APP_ID);
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_HOST);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MessageSerde.class);

    final StreamsBuilder builder = new StreamsBuilder();
    KStream<String, Message> inputMessageKStream = builder.stream(KAFKA_INPUT_TOPIC, Consumed.with(Serdes.String(), new MessageSerde()));

    KTable<String, Long> totalClickCountPerCampaign =
        inputMessageKStream
            .selectKey((k, v) -> v.campId)
            .groupByKey()
            .count();

    KTable<String, Long> fakeClickCountPerCampaign =
        inputMessageKStream
            .filter((k, v) -> v.isFake)
            .selectKey((k, v) -> v.campId)
            .groupByKey()
            .count();

    KTable<String, Double> clickFraud =
        totalClickCountPerCampaign
            .join(fakeClickCountPerCampaign, (value1, value2) -> (double) value2 / (double) value1);

    KStream<String, OutputMessage> outputMessageKStream =
        clickFraud
            .toStream()
            .map((k, v) -> KeyValue.pair(k, new OutputMessage(k, v)));
    KStream<String, String> outputStringKStream = outputMessageKStream.mapValues((key, value) -> value.toJsonString());
    outputStringKStream.foreach((campaign, outputMessage) -> System.out.println(outputMessage));
    outputStringKStream.to(KAFKA_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

    final Topology topology = builder.build();
    final KafkaStreams streams = new KafkaStreams(topology, props);
    final CountDownLatch latch = new CountDownLatch(1);

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
      @Override
      public void run() {
        streams.close();
        latch.countDown();
      }
    });

    try {
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }
}
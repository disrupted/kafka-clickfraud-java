package myapps;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class ClickCounter2 {

  public static void main(String[] args) throws Exception {
    JsonDeserializer<Message> json = new JsonDeserializer<>();
    json.addTrustedPackages("myapps.Message");

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-clickcounter");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MessageSerde.class); // working
//    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class);
//    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, json.getClass());
//    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Message.class.getName());

    Serde<String> stringSerde = Serdes.String();
    Consumed<String, String> consumed = Consumed.with(stringSerde, stringSerde);
//    Produced<String, Message> produced = Produced.with(stringSerde, new JsonSerde<>(Message.class));
    Produced<String, Message> produced = Produced.with(stringSerde, new MessageSerde());

    final StreamsBuilder builder = new StreamsBuilder();
//    KStream<String, String> kStream = builder.stream("streams-clickcounter-input", consumed);
//    KStream<String, Message> transformedKStream = kStream.mapValues((key, value) -> new Message());
    KStream<String, Message> transformedKStream = builder.stream("streams-clickcounter-input", consumed).mapValues((key, value) -> new Message());

    /* transformedKStream
        .filter((key, value) -> value.isFake)
        .selectKey((key, value) -> value.campId)
        .groupByKey()
        .count()
        .toStream()
        .to("streams-clickcounter-output", Produced.with(Serdes.String(), Serdes.Long())); */

    KTable<String, Long> clicksPerCampaign =
        transformedKStream
            .selectKey((key, value) -> value.campId)
            .groupByKey()
            .count();

    KTable<String, Long> fakeClicksPerCampaign =
        transformedKStream
            .filter((key, value) -> value.isFake)
            .selectKey((key, value) -> value.campId)
            .groupByKey()
            .count();

//    KTable<String, Pair<Long,Long>> allClicks =
//        clicksPerCampaign.join(fakeClicksPerCampaign, (value1, value2) -> new Pair<Long,Long>(value1,value2));


    KTable<String, Double> clickFraud = clicksPerCampaign.join(fakeClicksPerCampaign, (value1, value2) -> (double) value2 / (double) value1);

//    clickFraud.toStream().foreach((campId, result) -> System.out.println("campId: " + campId+ ", clickFraud: " + result));

//      KTable<String, OutputMessage> clickFraud = clicksPerCampaign.join(fakeClicksPerCampaign, (value1, value2) -> new OutputMessage(key, ((double) value2 / (double) value1)));
    KStream<String, OutputMessage> outputMessageKStream = clickFraud
        .toStream()
        .map((k,v) -> KeyValue.pair(k, new OutputMessage(k, v)));
    outputMessageKStream.foreach((campaign, outputMessage) -> System.out.println(outputMessage.toString()));
    outputMessageKStream.to("streams-clickcounter-output", Produced.with(stringSerde, new OutputMessageSerde()));


    /* transformedKStream.foreach(new ForeachAction<String, Message>() {
      @Override
      public void apply(String s, Message message) {
        clicksPerCampaign
            .filter((key, value) -> key.equals(message.campId))
            .toStream()
            .to("streams-clickcounter-output", Produced.with(Serdes.String(), Serdes.Long()));
      }
    }); *?

    /* transformedKStream.foreach(new ForeachAction<String, Message>() {
      @Override
      public void apply(String s, Message message) {
        if (message.isFake) {

        }
      }
    }); */

//    transformedKStream.groupByKey(Serialized.with(Serdes.String(), new JsonSerde<>(Message.class)));


    /* KStream<String, String> source = builder.stream("streams-clickcounter-input");
    source.foreach(new ForeachAction<String, String>() {
      @Override
      public void apply(String s, String s2) {

      }
    }); */
    /* source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
        .groupBy((key, value) -> value)
        .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
        .toStream()
        .to("streams-clickcounter-output", Produced.with(Serdes.String(), Serdes.Long())); */

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
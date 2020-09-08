package myapps;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

public class Pipe {

  public Pipe() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    final StreamsBuilder builder = new StreamsBuilder();

    // Serializers/deserializers (serde) for String and Long types
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

//    KStream<String, String> source = builder.stream("streams-plaintext-input");
//    source.to("streams-pipe-output");

    final Topology topology = builder.build();
    System.out.println(topology.describe());

    // Construct a `KStream` from the input topic "streams-plaintext-input", where message values
// represent lines of text (for the sake of this example, we ignore whatever may be stored
// in the message keys).
    KStream<String, String> textLines = builder.stream(
        "streams-plaintext-input",
        Consumed.with(stringSerde, stringSerde)
    );

    KTable<String, Long> wordCounts = textLines
        // Split each text line, by whitespace, into words.
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))

        // Group the text words as message keys
        .groupBy((key, value) -> value)

        // Count the occurrences of each word (message key).
        .count();

    // Store the running counts as a changelog stream to the output topic.
    wordCounts.toStream().to("streams-pipe-output", Produced.with(Serdes.String(), Serdes.Long()));

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

  public static void main(String[] args) {
    LineSplit pipe = new LineSplit();
  }

  public String generate_timestamp() {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    return df.format(new Date());
  }

}

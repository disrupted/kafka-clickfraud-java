package myapps;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ClickProducer {

  private static final Random random = new Random();

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    final CountDownLatch latch = new CountDownLatch(1);

    Producer<String, String> producer = new KafkaProducer<>(props);
    for (int i = 0; i < 1000; i++) {
      Message message = new Message();
      String messageString = message.toString();
      System.out.println(messageString);
      ProducerRecord<String, String> record = new ProducerRecord<>("streams-clickcounter-input", messageString);
      producer.send(record);
      producer.flush();
      TimeUnit.SECONDS.sleep(random.nextInt(10));
    }

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
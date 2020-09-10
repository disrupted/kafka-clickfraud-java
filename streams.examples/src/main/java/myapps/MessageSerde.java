package myapps;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class MessageSerde implements Serde<Message> {

  private final MessageSerializer serializer = new MessageSerializer();
  private final MessageDeserializer deserializer = new MessageDeserializer();

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    serializer.configure(configs, isKey);
    deserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    serializer.close();
    deserializer.close();
  }

  @Override
  public Serializer<Message> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<Message> deserializer() {
    return deserializer;
  }
}

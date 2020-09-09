package myapps;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class OutputMessageSerde implements Serde<OutputMessage> {
  private OutputMessageSerializer serializer = new OutputMessageSerializer();
  private OutputMessageDeserializer deserializer = new OutputMessageDeserializer();

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
  public Serializer<OutputMessage> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<OutputMessage> deserializer() {
    return deserializer;
  }
}

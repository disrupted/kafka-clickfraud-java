package myapps;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import com.google.gson.Gson;

public class MessageDeserializer implements Closeable, AutoCloseable, Deserializer<Message> {

  private static final Charset CHARSET = Charset.forName("UTF-8");
  static private Gson gson = new Gson();

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public Message deserialize(String topic, byte[] bytes) {
    try {
      // Transform the bytes to String
      String message = new String(bytes, CHARSET);
      // Return the Message object created from the String 'message'
//      ObjectMapper om = new ObjectMapper();
//      return om.readValue(topic, Message.class);
      return gson.fromJson(message, Message.class);

    } catch (Exception e) {
      throw new IllegalArgumentException("Error reading bytes", e);
    }
  }

  @Override
  public void close() {

  }
}
package myapps;

import com.google.gson.Gson;
import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;

public class OutputMessageDeserializer implements Closeable, AutoCloseable, Deserializer<OutputMessage> {

  private static final Charset CHARSET = Charset.forName("UTF-8");
  static private Gson gson = new Gson();

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public OutputMessage deserialize(String topic, byte[] bytes) {
    try {
      // Transform the bytes to String
      String outputMessage = new String(bytes, CHARSET);
      // Return the Message object created from the String 'message'
//      ObjectMapper om = new ObjectMapper();
//      return om.readValue(topic, Message.class);
      return gson.fromJson(outputMessage, OutputMessage.class);

    } catch (Exception e) {
      throw new IllegalArgumentException("Error reading bytes", e);
    }
  }

  @Override
  public void close() {

  }
}
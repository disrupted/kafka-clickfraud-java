package myapps;

import com.google.gson.Gson;
import java.io.Closeable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public class MessageSerializer implements Closeable, AutoCloseable, Serializer<Message> {

  private static final Charset CHARSET = StandardCharsets.UTF_8;
  static private final Gson gson = new Gson();

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public byte[] serialize(String s, Message message) {
    // Transform the Message object to String
//    String line = message.toString();
    String line = gson.toJson(message);
    // Return the bytes from the String 'line'
    return line.getBytes(CHARSET);
  }

  @Override
  public void close() {

  }
}
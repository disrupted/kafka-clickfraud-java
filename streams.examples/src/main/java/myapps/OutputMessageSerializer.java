package myapps;

import com.google.gson.Gson;
import java.io.Closeable;
import java.nio.charset.Charset;
import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;

public class OutputMessageSerializer implements Closeable, AutoCloseable, Serializer<OutputMessage> {

  private static final Charset CHARSET = Charset.forName("UTF-8");
  static private Gson gson = new Gson();

  @Override
  public void configure(Map<String, ?> map, boolean b) {
  }

  @Override
  public byte[] serialize(String s, OutputMessage outputMessage) {
    // Transform the Message object to String
//    String line = message.toString();
    String line = gson.toJson(outputMessage);
    // Return the bytes from the String 'line'
    return line.getBytes(CHARSET);
  }

  @Override
  public void close() {

  }
}
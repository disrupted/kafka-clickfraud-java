package myapps;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class Message implements Serde {

  private static final String[] campaigns = {"foo", "bar"};
  private static final Random random = new Random();
  public String cookie;
  public String campId;
  public boolean isFake;
  public String timestamp;

  public Message() {
    this.cookie = generateCookie();
    this.campId = getRandomCampaign(campaigns);
    this.isFake = random.nextBoolean();
    this.timestamp = generate_timestamp();
  }

  private String generate_timestamp() {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    return df.format(new Date());
  }

  private String generateCookie() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  private String getRandomCampaign(String[] campaigns) {
    return campaigns[random.nextInt(campaigns.length)];
  }

  @Override
  public Serializer serializer() {
    return new JsonSerializer();
  }

  @Override
  public Deserializer deserializer() {
    return new JsonDeserializer();
  }
}

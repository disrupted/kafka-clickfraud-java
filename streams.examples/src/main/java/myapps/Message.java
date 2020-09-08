package myapps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@JsonSerialize
public class Message implements Serde {
  private Random random = new Random();
  private String[] campaigns = {"foo", "bar"};
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
  public String toString() {
    ObjectMapper om = new ObjectMapper();
    try {
      return om.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return null;
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

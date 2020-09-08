package myapps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

@JsonSerialize
public class Message {
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
}

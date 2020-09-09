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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@JsonSerialize
public class OutputMessage implements Serde {
  public String campaign;
  public double clickFraud;

  public OutputMessage(String campaign, double clickFraud) {
    this.campaign = campaign;
    this.clickFraud = clickFraud;
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

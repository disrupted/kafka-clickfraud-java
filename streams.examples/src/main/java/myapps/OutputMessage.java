package myapps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
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
    super();
    this.campaign = campaign;
    this.clickFraud = clickFraud;
  }

  public String toJsonString() {
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

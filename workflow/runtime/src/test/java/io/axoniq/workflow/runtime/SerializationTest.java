package io.axoniq.workflow.runtime;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SerializationTest {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private static final Logger LOGGER = LoggerFactory.getLogger(SerializationTest.class);

  @Test
  public void canDeserializePojo() throws Exception {
    var p = new Pojo();
    p.setFlag(true);
    p.setName("name");
    p.setNumber(4711L);

    var string = objectMapper.writeValueAsString(p);
    LOGGER.info("deserialized pojo: '{}'", string);
  }

  @Test
  public void canDeserializeRecord() throws Exception {
    var r = new Record1("name", 4711L, true);
    var string = objectMapper.writeValueAsString(r);
    LOGGER.info("deserialized record: '{}'", string);
  }

  @Test
  public void canDeserializeMap() throws Exception {
    var map = Map.of("name", "name", "number", 4711L, "flag", true);
    var string = objectMapper.writeValueAsString(map);
    LOGGER.info("deserialized map: '{}'", string);
  }

  record Record1(
    String name,
    Long number,
    Boolean flag
  ) {}


    static class Pojo {
    String name;
    Long number;
    Boolean flag;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Long getNumber() {
      return number;
    }

    public void setNumber(Long number) {
      this.number = number;
    }

    public Boolean getFlag() {
      return flag;
    }

    public void setFlag(Boolean flag) {
      this.flag = flag;
    }
  }
}

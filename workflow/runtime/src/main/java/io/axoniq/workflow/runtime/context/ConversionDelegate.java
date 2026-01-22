package io.axoniq.workflow.runtime.context;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import io.axoniq.workflow.runtime.api.primitives.ConverterAware;
import jakarta.annotation.Nonnull;

import java.util.Map;
import java.util.function.Function;

public class ConversionDelegate implements ConverterAware {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final MapType payloadMapType = objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class);

  @Override
  public Function<Object, Map<String, Object>> typeToPayloadConverter() {
    return (t) -> objectMapper.convertValue(t, payloadMapType);
  }

  @Override
  public <T> Function<Map<String, Object>, T> payloadToTypeConverter(@Nonnull Class<T> payloadType) {
    return (payload) -> objectMapper.convertValue(payload, payloadType);
  }
}

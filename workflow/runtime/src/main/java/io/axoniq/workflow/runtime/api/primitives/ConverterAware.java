package io.axoniq.workflow.runtime.api.primitives;

import jakarta.annotation.Nonnull;

import java.util.Map;
import java.util.function.Function;

public interface ConverterAware {

  Function<Object, Map<String, Object>> typeToPayloadConverter();

  <T> Function<Map<String, Object>, T> payloadToTypeConverter(@Nonnull Class<T> payloadType);
}

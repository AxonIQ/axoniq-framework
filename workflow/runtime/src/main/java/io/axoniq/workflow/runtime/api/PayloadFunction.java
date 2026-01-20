package io.axoniq.workflow.runtime.api;

import java.util.Map;
import java.util.function.Function;

public interface PayloadFunction extends Function<Map<String, Object>, Map<String, Object>> {

}

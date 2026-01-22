package io.axoniq.workflow.runtime.api.workflow;

import java.util.Map;
import java.util.function.Function;

/**
 * Action executed consuming payload and returning payload as result.
 */
public interface PayloadProcessor extends Function<Map<String, Object>, Map<String, Object>> {

}

package io.axoniq.workflow.runtime.api;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Map;
import java.util.function.BiFunction;

/**
 * Action executed consuming payload and returning payload as result run in provided processing context.
 */
@FunctionalInterface
public interface PayloadProcessor extends BiFunction<ProcessingContext, Map<String, Object>, Map<String, Object>> {

}

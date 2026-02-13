package io.axoniq.workflow.runtime.api;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Responsible for delivering a workflow id from provided payload.
 */
@FunctionalInterface
public interface AssociationProvider extends Function<Map<String, Object>, Optional<String>> {

}

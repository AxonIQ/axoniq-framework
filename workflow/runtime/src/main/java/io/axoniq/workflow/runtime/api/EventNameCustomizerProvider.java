package io.axoniq.workflow.runtime.api;

import java.util.function.Supplier;

/**
 * Provides an event name customizer.
 */
@FunctionalInterface
public interface EventNameCustomizerProvider extends Supplier<EventNameCustomizer> {

}

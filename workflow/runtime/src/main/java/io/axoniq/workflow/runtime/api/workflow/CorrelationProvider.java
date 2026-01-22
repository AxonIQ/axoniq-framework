package io.axoniq.workflow.runtime.api.workflow;

import jakarta.annotation.Nonnull;

import java.util.Map;
import java.util.Optional;

/**
 * Responsible for delivering a workflow id from provided payload.
 */
@FunctionalInterface
public interface CorrelationProvider {

  /**
   * Returns a correlation key from given payload.
   *
   * @param payload payload to generate correlation key from.
   * @return workflow id or empty if no correlation key can be provided.
   */
  @Nonnull
  Optional<String> correlationKey(@Nonnull Map<String, Object> payload);
}

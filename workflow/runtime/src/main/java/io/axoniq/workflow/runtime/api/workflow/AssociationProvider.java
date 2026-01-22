package io.axoniq.workflow.runtime.api.workflow;

import jakarta.annotation.Nonnull;

import java.util.Map;
import java.util.Optional;

/**
 * Responsible for delivering a workflow id from provided payload.
 */
@FunctionalInterface
public interface AssociationProvider {

  /**
   * Returns an association key from given payload.
   *
   * @param payload payload to generate association key from.
   * @return workflow id or empty if no association key can be provided.
   */
  @Nonnull
  Optional<String> associationKey(@Nonnull Map<String, Object> payload);
}

/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.workflow.runtime.api.execution.state;

import jakarta.annotation.Nonnull;
import org.axonframework.common.annotation.Internal;

/**
 * Payload for the RETRYING event. Contains retry state for event sourcing and crash recovery.
 *
 * @param attempt    current retry attempt number (1-based).
 * @param maxRetries maximum number of retries configured.
 * @param error      the error that triggered the retry.
 *
 * @author Stefan Dragisic
 * @since 1.0.0
 */
@Internal
public record StepRetryInfo(int attempt, int maxRetries, @Nonnull Throwable error) {}

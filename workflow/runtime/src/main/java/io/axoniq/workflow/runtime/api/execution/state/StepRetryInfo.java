/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
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

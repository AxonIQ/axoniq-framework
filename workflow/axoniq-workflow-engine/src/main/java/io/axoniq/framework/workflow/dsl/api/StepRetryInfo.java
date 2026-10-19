/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import org.axonframework.common.annotation.Internal;

/**
 * Payload for the RETRYING event. Contains retry state for event sourcing and crash recovery.
 *
 * @param attempt    current retry attempt number (1-based).
 * @param maxRetries maximum number of retries configured.
 * @param error      the error that triggered the retry.
 * @author Stefan Dragisic
 * @since 5.4.0
 */
@Internal
public record StepRetryInfo(int attempt, int maxRetries, WorkflowError error) {

}

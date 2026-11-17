/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */
package io.axoniq.framework.workflow.dsl.api;

import org.axonframework.common.annotation.Internal;

import java.time.Instant;
import java.util.Set;

/**
 * Payload for the STARTED event of a wait-for-event step. Contains wait state for event sourcing and crash recovery.
 *
 * @param startTime    the instant the wait started
 * @param eventName    the qualified name of the awaited event
 * @param associations the associations the awaited event has to match
 * @param timeoutTime  the instant the wait times out
 * @author Stefan Dragisic
 * @since 5.4.2
 */
@Internal
public record StepWaitInfo(Instant startTime, String eventName, Set<String> associations, Instant timeoutTime) {

}

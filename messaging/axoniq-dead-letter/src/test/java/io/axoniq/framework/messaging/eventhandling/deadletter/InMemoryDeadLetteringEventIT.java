/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventhandling.deadletter;

import io.axoniq.framework.messaging.deadletter.InMemorySequencedDeadLetterQueue;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import org.axonframework.messaging.eventhandling.EventMessage;

/**
 * Integration test for {@link DeadLetteringEventHandlingComponent} using an {@link InMemorySequencedDeadLetterQueue}.
 *
 * @author Steven van Beelen
 * @author Mateusz Nowak
 * @since 5.0.0
 */
class InMemoryDeadLetteringEventIT extends AbstractDeadLetteringEventIT {

    @Override
    protected SequencedDeadLetterQueue<EventMessage> buildDeadLetterQueue() {
        return InMemorySequencedDeadLetterQueue.<EventMessage>builder()
                                               .maxSequences(1024)
                                               .maxSequenceSize(1024)
                                               .build();
    }
}

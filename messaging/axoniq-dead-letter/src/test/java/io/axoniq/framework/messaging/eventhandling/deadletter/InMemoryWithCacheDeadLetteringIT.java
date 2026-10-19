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
 * Integration test for {@link DeadLetteringEventHandlingComponent} using an {@link InMemorySequencedDeadLetterQueue}
 * wrapped with a {@link CachingSequencedDeadLetterQueue}.
 * <p>
 * This validates that the entire dead-letter processing pipeline (enqueue, retry, evict, concurrent processing) works
 * correctly when the sequence identifier cache is active. The cache optimizes {@code contains()} lookups by tracking
 * which sequence identifiers are known to be enqueued or not, and this test ensures those optimizations don't introduce
 * correctness regressions.
 *
 * @author Gerard Klijs
 * @author Mateusz Nowak
 * @since 5.1.0
 * @see CachingSequencedDeadLetterQueue
 * @see SequenceIdentifierCache
 */
class InMemoryWithCacheDeadLetteringIT extends AbstractDeadLetteringEventIT {

    @Override
    protected SequencedDeadLetterQueue<EventMessage> buildDeadLetterQueue() {
        return new CachingSequencedDeadLetterQueue<>(
                InMemorySequencedDeadLetterQueue.<EventMessage>builder()
                                                .maxSequences(1024)
                                                .maxSequenceSize(1024)
                                                .build()
        );
    }
}

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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
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
class InMemoryDeadLetteringEventIntegrationTest extends DeadLetteringEventIntegrationTest {

    @Override
    protected SequencedDeadLetterQueue<EventMessage> buildDeadLetterQueue() {
        return InMemorySequencedDeadLetterQueue.<EventMessage>builder()
                                               .maxSequences(1024)
                                               .maxSequenceSize(1024)
                                               .build();
    }
}

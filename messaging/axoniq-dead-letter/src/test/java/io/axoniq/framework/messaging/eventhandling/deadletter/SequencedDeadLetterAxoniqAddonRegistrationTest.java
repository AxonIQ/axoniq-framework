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

package io.axoniq.framework.messaging.eventhandling.deadletter;

import io.axoniq.license.entitlement.AxoniqAddonNotGrantedException;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.EntitlementMessageType;
import io.axoniq.framework.messaging.deadletter.Decisions;
import io.axoniq.framework.messaging.deadletter.InMemorySequencedDeadLetterQueue;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that constructing a {@link DeadLetteringEventHandlingComponent} registers the sequenced dead-letter addon
 * with the entitlement system.
 */
class SequencedDeadLetterAxoniqAddonRegistrationTest {

    @Test
    void constructingComponentRegistersDeadLetterAddon() {
        // given / when
        new DeadLetteringEventHandlingComponent(
                noOpDelegate(),
                InMemorySequencedDeadLetterQueue.<EventMessage>builder().build(),
                (letter, cause) -> Decisions.enqueue(cause),
                new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE),
                false
        );

        // then — claimMessage must not fail with ADDON_NOT_DECLARED, which would indicate the addon
        // was never registered. Other exceptions (e.g. grace period expired) are unrelated to registration.
        try {
            EntitlementManager.INSTANCE.claimMessage(SequencedDeadLetterAxoniqAddon.IDENTIFIER, EntitlementMessageType.EVENT, 1);
        } catch (AxoniqAddonNotGrantedException e) {
            assertThat(e.getMessage())
                    .as("Sequenced dead-letter addon should be registered with the entitlement system")
                    .doesNotContain("Addon was not found in the application");
        }
    }

    private static EventHandlingComponent noOpDelegate() {
        return new EventHandlingComponent() {
            @Override
            public Set<QualifiedName> supportedEvents() {
                return Set.of();
            }

            @Override
            public Object sequenceIdentifierFor(EventMessage event, ProcessingContext context) {
                return event.identifier();
            }

            @Override
            public void describeTo(ComponentDescriptor descriptor) {
            }

            @Override
            public MessageStream.Empty<Message> handle(EventMessage event, ProcessingContext context) {
                return MessageStream.empty();
            }
        };
    }
}

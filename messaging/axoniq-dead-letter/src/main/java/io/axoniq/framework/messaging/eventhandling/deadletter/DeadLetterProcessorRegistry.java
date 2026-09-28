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

import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterProcessor;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.common.infra.DescribableComponent;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * A {@link org.axonframework.common.configuration.Configuration}-scoped registry mapping an
 * {@link EventHandlingComponent}'s registered name to the {@link DeadLetteringEventHandlingComponent} created for it,
 * so it can be found again as a {@link SequencedDeadLetterProcessor} regardless of which decorators wrap it afterward.
 * <p>
 * {@link DeadLetterQueueConfigurationEnhancer} decorates an {@link EventHandlingComponent} with dead-lettering support
 * at a low decoration order, so that other decorators (such as messaging tracing) can still wrap the fully
 * dead-lettering-capable component afterward. As a consequence, the component ultimately registered under a given name
 * may no longer implement {@link SequencedDeadLetterProcessor} itself.
 * <p>
 * This registry lets the decorator that creates the {@link DeadLetteringEventHandlingComponent} hand out that exact
 * instance again later for resolution, independent of what any later decorator wraps around it.
 *
 * @author Jakob Hatzl
 * @see DeadLetterQueueConfigurationEnhancer
 * @since 5.4.0
 */
@Internal
class DeadLetterProcessorRegistry implements DescribableComponent {

    private final Map<String, SequencedDeadLetterProcessor<EventMessage>> processors = new ConcurrentHashMap<>();

    /**
     * Registers the given {@code processor} under the given {@code name}, so it can be {@link #get(String) looked up}
     * again later regardless of further decoration.
     *
     * @param name      the name of the {@link EventHandlingComponent} the {@code processor} was created for
     * @param processor the {@link SequencedDeadLetterProcessor} to register
     */
    void register(String name, SequencedDeadLetterProcessor<EventMessage> processor) {
        processors.put(requireNonNull(name, "The name may not be null"),
                       requireNonNull(processor, "The processor may not be null"));
    }

    /**
     * Returns the {@link SequencedDeadLetterProcessor} registered under the given {@code name}, if any.
     *
     * @param name the name of the {@link EventHandlingComponent} to find the processor for
     * @return the registered processor, or an empty {@link Optional} if none was registered under {@code name}
     */
    Optional<SequencedDeadLetterProcessor<EventMessage>> get(String name) {
        return Optional.ofNullable(processors.get(name));
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        Set<String> names = Set.copyOf(processors.keySet());
        descriptor.describeProperty("names", names);
    }
}

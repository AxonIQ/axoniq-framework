/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.eventhandling.deadletter;

import org.axonframework.common.configuration.Configuration;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import org.axonframework.messaging.eventhandling.EventMessage;

/**
 * Factory that creates a {@link SequencedDeadLetterQueue} for a given processing group.
 * <p>
 * Implementations provide the backing store for dead-lettered events. The factory is invoked once per event handling
 * component when DLQ support is enabled, allowing each component within a processor to have its own queue.
 * <p>
 * Example:
 * <pre>{@code
 * SequencedDeadLetterQueueFactory myFactory = (processingGroup, configuration) ->
 *     new MySequencedDeadLetterQueue(processingGroup, storage);
 * }</pre>
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 */
@FunctionalInterface
public interface SequencedDeadLetterQueueFactory {

    /**
     * Creates a {@link SequencedDeadLetterQueue} for the given {@code processingGroup}.
     * <p>
     * The {@code processingGroup} is a component-scoped identifier that uniquely identifies the dead letter queue
     * within its event processor. A single processor may contain multiple event handling components, each with its
     * own DLQ. The name follows the pattern {@code "DeadLetterQueue[processorName][componentName]"}, for example
     * {@code "DeadLetterQueue[myProcessor][myComponent]"} for a component named {@code "myComponent"} within a
     * processor named {@code "myProcessor"}.
     * <p>
     * Implementations should use this value as-is when scoping dead letters in their backing store
     * (e.g., as the {@code processingGroup} column in a database table).
     *
     * @param processingGroup The component-scoped identifier used to scope dead letters to a single event handling
     *                        component's queue, e.g. {@code "DeadLetterQueue[myProcessor][myComponent]"}.
     * @param configuration   The {@link Configuration} providing access to framework components needed to build the
     *                        queue (e.g., entity managers, converters). May be ignored when all dependencies are
     *                        already wired externally (e.g., in a Spring context via bean injection).
     * @return A {@link SequencedDeadLetterQueue} scoped to the given processing group.
     */
    SequencedDeadLetterQueue<EventMessage> create(String processingGroup, Configuration configuration);
}

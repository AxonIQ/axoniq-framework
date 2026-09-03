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

package io.axoniq.framework.messaging.deadletter;

import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * Contract describing a component that can process {@link DeadLetter dead letters} that it has enqueued.
 * <p>
 * Should use the {@link SequencedDeadLetterQueue} for processing as this ensures dead lettered {@link Message Messages}
 * are kept in sequence. Thus processed in order through this component.
 * <p>
 * All methods in this interface return {@link CompletableFuture} to support asynchronous processing.
 * <p>
 * Implementations are responsible for creating the appropriate processing context (e.g., a {@code UnitOfWork}) for each
 * dead letter being processed. The {@link DeadLetter} and its message will be added as resources to the processing
 * context via {@link DeadLetter#RESOURCE_KEY} and {@link Message#RESOURCE_KEY} respectively.
 *
 * @param <M> An implementation of {@link Message} contained in the processed {@link DeadLetter dead letters}.
 * @author Steven van Beelen
 * @since 4.6.0
 */
public interface SequencedDeadLetterProcessor<M extends Message> {

    /**
     * Process a sequence of {@link DeadLetter dead letters} matching the given {@code sequenceFilter}.
     * <p>
     * Note that only a <em>single</em> matching sequence is processed! Furthermore, the {@code sequenceFilter} is
     * <em>only</em> invoked for the first letter of a sequence, as the first entry blocks the entire sequence.
     * <p>
     * Each dead letter is processed in its own processing context (typically a {@code UnitOfWork}), which will have the
     * {@link DeadLetter} added as a resource (via {@link DeadLetter#RESOURCE_KEY}). The message from the dead letter is
     * also added to the context via {@link Message#RESOURCE_KEY}.
     *
     * @param sequenceFilter A filter for the first {@link DeadLetter dead letter} entries of each sequence.
     * @return a {@link CompletableFuture} with {@code true} if at least one {@link DeadLetter dead letter} was
     * processed successfully, {@code false} otherwise
     */
    CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends M>> sequenceFilter);

    /**
     * Process a sequence of {@link DeadLetter dead letters} matching the given {@code sequenceFilter} in the given
     * {@code context}.
     * <p>
     * The {@code context} can provide resources that may be required during dead-letter processing. The default
     * implementation delegates to {@link #process(Predicate)} and ignores the context.
     *
     * @param sequenceFilter a filter for the first {@link DeadLetter dead letter} entries of each sequence
     * @param context        the processing context in which to process the dead letters, if any
     * @return a {@link CompletableFuture} with {@code true} if at least one {@link DeadLetter dead letter} was
     * processed successfully, {@code false} otherwise
     */
    default CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends M>> sequenceFilter,
                                               @Nullable ProcessingContext context) {
        return process(sequenceFilter);
    }

    /**
     * Process any sequence of {@link DeadLetter dead letters} belonging to this component.
     * <p>
     * Note that only a <em>single</em> matching sequence is processed!
     * <p>
     * Each dead letter is processed in its own processing context (typically a {@code UnitOfWork}), which will have the
     * {@link DeadLetter} added as a resource (via {@link DeadLetter#RESOURCE_KEY}). The message from the dead letter is
     * also added to the context via {@link Message#RESOURCE_KEY}.
     *
     * @return a {@link CompletableFuture} with {@code true} if at least one {@link DeadLetter dead letter} was
     * processed successfully, {@code false} otherwise
     */
    default CompletableFuture<Boolean> processAny() {
        return process(letter -> true);
    }

    /**
     * Process any sequence of {@link DeadLetter dead letters} belonging to this component in the given {@code context}.
     * <p>
     * The {@code context} can provide resources that may be required during dead-letter processing. The default
     * implementation delegates to {@link #processAny()} and ignores the context.
     *
     * @param context the processing context in which to process the dead letters, if any
     * @return a {@link CompletableFuture} with {@code true} if at least one {@link DeadLetter dead letter} was
     * processed successfully, {@code false} otherwise
     */
    default CompletableFuture<Boolean> processAny(@Nullable ProcessingContext context) {
        return processAny();
    }
}

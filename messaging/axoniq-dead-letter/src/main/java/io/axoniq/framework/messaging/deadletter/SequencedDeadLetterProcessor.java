/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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
 * context via {@link DeadLetter#RESOURCE_KEY} and {@link Message#RESOURCE_KEY} respectively. The
 * {@link ProcessingContext} that can be passed on {@link #process(Predicate, ProcessingContext)} and
 * {@link #processAny(ProcessingContext)} allows for effective resource management to the downstream
 * {@code SequencedDeadLetterQueue} and actual dead-letter processing.
 *
 * @param <M> an implementation of {@link Message} contained in the processed {@link DeadLetter dead letters}
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
     * @param sequenceFilter a filter for the first {@link DeadLetter dead letter} entries of each sequence
     * @return a {@link CompletableFuture} with {@code true} if at least one {@link DeadLetter dead letter} was
     * processed successfully, {@code false} otherwise
     */
    CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends M>> sequenceFilter);

    /**
     * Process a sequence of {@link DeadLetter dead letters} matching the given {@code sequenceFilter}.
     * <p>
     * The {@code context} can be provided optionally to support effective resource management down stream in the
     * {@link SequencedDeadLetterQueue} and dead-letter processing task.
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
     * Process any sequence of {@link DeadLetter dead letters} belonging to this component in the given
     * {@code context}.
     * <p>
     * The {@code context} can be provided optionally to support effective resource management down stream in the
     * {@link SequencedDeadLetterQueue} and dead-letter processing task.
     *
     * @param context the processing context in which to process the dead letters, if any
     * @return a {@link CompletableFuture} with {@code true} if at least one {@link DeadLetter dead letter} was
     * processed successfully, {@code false} otherwise
     */
    default CompletableFuture<Boolean> processAny(@Nullable ProcessingContext context) {
        return processAny();
    }
}

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

import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.EnqueueDecision;
import io.axoniq.framework.messaging.deadletter.EnqueuePolicy;
import io.axoniq.framework.messaging.deadletter.GenericDeadLetter;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterProcessor;
import io.axoniq.framework.messaging.deadletter.SequencedDeadLetterQueue;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.DelayedMessageStream;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.eventhandling.DelegatingEventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventHandlingComponent;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.processing.streaming.token.ReplayToken;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.replay.ResetContext;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

/**
 * An {@link EventHandlingComponent} decorator that uses a {@link SequencedDeadLetterQueue} to enqueue
 * {@link EventMessage events} for which handling failed.
 * <p>
 * Uses an {@link EnqueuePolicy} to decide whether a failed event should be
 * {@link SequencedDeadLetterQueue#enqueue(Object, DeadLetter, ProcessingContext) enqueued}. Subsequent events belonging
 * to an already enqueued "sequence identifier" are also enqueued to maintain event ordering in the face of failures.
 * <p>
 * Replayed events are treated differently when their sequence already holds dead letters. Replayed events positioned
 * before the head of the sequence are handled by the delegate, since they may rebuild state the head letter depends on.
 * Replayed events already present in the sequence are skipped, and a replayed event that fails is enqueued in position
 * order instead of at the tail. Placing letters in position order touches every later letter of the sequence, so it is
 * strongly recommended to resolve enqueued dead letters, by processing or evicting them, before replaying.
 * <p>
 * This component provides operations to {@link #processAny()} {@link DeadLetter dead letters} it has enqueued through
 * the {@link SequencedDeadLetterProcessor} contract. It ensures the same delegate {@link EventHandlingComponent} is
 * used when processing dead letters as with regular event handling. Each dead letter is processed in its own
 * {@link org.axonframework.messaging.core.unitofwork.UnitOfWork UnitOfWork}, providing proper transaction boundaries.
 * <p>
 *
 * @author Steven van Beelen
 * @author Mitchell Herrijgers
 * @author Mateusz Nowak
 * @see SequencedDeadLetterQueue
 * @see SequencedDeadLetterProcessor
 * @see EnqueuePolicy
 * @since 4.6.0
 */
public class DeadLetteringEventHandlingComponent extends DelegatingEventHandlingComponent
        implements SequencedDeadLetterProcessor<EventMessage> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * The order for decorating {@link EventHandlingComponent EventHandlingComponents} with dead-lettering support.
     * <p>
     * This order is applied after interceptors to ensure dead-lettering wraps the fully decorated component.
     */
    public static final int DECORATION_ORDER = Integer.MIN_VALUE + 200;

    private final SequencedDeadLetterQueue<EventMessage> queue;
    private final EnqueuePolicy<EventMessage> enqueuePolicy;
    private final UnitOfWorkFactory unitOfWorkFactory;
    private final boolean allowReset;

    /**
     * Instantiate a {@code DeadLetteringEventHandlingComponent} with the given {@code delegate}, {@code queue}, custom
     * {@link EnqueuePolicy}, reset behavior, and {@code unitOfWorkFactory}.
     *
     * @param delegate          the {@link EventHandlingComponent} to delegate event handling to
     * @param queue             the {@link SequencedDeadLetterQueue} to store dead letters in
     * @param enqueuePolicy     the {@link EnqueuePolicy} to decide whether a failed event should be dead-lettered
     * @param unitOfWorkFactory the {@link UnitOfWorkFactory} to create
     *                          {@link org.axonframework.messaging.core.unitofwork.UnitOfWork UnitOfWork} instances for
     *                          processing dead letters
     * @param allowReset        whether to clear the queue on reset. If {@code true},
     *                          {@link SequencedDeadLetterQueue#clear(ProcessingContext)} will be invoked upon a reset
     */
    public DeadLetteringEventHandlingComponent(EventHandlingComponent delegate,
                                               SequencedDeadLetterQueue<EventMessage> queue,
                                               EnqueuePolicy<EventMessage> enqueuePolicy,
                                               UnitOfWorkFactory unitOfWorkFactory,
                                               boolean allowReset) {
        super(delegate);
        this.queue = Objects.requireNonNull(queue, "SequencedDeadLetterQueue may not be null");
        this.enqueuePolicy = Objects.requireNonNull(enqueuePolicy, "EnqueuePolicy may not be null");
        this.unitOfWorkFactory = Objects.requireNonNull(unitOfWorkFactory, "UnitOfWorkFactory may not be null");
        this.allowReset = allowReset;
        EntitlementManager.INSTANCE.registerAddon(SequencedDeadLetterAxoniqAddon.class);
    }

    @Override
    public MessageStream.Empty<Message> handle(EventMessage event,
                                               ProcessingContext context) {
        Object sequenceIdentifier = sequenceIdentifierFor(event, context);
        boolean replaying = TrackingToken.fromContext(context)
                                         .map(ReplayToken::isReplay)
                                         .orElse(false);

        CompletableFuture<MessageStream<Message>> resultStreamFuture = replaying
                ? handleReplayed(event, context, sequenceIdentifier)
                : handleRegular(event, context, sequenceIdentifier);
        return DelayedMessageStream.create(resultStreamFuture)
                                   .ignoreEntries()
                                   .cast();
    }

    private CompletableFuture<MessageStream<Message>> handleReplayed(EventMessage event,
                                                                     ProcessingContext context,
                                                                     Object sequenceId) {
        return queue.deadLetterSequence(sequenceId, context)
                    .thenCompose(sequence -> sequence.iterator().hasNext()
                            ? decideAndHandle(event, context, sequenceId, sequence.spliterator())
                            : CompletableFuture.completedFuture(handle(event, context, sequenceId, false)));
    }

    private CompletionStage<MessageStream<Message>> decideAndHandle(
            EventMessage event,
            ProcessingContext context,
            Object sequenceId,
            Spliterator<DeadLetter<? extends EventMessage>> letters
    ) {
        List<DeadLetter<? extends EventMessage>> letterList = StreamSupport.stream(letters, false).toList();
        return switch (ReplayHandleDecision.decide(event, context, letterList)) {
            case HANDLE -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Replayed event [{}] precedes the dead letters of sequence [{}]. "
                                         + "It is delivered to the wrapped EventHandlingComponent.",
                                 event.identifier(), sequenceId);
                }
                yield CompletableFuture.completedFuture(handle(event, context, sequenceId, true));
            }
            case SKIP -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Replayed event [{}] is already dead-lettered in sequence [{}]. Skipping it.",
                                 event.identifier(), sequenceId);
                }
                yield CompletableFuture.completedFuture(MessageStream.empty());
            }
            case ENQUEUE_IN_ORDER -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Replayed event [{}] will be enqueued in order within sequence [{}].",
                                 event.identifier(), sequenceId);
                }
                DeadLetter<EventMessage> letter =
                        new GenericDeadLetter<>(sequenceId, event, null, captureContext(context));
                yield DeadLetterQueueUtils.enqueueInOrder(queue, sequenceId, letter, context)
                                          .thenApply(v -> MessageStream.empty());
            }
            case REGULAR -> handleRegular(event, context, sequenceId);
        };
    }

    private CompletableFuture<MessageStream<Message>> handleRegular(EventMessage event,
                                                                    ProcessingContext context,
                                                                    Object sequenceId) {
        return queue.enqueueIfPresent(
                sequenceId,
                () -> new GenericDeadLetter<>(sequenceId, event, null, captureContext(context)),
                context
        ).thenApply(wasEnqueued -> {
            if (wasEnqueued) {
                if (logger.isInfoEnabled()) {
                    logger.info("Event with id [{}] is added to the dead-letter queue "
                                        + "since its sequence id [{}] is already present.",
                                event.identifier(), sequenceId);
                }
                return MessageStream.empty();
            }
            return handle(event, context, sequenceId, false);
        });
    }

    /**
     * Handles an event normally through the delegate, with dead-lettering on failure.
     *
     * @param event          the event to handle
     * @param context        the processing context
     * @param sequenceId     the sequence identifier
     * @param enqueueInOrder whether to enqueue this in the order of the event's stream position instead of at the
     *                       end
     * @return a stream representing the handling result with error handling
     */
    private MessageStream<Message> handle(EventMessage event,
                                          ProcessingContext context,
                                          Object sequenceId,
                                          boolean enqueueInOrder) {
        if (logger.isTraceEnabled()) {
            logger.trace("Event [{}] with sequence id [{}] is not present in the dead-letter queue. "
                                 + "Handle operation is delegated to the wrapped EventHandlingComponent.",
                         event.identifier(), sequenceId);
        }

        return delegate.handle(event, context)
                       .onErrorContinue(error -> handleError(event, context, sequenceId, error, enqueueInOrder));
    }

    /**
     * Handles an error during event processing by applying the enqueue policy.
     * <p>
     * After the dead-letter decision is made and executed (enqueue or evict), processing continues normally. The error
     * is considered "handled" by being dead-lettered or evicted, and is not propagated to the processor. This allows
     * the processor to continue with the next event rather than aborting the work package.
     *
     * @param event          the event that failed
     * @param sequenceId     the sequence identifier
     * @param error          the error that occurred
     * @param enqueueInOrder whether to enqueue this in the order of the event's stream position instead of at the
     *                       end
     * @return a stream that completes after the error is handled (either enqueued or evicted)
     */
    private MessageStream<Message> handleError(EventMessage event,
                                               ProcessingContext context,
                                               Object sequenceId,
                                               Throwable error,
                                               boolean enqueueInOrder) {
        DeadLetter<EventMessage> letter = new GenericDeadLetter<>(
                sequenceId, event, error, captureContext(context)
        );
        EnqueueDecision<EventMessage> decision = enqueuePolicy.decide(letter, error);

        if (decision.shouldEnqueue()) {
            return enqueueDeadLetter(event, context, sequenceId, error, letter, decision, enqueueInOrder);
        }

        if (logger.isInfoEnabled()) {
            logger.info("The enqueue policy decided not to dead letter event [{}].", event.identifier());
        }
        return MessageStream.empty();
    }

    private MessageStream<Message> enqueueDeadLetter(EventMessage event,
                                                     ProcessingContext context,
                                                     Object sequenceId,
                                                     Throwable error,
                                                     DeadLetter<EventMessage> letter,
                                                     EnqueueDecision<EventMessage> decision,
                                                     boolean enqueueInOrder) {
        Throwable cause = decision.enqueueCause().orElse(null);
        DeadLetter<? extends EventMessage> letterWithCause = cause != null ? letter.withCause(cause) : letter;
        DeadLetter<? extends EventMessage> letterToEnqueue = decision.withDiagnostics(letterWithCause);

        if (logger.isInfoEnabled()) {
            logger.info("Event with id [{}] is being dead-lettered due to error: {}",
                        event.identifier(), error.getMessage());
        }

        CompletableFuture<Void> enqueued = enqueueInOrder
                ? DeadLetterQueueUtils.enqueueInOrder(queue, sequenceId, letterToEnqueue, context)
                : queue.enqueue(sequenceId, letterToEnqueue, context);
        CompletableFuture<MessageStream<Message>> enqueueFuture = enqueued.thenApply(v -> MessageStream.empty());

        return DelayedMessageStream.create(enqueueFuture);
    }

    @Override
    public MessageStream.Empty<Message> handle(ResetContext resetContext, ProcessingContext context) {
        if (allowReset) {
            CompletableFuture<MessageStream<Message>> resultFuture =
                    queue.clear(context)
                         .thenApply(v -> delegate.handle(resetContext, context));
            return DelayedMessageStream.create(resultFuture)
                                       .ignoreEntries()
                                       .cast();
        }
        return delegate.handle(resetContext, context);
    }

    @Override
    public CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends EventMessage>> sequenceFilter) {
        return process(sequenceFilter, null);
    }

    @Override
    public CompletableFuture<Boolean> process(Predicate<DeadLetter<? extends EventMessage>> sequenceFilter,
                                              @Nullable ProcessingContext context) {
        DeadLetteredEventProcessingTask processingTask = new DeadLetteredEventProcessingTask(
                delegate, enqueuePolicy, unitOfWorkFactory
        );
        return queue.process(sequenceFilter, letter -> processingTask.process(letter, context), context);
    }

    @Override
    public CompletableFuture<Boolean> processAny() {
        return processAny(null);
    }

    @Override
    public CompletableFuture<Boolean> processAny(@Nullable ProcessingContext context) {
        return process(letter -> true, context);
    }

    @SuppressWarnings("unchecked")
    private static Context captureContext(ProcessingContext processingContext) {
        Context snapshot = Context.empty();
        for (Map.Entry<Context.ResourceKey<?>, Object> entry : processingContext.resources().entrySet()) {
            snapshot = snapshot.withResource((Context.ResourceKey<Object>) entry.getKey(), entry.getValue());
        }
        return snapshot;
    }
}

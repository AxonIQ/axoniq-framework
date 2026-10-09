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

package org.axonframework.test.deadline;

import org.axonframework.common.FutureUtils;
import org.axonframework.common.ObjectUtils;
import org.axonframework.common.Registration;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.DeadlineMessage;
import org.axonframework.deadline.GenericDeadlineMessage;
import org.axonframework.messaging.ContextAwareScope;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.messaging.core.DefaultMessageDispatchInterceptorChain;
import org.axonframework.messaging.core.EmptyApplicationContext;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageDispatchInterceptor;
import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.MessageHandlerInterceptorChain;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.SimpleUnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.test.FixtureExecutionException;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stub implementation of {@link DeadlineManager}. Records all scheduled, canceled and met deadlines.
 * <p>
 * Nothing fires on its own: a test advances the stub's time with {@link #advanceTimeBy(Duration, DeadlineConsumer)} or
 * {@link #advanceTimeTo(Instant, DeadlineConsumer)}, which hands every deadline that is due by then to the given
 * {@link DeadlineConsumer}. Each fired deadline runs in a unit of work from the {@link UnitOfWorkFactory} this stub was
 * built with, with the registered handler interceptors around the consumer.
 * <p>
 * The {@link org.axonframework.test.saga.SagaTestFixture} registers a stub as its {@code DeadlineManager}, so a Saga
 * handler that takes a {@code DeadlineManager} parameter schedules on it, and the fixture's deadline assertions read
 * what it recorded.
 *
 * @author Milan Savic
 * @author Steven van Beelen
 * @since 3.3
 */
public class StubDeadlineManager implements DeadlineManager {

    private final NavigableSet<ScheduledDeadlineInfo> scheduledDeadlines = new TreeSet<>();
    private final List<ScheduledDeadlineInfo> triggeredDeadlines = new CopyOnWriteArrayList<>();
    private final AtomicInteger deadlineCounter = new AtomicInteger(0);
    private final List<MessageDispatchInterceptor<? super DeadlineMessage>> dispatchInterceptors =
            new CopyOnWriteArrayList<>();
    private final List<MessageHandlerInterceptor<? super DeadlineMessage>> handlerInterceptors =
            new CopyOnWriteArrayList<>();
    private final UnitOfWorkFactory unitOfWorkFactory;
    private Instant currentDateTime;

    /**
     * Initializes the manager with {@link ZonedDateTime#now()} as current time.
     */
    public StubDeadlineManager() {
        this(ZonedDateTime.now());
    }

    /**
     * Initializes the manager with provided {@code currentDateTime} as current time.
     * <p>
     * Fired deadlines run in a unit of work bound to no application, the counterpart of the plain unit of work Axon
     * Framework 4 started. A consumer that needs components from the context, such as a Saga dispatching a command,
     * needs the {@link #StubDeadlineManager(TemporalAccessor, UnitOfWorkFactory) constructor} taking the
     * configuration's {@link UnitOfWorkFactory} instead.
     *
     * @param currentDateTime the instance to use as current date and time
     */
    public StubDeadlineManager(TemporalAccessor currentDateTime) {
        this(currentDateTime, new SimpleUnitOfWorkFactory(EmptyApplicationContext.INSTANCE));
    }

    /**
     * Initializes the manager with provided {@code currentDateTime} as current time, firing deadlines in units of work
     * from the given {@code unitOfWorkFactory}.
     * <p>
     * New in Axon Framework 5, where a unit of work is created by a factory rather than started on the current thread.
     * Passing the configuration's {@link UnitOfWorkFactory} gives a fired deadline the same components a handler gets
     * anywhere else in that configuration.
     *
     * @param currentDateTime   the instant to use as current date and time
     * @param unitOfWorkFactory the factory of the unit of work each fired deadline runs in
     */
    public StubDeadlineManager(TemporalAccessor currentDateTime, UnitOfWorkFactory unitOfWorkFactory) {
        this.currentDateTime = Instant.from(currentDateTime);
        this.unitOfWorkFactory =
                Objects.requireNonNull(unitOfWorkFactory, "The UnitOfWorkFactory may not be null.");
    }

    /**
     * Resets the initial "current time" of this manager. Must be called before any deadlines are scheduled.
     *
     * @param currentDateTime the instant to use as the current date and time
     * @throws IllegalStateException when calling this method after deadlines are scheduled
     */
    public void initializeAt(TemporalAccessor currentDateTime) throws IllegalStateException {
        if (!scheduledDeadlines.isEmpty()) {
            throw new IllegalStateException("Initializing the deadline manager at a specific dateTime must take place "
                                                    + "before any deadlines are scheduled");
        }
        this.currentDateTime = Instant.from(currentDateTime);
    }

    @Override
    public String schedule(Instant triggerDateTime,
                           String deadlineName,
                           @Nullable Object payloadOrMessage,
                           ScopeDescriptor deadlineScope) {
        DeadlineMessage scheduledMessage =
                processDispatchInterceptors(asDeadlineMessage(deadlineName, payloadOrMessage, triggerDateTime));

        scheduledDeadlines.add(new ScheduledDeadlineInfo(triggerDateTime,
                                                         deadlineName,
                                                         scheduledMessage.identifier(),
                                                         deadlineCounter.getAndIncrement(),
                                                         scheduledMessage,
                                                         deadlineScope));
        return scheduledMessage.identifier();
    }

    @Override
    public String schedule(Duration triggerDuration,
                           String deadlineName,
                           @Nullable Object payloadOrMessage,
                           ScopeDescriptor deadlineScope) {
        return schedule(currentDateTime.plus(triggerDuration), deadlineName, payloadOrMessage, deadlineScope);
    }

    @Override
    public void cancelSchedule(String deadlineName, String scheduleId) {
        scheduledDeadlines.removeIf(
                scheduledDeadline -> scheduledDeadline.getDeadlineName().equals(deadlineName)
                        && scheduledDeadline.getScheduleId().equals(scheduleId)
        );
    }

    @Override
    public void cancelAll(String deadlineName) {
        scheduledDeadlines.removeIf(scheduledDeadline -> scheduledDeadline.getDeadlineName().equals(deadlineName));
    }

    @Override
    public void cancelAllWithinScope(String deadlineName, ScopeDescriptor scope) {
        scheduledDeadlines.removeIf(
                scheduledDeadline -> scheduledDeadline.getDeadlineName().equals(deadlineName)
                        && scheduledDeadline.getDeadlineScope().equals(scope)
        );
    }

    /**
     * Return all scheduled deadlines which have not been met (yet).
     *
     * @return all scheduled deadlines which have not been met (yet)
     */
    public List<ScheduledDeadlineInfo> getScheduledDeadlines() {
        return new ArrayList<>(scheduledDeadlines);
    }

    /**
     * Return all triggered deadlines.
     *
     * @return all triggered deadlines
     */
    public List<ScheduledDeadlineInfo> getTriggeredDeadlines() {
        return Collections.unmodifiableList(triggeredDeadlines);
    }

    /**
     * Return the current date and time as an {@link Instant} as is being used by this {@link DeadlineManager}.
     *
     * @return the current date and time used by the manager
     */
    public Instant getCurrentDateTime() {
        return currentDateTime;
    }

    /**
     * Advances the "current time" of the manager to the next scheduled deadline, and returns that deadline. In theory,
     * this may cause "current time" to move backwards.
     *
     * @return {@link ScheduledDeadlineInfo} of the first scheduled deadline
     */
    public ScheduledDeadlineInfo advanceToNextTrigger() {
        ScheduledDeadlineInfo nextItem = scheduledDeadlines.pollFirst();
        if (nextItem == null) {
            throw new NoSuchElementException("There are no scheduled deadlines");
        }
        if (nextItem.getScheduleTime().isAfter(currentDateTime)) {
            currentDateTime = nextItem.getScheduleTime();
        }
        triggeredDeadlines.add(nextItem);
        return nextItem;
    }

    /**
     * Advances time to the given {@code newDateTime} and invokes the given {@code deadlineConsumer} for each deadline
     * scheduled until that time.
     *
     * @param newDateTime      the time to advance the "current time" of the manager to
     * @param deadlineConsumer the consumer to invoke for each deadline to trigger
     */
    public void advanceTimeTo(Instant newDateTime, DeadlineConsumer deadlineConsumer) {
        while (!scheduledDeadlines.isEmpty() && !scheduledDeadlines.first().getScheduleTime().isAfter(newDateTime)) {
            ScheduledDeadlineInfo scheduledDeadlineInfo = advanceToNextTrigger();
            DeadlineMessage consumedMessage = consumeDeadline(deadlineConsumer, scheduledDeadlineInfo);
            triggeredDeadlines.remove(scheduledDeadlineInfo);
            triggeredDeadlines.add(scheduledDeadlineInfo.recreateWithNewMessage(consumedMessage));
        }
        if (newDateTime.isAfter(currentDateTime)) {
            currentDateTime = newDateTime;
        }
    }

    /**
     * Advances time by the given {@code duration} and invokes the given {@code deadlineConsumer} for each deadline
     * scheduled until that time.
     *
     * @param duration         the amount of time to advance the "current time" of the manager with
     * @param deadlineConsumer the consumer to invoke for each deadline to trigger
     */
    public void advanceTimeBy(Duration duration, DeadlineConsumer deadlineConsumer) {
        advanceTimeTo(currentDateTime.plus(duration), deadlineConsumer);
    }

    /**
     * Registers the given {@code dispatchInterceptor}, which is invoked for each deadline that is scheduled.
     *
     * @param dispatchInterceptor the interceptor deciding which deadline message is scheduled
     * @return a registration to remove the interceptor again
     */
    public Registration registerDispatchInterceptor(
            MessageDispatchInterceptor<? super DeadlineMessage> dispatchInterceptor) {
        Objects.requireNonNull(dispatchInterceptor, "The dispatch interceptor may not be null.");
        dispatchInterceptors.add(dispatchInterceptor);
        return () -> dispatchInterceptors.remove(dispatchInterceptor);
    }

    /**
     * Registers the given {@code handlerInterceptor}, which is invoked around the consumer of each fired deadline.
     *
     * @param handlerInterceptor the interceptor to invoke around the consumer of a fired deadline
     * @return a registration to remove the interceptor again
     */
    public Registration registerHandlerInterceptor(
            MessageHandlerInterceptor<? super DeadlineMessage> handlerInterceptor) {
        Objects.requireNonNull(handlerInterceptor, "The handler interceptor may not be null.");
        handlerInterceptors.add(handlerInterceptor);
        return () -> handlerInterceptors.remove(handlerInterceptor);
    }

    private static DeadlineMessage asDeadlineMessage(String deadlineName,
                                                     @Nullable Object messageOrPayload,
                                                     Instant expiryTime) {
        if (messageOrPayload instanceof Message message) {
            return new GenericDeadlineMessage(deadlineName, message, () -> expiryTime);
        }
        MessageType type = new MessageType(ObjectUtils.nullSafeTypeOf(messageOrPayload));
        return new GenericDeadlineMessage(
                deadlineName, new GenericMessage(type, messageOrPayload), () -> expiryTime
        );
    }

    private DeadlineMessage processDispatchInterceptors(DeadlineMessage message) {
        // Axon Framework 4 ran the interceptors without a unit of work. Axon Framework 5's take the context of the
        // handler scheduling the deadline, if there is one.
        ProcessingContext context = ContextAwareScope.currentProcessingContext().orElse(null);
        return FutureUtils.joinAndUnwrap(
                new DefaultMessageDispatchInterceptorChain<>(dispatchInterceptors)
                        .proceed(message, context)
                        .first()
                        .<DeadlineMessage>cast()
                        .asCompletableFuture()
                        .thenApply(entry -> {
                            if (entry == null) {
                                throw new IllegalStateException(
                                        "A dispatch interceptor ended the chain without a deadline message to schedule"
                                );
                            }
                            return entry.message();
                        })
        );
    }

    private DeadlineMessage consumeDeadline(DeadlineConsumer deadlineConsumer,
                                            ScheduledDeadlineInfo scheduledDeadlineInfo) {
        DeadlineMessage deadlineMessage = scheduledDeadlineInfo.deadlineMessage();
        // Axon Framework 4 recorded the message the interceptors handed to the consumer as the triggered one.
        AtomicReference<DeadlineMessage> consumedMessage = new AtomicReference<>(deadlineMessage);
        MessageHandlerInterceptorChain<DeadlineMessage> chain = interceptorChain((message, context) -> {
            consumedMessage.set(message);
            deadlineConsumer.consume(scheduledDeadlineInfo.getDeadlineScope(), message, context);
        });
        try {
            FutureUtils.joinAndUnwrap(
                    unitOfWorkFactory.create()
                                     .executeWithResult(context -> chain
                                             .proceed(deadlineMessage, Message.addToContext(context, deadlineMessage))
                                             .ignoreEntries()
                                             .asCompletableFuture())
            );
        } catch (Exception e) {
            throw new FixtureExecutionException("Exception occurred while handling the deadline", e);
        }
        return consumedMessage.get();
    }

    private MessageHandlerInterceptorChain<DeadlineMessage> interceptorChain(Handler handler) {
        // Interceptors are declared against a super type of DeadlineMessage, so each can handle the deadline fired
        // here; narrowing them lets the chain be typed against it.
        @SuppressWarnings("unchecked")
        List<MessageHandlerInterceptor<DeadlineMessage>> narrowed =
                (List<MessageHandlerInterceptor<DeadlineMessage>>) (List<?>) List.copyOf(handlerInterceptors);
        Iterator<MessageHandlerInterceptor<DeadlineMessage>> interceptors = narrowed.iterator();
        return new MessageHandlerInterceptorChain<>() {
            @Override
            public MessageStream<?> proceed(DeadlineMessage message, ProcessingContext context) {
                try {
                    if (interceptors.hasNext()) {
                        return interceptors.next().interceptOnHandle(message, context, this);
                    }
                    handler.handle(message, Message.addToContext(context, message));
                    return MessageStream.empty();
                } catch (Exception e) {
                    return MessageStream.failed(e);
                }
            }
        };
    }

    @FunctionalInterface
    private interface Handler {

        void handle(DeadlineMessage message, ProcessingContext context) throws Exception;
    }
}

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

package org.axonframework.deadline;

import io.axoniq.framework.legacy.LegacySupportAxoniqAddon;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.ObjectUtils;
import org.axonframework.common.Registration;
import org.axonframework.messaging.ContextAwareScope;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.Context;
import org.axonframework.messaging.core.DefaultMessageDispatchInterceptorChain;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageDispatchInterceptor;
import org.axonframework.messaging.core.MessageHandlerInterceptor;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.ProcessingLifecycle;
import org.axonframework.modelling.saga.repository.AnnotatedSagaRepository;
import org.jspecify.annotations.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Abstract implementation of the {@link DeadlineManager} to be implemented by concrete solutions for the
 * DeadlineManager. Provides functionality to perform a call to the DeadlineManager when the {@link ProcessingContext}
 * of the current invocation prepares its commit. This {@link #runOnPrepareCommitOrNow(Runnable)} functionality is
 * required, as the DeadlineManager schedules a Message which needs to happen in order with the other messages
 * published throughout the system.
 * <p>
 * Axon Framework 4 found that moment through the ambient unit of work. Axon Framework 5 has none, so a call is
 * deferred when it is made while a {@link ContextAwareScope} is current, which is the case while a Saga handler
 * method runs. A call made anywhere else runs immediately.
 *
 * @author Steven van Beelen
 * @since 3.3
 */
public abstract class AbstractDeadlineManager implements DeadlineManager {

    /**
     * The {@link ProcessingLifecycle.Phase phase} in which calls deferred by {@link #runOnPrepareCommitOrNow(Runnable)}
     * run, ordered after {@link ProcessingLifecycle.DefaultPhases#PREPARE_COMMIT PREPARE_COMMIT} and the Saga write
     * ({@link AnnotatedSagaRepository#WRITE_SAGA}), and before {@link ProcessingLifecycle.DefaultPhases#COMMIT COMMIT}.
     * It is derived from the Saga write, so that moving the Saga write cannot reorder the deadline calls before it.
     * <p>
     * A Saga is invoked from within {@code PREPARE_COMMIT} when a subscribing event processor is fed by a
     * {@link org.axonframework.messaging.eventhandling.SimpleEventBus}. Registering for {@code PREPARE_COMMIT} itself
     * would therefore fail. Following the Saga write keeps the Axon Framework 4 order, where the Saga write was
     * registered for prepare-commit when the Saga was loaded, before its handler made any deadline call.
     */
    static final ProcessingLifecycle.Phase RUN_DEADLINE_CALLS =
            () -> AnnotatedSagaRepository.WRITE_SAGA.order() + 2_500;

    private static final String TOO_LATE_TO_DEFER =
            "Cannot defer a deadline call to a ProcessingContext whose deferred deadline calls already ran";

    private static final MessageTypeResolver MESSAGE_TYPE_RESOLVER = new ClassBasedMessageTypeResolver();

    private final List<MessageDispatchInterceptor<? super DeadlineMessage>> dispatchInterceptors =
            new CopyOnWriteArrayList<>();
    private final List<MessageHandlerInterceptor<? super DeadlineMessage>> handlerInterceptors =
            new CopyOnWriteArrayList<>();
    private final Context.ResourceKey<DeferredCalls> deferredCallsKey =
            Context.ResourceKey.withLabel("deferredDeadlineCalls");

    /**
     * Instantiate an {@link AbstractDeadlineManager}, registering the {@link LegacySupportAxoniqAddon} with the
     * license entitlement system.
     */
    protected AbstractDeadlineManager() {
        EntitlementManager.INSTANCE.registerAddon(LegacySupportAxoniqAddon.class);
    }

    /**
     * Run a given {@code deadlineCall} immediately, or defer it to a phase of the {@link ProcessingContext} carried by
     * the current {@link ContextAwareScope}, if one is active. That phase runs after the Saga write and before the
     * context commits. This is required as
     * the DeadlineManager schedules messages which we want to happen in order with other messages being handled.
     * <p>
     * Deferred calls run in the order they were made, also across Sagas sharing the same {@link ProcessingContext}, as
     * they did in the prepare-commit phase of an Axon Framework 4 unit of work. They never run when the context rolls
     * back before reaching that phase.
     *
     * @param deadlineCall a {@link Runnable} to be executed now, or when the {@link ProcessingContext} of the current
     *                     scope prepares its commit
     * @throws IllegalStateException if the calls deferred to the {@link ProcessingContext} of the current scope already
     *                               ran, as nothing would run the given {@code deadlineCall} anymore
     */
    protected void runOnPrepareCommitOrNow(Runnable deadlineCall) {
        Objects.requireNonNull(deadlineCall, "The deadline call may not be null.");
        Optional<ProcessingContext> context = ContextAwareScope.currentProcessingContext();
        if (context.isEmpty()) {
            deadlineCall.run();
            return;
        }
        if (!deferredCalls(context.get()).offer(deadlineCall)) {
            throw new IllegalStateException(TOO_LATE_TO_DEFER);
        }
    }

    /**
     * Returns the calls deferred to the given {@code context}, creating them and registering their
     * {@link #RUN_DEADLINE_CALLS} action on first use.
     * <p>
     * One phase action runs all calls so that they keep their order: the actions registered for a single phase may run
     * concurrently. When the {@code context} already started the {@link #RUN_DEADLINE_CALLS} phase, it rejects that
     * action. The rejection is rethrown with the message of a call made after the deferred calls ran, as both mean
     * nothing would run the call anymore.
     */
    private DeferredCalls deferredCalls(ProcessingContext context) {
        return context.computeResourceIfAbsent(deferredCallsKey, () -> {
            DeferredCalls calls = new DeferredCalls();
            try {
                context.runOn(RUN_DEADLINE_CALLS, phaseContext -> calls.drain().forEach(Runnable::run));
            } catch (IllegalStateException e) {
                throw new IllegalStateException(TOO_LATE_TO_DEFER, e);
            }
            return calls;
        });
    }

    /**
     * Registers the given {@code dispatchInterceptor}, which is applied to every {@link DeadlineMessage} before it is
     * scheduled.
     * <p>
     * Concrete deadline managers apply the registered interceptors through
     * {@link #processDispatchInterceptors(DeadlineMessage)}, inside the deferred call when the call is deferred.
     *
     * @param dispatchInterceptor the interceptor to apply to deadline messages before they are scheduled
     * @return a {@link Registration} that removes the given {@code dispatchInterceptor} again when cancelled
     */
    public Registration registerDispatchInterceptor(
            MessageDispatchInterceptor<? super DeadlineMessage> dispatchInterceptor) {
        Objects.requireNonNull(dispatchInterceptor, "The dispatch interceptor may not be null.");
        dispatchInterceptors.add(dispatchInterceptor);
        return () -> dispatchInterceptors.remove(dispatchInterceptor);
    }

    /**
     * Registers the given {@code handlerInterceptor}, which is applied around the handling of every
     * {@link DeadlineMessage} when its deadline fires.
     * <p>
     * Concrete deadline managers apply the registered interceptors, obtained through {@link #handlerInterceptors()},
     * when they deliver a fired deadline to its scope.
     *
     * @param handlerInterceptor the interceptor to apply around the handling of fired deadline messages
     * @return a {@link Registration} that removes the given {@code handlerInterceptor} again when cancelled
     */
    public Registration registerHandlerInterceptor(
            MessageHandlerInterceptor<? super DeadlineMessage> handlerInterceptor) {
        Objects.requireNonNull(handlerInterceptor, "The handler interceptor may not be null.");
        handlerInterceptors.add(handlerInterceptor);
        return () -> handlerInterceptors.remove(handlerInterceptor);
    }

    /**
     * Provides a list of registered dispatch interceptors. Do note that this list is not modifiable, and that changes
     * in the internal structure for dispatch interceptors will be reflected in this list.
     *
     * @return a list of dispatch interceptors
     */
    protected List<MessageDispatchInterceptor<? super DeadlineMessage>> dispatchInterceptors() {
        return Collections.unmodifiableList(dispatchInterceptors);
    }

    /**
     * Provides a list of registered handler interceptors. Do note that this list is not modifiable, and that changes
     * in the internal structure for handler interceptors will be reflected in this list.
     *
     * @return a list of handler interceptors
     */
    protected List<MessageHandlerInterceptor<? super DeadlineMessage>> handlerInterceptors() {
        return Collections.unmodifiableList(handlerInterceptors);
    }

    /**
     * Applies registered {@link MessageDispatchInterceptor}s to the given {@code message}.
     * <p>
     * A failing interceptor fails this call with the interceptor's own exception. A deadline always needs a message to
     * schedule, so an interceptor that ends the chain without one fails this call with an
     * {@link IllegalStateException}, instead of scheduling a {@code null} message.
     *
     * @param message the deadline message to be intercepted
     * @return the intercepted message, never {@code null}
     * @throws IllegalStateException if an interceptor ended the chain without a message
     */
    protected DeadlineMessage processDispatchInterceptors(DeadlineMessage message) {
        Objects.requireNonNull(message, "The deadline message may not be null.");
        return FutureUtils.joinAndUnwrap(
                new DefaultMessageDispatchInterceptorChain<>(dispatchInterceptors())
                        .proceed(message, null)
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

    /**
     * Returns the given {@code deadlineName} and {@code messageOrPayload} as a DeadlineMessage which expires at the
     * given {@code expiryTime}. If the {@code messageOrPayload} parameter is of type {@link Message}, a new
     * {@code DeadlineMessage} instance will be created using the payload and metadata of the given message. Otherwise,
     * the given {@code messageOrPayload} is wrapped into a {@code GenericDeadlineMessage} as its payload.
     *
     * @param deadlineName     the name for this {@link DeadlineMessage}
     * @param messageOrPayload a {@link Message} or payload to wrap as a DeadlineMessage
     * @param expiryTime       the timestamp at which the deadline expires
     * @return a DeadlineMessage using the {@code deadlineName} as its deadline name and containing the given
     * {@code messageOrPayload} as the payload
     */
    protected DeadlineMessage asDeadlineMessage(String deadlineName,
                                                @Nullable Object messageOrPayload,
                                                Instant expiryTime) {
        if (messageOrPayload instanceof Message message) {
            return new GenericDeadlineMessage(deadlineName,
                                              message,
                                              () -> expiryTime);
        }
        MessageType type = MESSAGE_TYPE_RESOLVER.resolveOrThrow(ObjectUtils.nullSafeTypeOf(messageOrPayload));
        return new GenericDeadlineMessage(
                deadlineName, new GenericMessage(type, messageOrPayload), () -> expiryTime
        );
    }

    /**
     * The calls deferred to a single {@link ProcessingContext}, in the order they were made.
     * <p>
     * Offering a call and draining all calls exclude each other. A call is therefore either part of the drained calls
     * or rejected, but never accepted after the drained calls were taken, where nothing would run it. A call can arrive
     * while the calls are drained when a context with a multithreaded work scheduler invokes a Saga from another
     * action of the {@link #RUN_DEADLINE_CALLS} phase.
     */
    private static final class DeferredCalls {

        private final List<Runnable> calls = new ArrayList<>();
        private boolean drained;

        private synchronized boolean offer(Runnable call) {
            if (drained) {
                return false;
            }
            calls.add(call);
            return true;
        }

        private synchronized List<Runnable> drain() {
            drained = true;
            return List.copyOf(calls);
        }
    }
}

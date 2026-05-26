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

package io.axoniq.framework.statecontroller.decisions;

import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.jspecify.annotations.Nullable;

import java.time.Clock;

/**
 * Internal helpers shared by the annotation-based handler enhancer ({@link StateControllerHandlerEnhancer}) and
 * the declarative {@link StateControllerComponent}.
 * <p>
 * Two responsibilities:
 * <ul>
 *     <li>{@link #decisionContextFor(ProcessingContext)} — build (and cache) a {@link DecisionContext} for the
 *         current {@link ProcessingContext}, looking up the {@link EventStore} and an optional {@link Clock}
 *         from the surrounding {@link org.axonframework.messaging.core.ApplicationContext ApplicationContext}.
 *         Caching ensures every parameter resolver, declarative handler, or downstream lookup observes the same
 *         shared loading-context for the in-flight command.</li>
 *     <li>{@link #apply(Decision, ProcessingContext)} — translate a returned {@link Decision} into side effects
 *         on the in-context {@link EventAppender}: {@link Decision.Accept} appends events and yields its
 *         {@link Decision.Accept#result()}; {@link Decision.Reject} appends audit events and throws a
 *         {@link CommandExecutionException} carrying the rejection reason as its message and the
 *         {@link Decision.Reject#auditEvents() audit events} as its
 *         {@link CommandExecutionException#getDetails() details}.</li>
 * </ul>
 * Both paths intentionally route through the same helper so the observable behaviour from a command caller's
 * point of view is identical whether the decision was wired via {@link StateController @StateController} or via
 * the declarative builder API.
 * <p>
 * Marked {@link Internal @Internal} because this class stitches together the annotation-based enhancer and the
 * declarative component; it is not a stable API for direct user invocation and may evolve as the runtime adds
 * Phase 4 / Phase 5 capabilities.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
@Internal
public final class DecisionDispatch {

    /**
     * Resource key under which the shared {@link DecisionContext} for a given {@link ProcessingContext} is
     * cached. Private because external lookup should go through {@link #decisionContextFor(ProcessingContext)}.
     */
    private static final ResourceKey<DecisionContext> DECISION_CONTEXT_KEY =
            ResourceKey.withLabel("io.axoniq.framework.statecontroller.DecisionContext");

    private DecisionDispatch() {
        // utility
    }

    /**
     * Returns the shared {@link DecisionContext} for the given {@code processingContext}, creating it on first
     * call. Subsequent invocations on the same {@code ProcessingContext} return the same instance, so all
     * conditions declared during the in-flight command participate in a single loading-context.
     *
     * @param processingContext the current processing context
     * @return the shared decision context for this processing context
     */
    public static DecisionContext decisionContextFor(ProcessingContext processingContext) {
        return processingContext.computeResourceIfAbsent(DECISION_CONTEXT_KEY,
                                                         () -> buildDecisionContext(processingContext));
    }

    /**
     * Processes the given {@code decision} against the in-context {@link EventAppender}: appends events on
     * {@link Decision.Accept Accept} (returning the optional {@link Decision.Accept#result() result}), or
     * appends audit events on {@link Decision.Reject Reject} before throwing a
     * {@link CommandExecutionException}.
     * <p>
     * On rejection the exception's {@link CommandExecutionException#getMessage() message} carries the
     * {@link Decision.Reject#reason() rejection reason} and its
     * {@link CommandExecutionException#getDetails() details} carry the
     * {@link Decision.Reject#auditEvents() audit events} (already appended to the event store at that point).
     *
     * @param decision          the decision returned by a state-controller handler
     * @param processingContext the current processing context, used to obtain the in-context {@link EventAppender}
     * @return the {@link Decision.Accept#result() result value} when the decision was an accept (may be
     *         {@code null}); never returns when the decision was a reject
     * @throws CommandExecutionException when {@code decision} is a {@link Decision.Reject}; audit events are
     *                                   appended through the in-context {@link EventAppender} before the
     *                                   exception is raised
     */
    public static @Nullable Object apply(Decision decision, ProcessingContext processingContext) {
        EventAppender appender = EventAppender.forContext(processingContext);
        return switch (decision) {
            case Decision.Accept accept -> {
                if (!accept.events().isEmpty()) {
                    appender.append(accept.events());
                }
                yield accept.result();
            }
            case Decision.Reject reject -> {
                if (!reject.auditEvents().isEmpty()) {
                    appender.append(reject.auditEvents());
                }
                throw new CommandExecutionException(reject.reason(), null, reject.auditEvents());
            }
        };
    }

    private static DecisionContext buildDecisionContext(ProcessingContext context) {
        EventStore eventStore = context.component(EventStore.class);
        Clock clock = resolveClock(context);
        return new DecisionContextImpl(eventStore, context, clock);
    }

    private static Clock resolveClock(ProcessingContext context) {
        try {
            return context.component(Clock.class);
        } catch (ComponentNotFoundException notFound) {
            return Clock.systemUTC();
        }
    }
}

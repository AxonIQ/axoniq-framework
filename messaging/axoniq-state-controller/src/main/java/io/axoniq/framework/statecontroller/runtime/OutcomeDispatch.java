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

package io.axoniq.framework.statecontroller.runtime;

import io.axoniq.framework.statecontroller.Outcome;
import io.axoniq.framework.statecontroller.UncoveredEventException;
import io.axoniq.framework.statecontroller.eventstream.ReadBoundaries;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.core.Context.ResourceKey;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.Metadata;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.messaging.eventstreaming.EventCriteria;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Internal helpers shared by the {@link OutcomeHandlerEnhancer annotation-based} and
 * {@link io.axoniq.framework.statecontroller.decisions.StateControllerComponent declarative} state-controller
 * dispatch paths.
 * <p>
 * Two responsibilities:
 * <ul>
 *     <li>{@link #sessionFor(ProcessingContext)} — build (and cache) the {@link HistorySession} for the current
 *         {@link ProcessingContext}, looking up the {@link EventStore} and an optional {@link Clock} from the
 *         surrounding {@link org.axonframework.messaging.core.ApplicationContext ApplicationContext}. Caching
 *         ensures every parameter resolver and downstream lookup observes the same loading session for the
 *         in-flight command, so all declared conditions batch into the same reads.</li>
 *     <li>{@link #apply(Outcome, ProcessingContext)} — translate a returned {@link Outcome} into side effects on
 *         the in-context {@link EventAppender}: {@link Outcome.Accept} appends events (after the DCB coverage
 *         guard) and yields its {@link Outcome.Accept#result()}; {@link Outcome.Reject} appends audit events and
 *         throws a {@link CommandExecutionException} carrying the rejection reason as its message and the
 *         {@link Outcome.Reject#auditEvents() audit events} as its
 *         {@link CommandExecutionException#getDetails() details}.</li>
 * </ul>
 * On {@link Outcome.Accept Accept}, a Dynamic Consistency Boundary (DCB) coverage guard runs before the append:
 * when the decision read at least one scope (recorded via
 * {@link io.axoniq.framework.statecontroller.eventstream.ReadBoundaries ReadBoundaries}), every accepted event
 * must fall inside one of those read boundaries, or an {@link UncoveredEventException} is thrown. This closes the
 * tagging-drift gap where an appended event tagged differently from any read scope would leave the optimistic
 * lock guarding the wrong surface.
 * <p>
 * Marked {@link Internal @Internal} because this class stitches together the enhancer, the parameter resolvers,
 * and the declarative component; it is not a stable API for direct user invocation.
 *
 * @author Allard Buijze
 * @author Stefan Dragisic
 * @since 5.2.0
 */
@Internal
public final class OutcomeDispatch {

    /**
     * Resource key under which the shared {@link HistorySession} for a given {@link ProcessingContext} is cached.
     * Private because external lookup should go through {@link #sessionFor(ProcessingContext)}.
     */
    private static final ResourceKey<HistorySession> SESSION_KEY =
            ResourceKey.withLabel("io.axoniq.framework.statecontroller.HistorySession");

    private OutcomeDispatch() {
        // utility
    }

    /**
     * Returns the shared {@link HistorySession} for the given {@code processingContext}, creating it on first
     * call. Subsequent invocations on the same {@code ProcessingContext} return the same instance, so all
     * conditions declared during the in-flight command participate in the same loading session.
     *
     * @param processingContext the current processing context
     * @return the shared loading session for this processing context
     */
    public static HistorySession sessionFor(ProcessingContext processingContext) {
        return processingContext.computeResourceIfAbsent(SESSION_KEY, () -> buildSession(processingContext));
    }

    /**
     * Processes the given {@code outcome} against the in-context {@link EventAppender}: appends events on
     * {@link Outcome.Accept Accept} (returning the optional {@link Outcome.Accept#result() result}), or appends
     * audit events on {@link Outcome.Reject Reject} before throwing a {@link CommandExecutionException}.
     * <p>
     * On acceptance, when the decision read at least one scope, every accepted event must be covered by one of
     * those read consistency boundaries (the DCB coverage guard); audit events appended on rejection are never
     * guarded.
     *
     * @param outcome           the outcome returned by a state-controlled handler
     * @param processingContext the current processing context, used to obtain the in-context {@link EventAppender}
     * @return the {@link Outcome.Accept#result() result value} when the outcome was an accept (may be
     *         {@code null}); never returns when the outcome was a reject
     * @throws CommandExecutionException when {@code outcome} is an {@link Outcome.Reject}; audit events are
     *                                   appended through the in-context {@link EventAppender} before the
     *                                   exception is raised
     * @throws UncoveredEventException   when {@code outcome} is an {@link Outcome.Accept} that read at least one
     *                                   scope yet emits an event no read boundary covers
     */
    public static @Nullable Object apply(Outcome outcome, ProcessingContext processingContext) {
        EventAppender appender = EventAppender.forContext(processingContext);
        return switch (outcome) {
            case Outcome.Accept accept -> {
                if (!accept.events().isEmpty()) {
                    // Wrap each accepted payload into an EventMessage once, mirroring the appender's own wrapping,
                    // so the coverage guard computes the exact tags the append will use, then hand the same wrapped
                    // messages to append(...) (which short-circuits on already-wrapped EventMessages).
                    List<EventMessage> events = wrap(accept.events(), processingContext);
                    requireEventsCovered(events, processingContext);
                    appender.append(events);
                }
                yield accept.result();
            }
            case Outcome.Reject reject -> {
                if (!reject.auditEvents().isEmpty()) {
                    appender.append(reject.auditEvents());
                }
                throw new CommandExecutionException(reject.reason(), null, reject.auditEvents());
            }
        };
    }

    /**
     * Enforces the Dynamic Consistency Boundary (DCB) coverage guard: every accepted event must fall inside at
     * least one consistency boundary the decision read.
     * <p>
     * No-op when the decision read no scope — an unconditional append (for example a creation) is a legitimate
     * choice — or when no {@link TagResolver} is configured, in which case the module has no contract to enforce
     * the appended-event tagging against. Otherwise each event's {@link QualifiedName} and tags are resolved with
     * the same components the append uses, and an {@link UncoveredEventException} is thrown for the first event
     * no recorded boundary {@link EventCriteria#matches(QualifiedName, Set) matches}.
     */
    private static void requireEventsCovered(List<EventMessage> events, ProcessingContext processingContext) {
        List<EventCriteria> readCriteria = ReadBoundaries.readCriteria(processingContext);
        if (readCriteria.isEmpty()) {
            return;
        }
        TagResolver tagResolver = resolveTagResolver(processingContext);
        if (tagResolver == null) {
            return;
        }
        for (EventMessage event : events) {
            QualifiedName name = event.type().qualifiedName();
            Set<Tag> tags = tagResolver.resolve(event);
            boolean covered = readCriteria.stream().anyMatch(criterion -> criterion.matches(name, tags));
            if (!covered) {
                throw new UncoveredEventException(event.payloadType(), name);
            }
        }
    }

    /**
     * Wraps each payload into an {@link EventMessage}, mirroring the in-context {@link EventAppender}'s own
     * wrapping so the tags and {@link QualifiedName} the guard computes equal those the append produces: a payload
     * already an {@link EventMessage} is used verbatim, a {@link Message} is re-wrapped as a
     * {@link GenericEventMessage}, and any other payload is wrapped with the
     * {@link org.axonframework.messaging.core.MessageType MessageType} resolved through the in-context
     * {@link MessageTypeResolver}.
     */
    private static List<EventMessage> wrap(List<?> payloads, ProcessingContext processingContext) {
        MessageTypeResolver typeResolver = processingContext.component(MessageTypeResolver.class);
        List<EventMessage> events = new ArrayList<>(payloads.size());
        for (Object payload : payloads) {
            if (payload instanceof EventMessage eventMessage) {
                events.add(eventMessage);
            } else if (payload instanceof Message message) {
                events.add(new GenericEventMessage(message));
            } else {
                events.add(new GenericEventMessage(typeResolver.resolveOrThrow(payload),
                                                   payload,
                                                   Metadata.emptyInstance()));
            }
        }
        return events;
    }

    private static @Nullable TagResolver resolveTagResolver(ProcessingContext context) {
        try {
            return context.component(TagResolver.class);
        } catch (ComponentNotFoundException notFound) {
            return null;
        }
    }

    private static HistorySession buildSession(ProcessingContext context) {
        EventStore eventStore = context.component(EventStore.class);
        return new HistorySession(eventStore, context, resolveClock(context));
    }

    private static Clock resolveClock(ProcessingContext context) {
        try {
            return context.component(Clock.class);
        } catch (ComponentNotFoundException notFound) {
            return Clock.systemUTC();
        }
    }
}

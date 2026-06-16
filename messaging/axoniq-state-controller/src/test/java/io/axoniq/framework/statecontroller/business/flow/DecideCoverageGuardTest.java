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

package io.axoniq.framework.statecontroller.business.flow;

import io.axoniq.framework.statecontroller.decisions.Decide;
import io.axoniq.framework.statecontroller.decisions.Decision;
import io.axoniq.framework.statecontroller.decisions.UncoveredEventException;
import io.axoniq.framework.statecontroller.history.History;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.annotation.AnnotatedHandlerInspector;
import org.axonframework.messaging.core.annotation.MessageHandlingMember;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the Dynamic Consistency Boundary (DCB) coverage guard on the {@code @Decide} accept path: an accepted event
 * must fall inside at least one consistency boundary the decision read through {@link History}, otherwise the
 * optimistic lock would guard the wrong surface and concurrent commands could both commit.
 * <p>
 * The harness mirrors {@code DecideAnnotationFlowTest}: a {@link StorageEngineBackedEventStore} tagged through an
 * {@link AnnotationBasedTagResolver} (the same instance registered as the {@link TagResolver} component the guard
 * resolves), so the tags the guard computes equal the tags the append commits. Each fixture is a tiny
 * self-contained decision exercising one branch of the guard.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class DecideCoverageGuardTest {

    private EventStore eventStore;
    private ProcessingContext processingContext;
    private List<EventMessage> capturedAppends;

    @BeforeEach
    void setUp() {
        InMemoryEventStorageEngine engine = new InMemoryEventStorageEngine();
        TagResolver tagResolver = new AnnotationBasedTagResolver();
        eventStore = new StorageEngineBackedEventStore(engine, new SimpleEventBus(), tagResolver);
        MessageTypeResolver typeResolver = new ClassBasedMessageTypeResolver();
        processingContext = new StubProcessingContext(new ApplicationContext() {
            @SuppressWarnings("unchecked")
            @Override
            public <C> C component(Class<C> type, @Nullable String name) {
                if (type == EventStore.class || type == EventSink.class) {
                    return (C) eventStore;
                }
                if (type == MessageTypeResolver.class) {
                    return (C) typeResolver;
                }
                if (type == TagResolver.class) {
                    return (C) tagResolver;
                }
                throw new ComponentNotFoundException(type, name);
            }
        });
        capturedAppends = new ArrayList<>();
        eventStore.transaction(processingContext).onAppend(capturedAppends::add);
    }

    @Nested
    class Fires {

        @Test
        void anAcceptedEventTaggedToADifferentScopeThanWasReadIsRejected() {
            // given — a decision reading history.of("account", id) but emitting an event tagged on a foreign key
            MessageHandlingMember<? super DriftingScope> handler = singleHandlerOf(DriftingScope.class);

            // when / then — the guard rejects the drift, naming the offending event and advising @EventTag
            assertThatThrownBy(() -> invoke(handler, new DriftingScope(), new Act("a1")))
                    .isInstanceOf(UncoveredEventException.class)
                    .hasMessageContaining(WronglyTagged.class.getName())
                    .hasMessageContaining("@EventTag");
            // and — nothing was appended for the uncovered command
            assertThat(capturedAppends).isEmpty();
        }

        @Test
        void anAcceptedEventWithNoTagsAfterAScopedReadIsRejected() {
            // given — a decision reading a tag scope but emitting a completely untagged event
            MessageHandlingMember<? super UntaggedEmit> handler = singleHandlerOf(UntaggedEmit.class);

            // when / then — the untagged event is covered by no tag-scoped boundary
            assertThatThrownBy(() -> invoke(handler, new UntaggedEmit(), new Act("a1")))
                    .isInstanceOf(UncoveredEventException.class)
                    .hasMessageContaining(Untagged.class.getName());
            assertThat(capturedAppends).isEmpty();
        }
    }

    @Nested
    class Passes {

        @Test
        void anAcceptedEventTaggedToMatchAReadScopeIsAppended() {
            // given — a decision reading history.of("account", id) and emitting an event tagged account=id
            MessageHandlingMember<? super CoveredScope> handler = singleHandlerOf(CoveredScope.class);

            // when — the covered event passes the guard
            Object result = invoke(handler, new CoveredScope(), new Act("a1"));

            // then — the event was appended through the transaction
            assertThat(result).isNull();
            assertThat(payloadsOf(CorrectlyTagged.class))
                    .singleElement()
                    .satisfies(e -> assertThat(e.accountId()).isEqualTo("a1"));
        }

        @Test
        void aTaglessTypeFirstReadCoversTheEmittedEventRegardlessOfItsTags() {
            // given — a decision reading history.of(CorrectlyTagged.class) (a tagless, type-first scope)
            MessageHandlingMember<? super TypeFirstScope> handler = singleHandlerOf(TypeFirstScope.class);

            // when — the emitted CorrectlyTagged matches the read type across all tags
            Object result = invoke(handler, new TypeFirstScope(), new Act("a1"));

            // then — the type-first scope covers it, so the append proceeds
            assertThat(result).isNull();
            assertThat(payloadsOf(CorrectlyTagged.class)).hasSize(1);
        }

        @Test
        void anUnconditionalAcceptThatReadNoHistoryIsNotGuarded() {
            // given — a decision that never reads any History scope and emits an untagged event
            MessageHandlingMember<? super NoRead> handler = singleHandlerOf(NoRead.class);

            // when — with no recorded read boundaries the guard stays disabled (legitimate creation append)
            Object result = invoke(handler, new NoRead(), new Act("a1"));

            // then — the untagged event is appended despite carrying no tags
            assertThat(result).isNull();
            assertThat(payloadsOf(Untagged.class)).hasSize(1);
        }

        @Test
        void rejectAuditEventsAreNotGuardedEvenAfterAScopedRead() {
            // given — a decision reading a scope then rejecting with an untagged audit event
            MessageHandlingMember<? super RejectWithAudit> handler = singleHandlerOf(RejectWithAudit.class);

            // when / then — the rejection surfaces as a CommandExecutionException, not an UncoveredEventException
            assertThatThrownBy(() -> invoke(handler, new RejectWithAudit(), new Act("a1")))
                    .isInstanceOf(CommandExecutionException.class)
                    .hasMessageContaining("denied");
            // and — the untagged audit event was appended unconditionally
            assertThat(payloadsOf(Untagged.class)).hasSize(1);
        }
    }

    // ----------------------------------------------------------------------
    // Harness
    // ----------------------------------------------------------------------

    private <T> MessageHandlingMember<? super T> singleHandlerOf(Class<T> type) {
        var handlers = AnnotatedHandlerInspector.inspectType(type).getHandlers(type);
        assertThat(handlers).hasSize(1);
        return handlers.iterator().next();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private @Nullable Object invoke(MessageHandlingMember handler, Object target, Object commandPayload) {
        CommandMessage command = new GenericCommandMessage(new MessageType(commandPayload.getClass()),
                                                           commandPayload);
        MessageStream<?> stream = handler.handle(command, processingContext, target);
        CompletableFuture<? extends MessageStream.Entry<?>> future = stream.first().asCompletableFuture();
        try {
            MessageStream.Entry<?> entry = future.join();
            return entry == null ? null : entry.message().payload();
        } catch (CompletionException ce) {
            Throwable cause = ce.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw new RuntimeException(cause);
        }
    }

    @SuppressWarnings("unchecked")
    private <E> List<E> payloadsOf(Class<E> type) {
        return capturedAppends.stream()
                              .map(EventMessage::payload)
                              .filter(type::isInstance)
                              .map(p -> (E) p)
                              .toList();
    }

    // ----------------------------------------------------------------------
    // Fixtures
    // ----------------------------------------------------------------------

    record Act(String accountId) {

    }

    record CorrectlyTagged(@EventTag(key = "account") String accountId) {

    }

    record WronglyTagged(@EventTag(key = "ledger") String ledgerId) {

    }

    record Untagged(String accountId) {

    }

    /** Reads the {@code account} scope but emits an event tagged on a foreign {@code ledger} key. */
    public static class DriftingScope {

        @Decide
        public Decision act(Act cmd, History history) {
            history.of("account", cmd.accountId()).has(CorrectlyTagged.class);
            return Decision.accept(new WronglyTagged(cmd.accountId()));
        }
    }

    /** Reads the {@code account} scope but emits an event carrying no tags at all. */
    public static class UntaggedEmit {

        @Decide
        public Decision act(Act cmd, History history) {
            history.of("account", cmd.accountId()).has(CorrectlyTagged.class);
            return Decision.accept(new Untagged(cmd.accountId()));
        }
    }

    /** Reads the {@code account} scope and emits an event tagged into that same scope. */
    public static class CoveredScope {

        @Decide
        public Decision act(Act cmd, History history) {
            history.of("account", cmd.accountId()).has(CorrectlyTagged.class);
            return Decision.accept(new CorrectlyTagged(cmd.accountId()));
        }
    }

    /** Reads a tagless, type-first scope; any event of that type is covered regardless of its tags. */
    public static class TypeFirstScope {

        @Decide
        public Decision act(Act cmd, History history) {
            history.of(CorrectlyTagged.class).has(CorrectlyTagged.class);
            return Decision.accept(new CorrectlyTagged(cmd.accountId()));
        }
    }

    /** Reads no History scope at all; an unconditional append is a legitimate, unguarded choice. */
    public static class NoRead {

        @Decide
        public Decision act(Act cmd, History history) {
            return Decision.accept(new Untagged(cmd.accountId()));
        }
    }

    /** Reads a scope then rejects with an untagged audit event; audit events are never guarded. */
    public static class RejectWithAudit {

        @Decide
        public Decision act(Act cmd, History history) {
            history.of("account", cmd.accountId()).has(CorrectlyTagged.class);
            return Decision.reject("denied").recording(new Untagged(cmd.accountId()));
        }
    }
}

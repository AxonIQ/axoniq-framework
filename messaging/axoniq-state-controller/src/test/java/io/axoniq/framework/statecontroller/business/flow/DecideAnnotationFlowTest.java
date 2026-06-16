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
import io.axoniq.framework.statecontroller.decisions.DecideHandlerEnhancer;
import io.axoniq.framework.statecontroller.decisions.Decision;
import io.axoniq.framework.statecontroller.history.History;
import io.axoniq.framework.statecontroller.history.HistoryParameterResolverFactory;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.annotation.EventTag;
import org.axonframework.eventsourcing.eventstore.AnnotationBasedTagResolver;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine.AppendTransaction;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TagResolver;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
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
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.EventSink;
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end test for the business-first {@code @Decide} / {@link History} decision API: it pins the annotation
 * discovery + handler-enhancer + parameter-resolver flow.
 * <p>
 * Specifically, this exercises the wiring that makes {@link Decide @Decide} methods first-class command handlers:
 * {@link AnnotatedHandlerInspector} discovers the method through its meta-annotated {@code @CommandHandler}, the
 * {@link DecideHandlerEnhancer} (loaded via ServiceLoader) wraps the resulting {@link MessageHandlingMember}, and
 * the {@link HistoryParameterResolverFactory} (also loaded via ServiceLoader) injects a {@link History} resolved
 * against the surrounding {@link ProcessingContext}. On invocation, the enhancer translates the returned
 * {@link Decision} into event appends or a {@link CommandExecutionException} so the command caller observes
 * outcomes through standard AF5 semantics.
 * <p>
 * The decision fixture ({@link OpenAccounts}) is self-contained in this class so the focus stays on the wiring
 * rather than on any shared sample domain.
 *
 * @author Stefan Dragisic
 * @since 5.2.0
 */
class DecideAnnotationFlowTest {

    private InMemoryEventStorageEngine engine;
    private EventStore eventStore;
    private ProcessingContext processingContext;
    private List<EventMessage> capturedAppends;
    private MessageHandlingMember<? super OpenAccounts> openHandler;

    @BeforeEach
    void setUp() {
        engine = new InMemoryEventStorageEngine();
        // The event store tags appended events through the SAME TagResolver the coverage guard resolves from the
        // processing context, mirroring the production wiring (EventSourcingConfigurationDefaults builds the store
        // from the registered TagResolver). AnnotationBasedTagResolver reads @EventTag off each event's fields.
        TagResolver tagResolver = new AnnotationBasedTagResolver();
        eventStore = new StorageEngineBackedEventStore(engine, new SimpleEventBus(), tagResolver);
        MessageTypeResolver typeResolver = new ClassBasedMessageTypeResolver();
        processingContext = new StubProcessingContext(new ApplicationContext() {
            @SuppressWarnings("unchecked")
            @Override
            public <C> C component(Class<C> type, @Nullable String name) {
                if (type == EventStore.class) {
                    return (C) eventStore;
                }
                if (type == EventSink.class) {
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
        // The enhancer uses EventAppender.forContext(pc), which appends through the same EventStoreTransaction.
        // Capturing here gives us a single point of truth for what the enhancer told the framework to write.
        eventStore.transaction(processingContext).onAppend(capturedAppends::add);
        openHandler = singleHandlerOf(OpenAccounts.class);
    }

    private <T> MessageHandlingMember<? super T> singleHandlerOf(Class<T> type) {
        var inspector = AnnotatedHandlerInspector.inspectType(type);
        var handlers = inspector.getHandlers(type);
        assertThat(handlers).hasSize(1);
        return handlers.iterator().next();
    }

    private <P> void seed(P payload, Set<Tag> tags) {
        var message = new GenericEventMessage(new MessageType(payload.getClass()), payload);
        TaggedEventMessage<?> tagged = new GenericTaggedEventMessage<>(message, tags);
        @SuppressWarnings("unchecked")
        AppendTransaction<ConsistencyMarker> tx = (AppendTransaction<ConsistencyMarker>)
                engine.appendEvents(AppendCondition.none(), null, List.of(tagged)).join();
        tx.commit().thenCompose(tx::afterCommit).join();
    }

    private static CommandMessage commandFor(Object payload) {
        return new GenericCommandMessage(new MessageType(payload.getClass()), payload);
    }

    @Nested
    class Discovery {

        @Test
        void inspectorDiscoversTheDecideMethodThroughItsMetaAnnotatedCommandHandler() {
            // given — a class with a single @Decide method, discovered in setUp()

            // when — the underlying member is unwrapped to its reflective method
            Method method = openHandler.unwrap(Method.class).orElseThrow();

            // then — the discovered handler is the @Decide method, picked up via @CommandHandler meta-annotation
            assertThat(method.getName()).isEqualTo("open");
            assertThat(method.isAnnotationPresent(Decide.class)).isTrue();
        }

        @Test
        void decideHandlerEnhancerWrapsTheDiscoveredHandler() {
            // given — the raw, un-enhanced member for the @Decide method
            MessageHandlingMember<? super OpenAccounts> raw =
                    AnnotatedHandlerInspector.inspectType(OpenAccounts.class)
                                             .getHandlers(OpenAccounts.class)
                                             .iterator()
                                             .next();

            // when — the module's enhancer (also applied during real discovery) wraps it
            MessageHandlingMember<? super OpenAccounts> wrapped =
                    new DecideHandlerEnhancer().wrapHandler(raw);

            // then — the enhancer produced a distinct wrapping member, not the original delegate
            assertThat(wrapped).isNotSameAs(raw);
        }

        @Test
        void historyParameterResolverFactoryInjectsHistoryForTheHistoryParameter() {
            // given — the reflective @Decide method whose second parameter is a History
            Method method = openHandler.unwrap(Method.class).orElseThrow();

            // when — the factory is asked to resolve the History parameter (index 1)
            ParameterResolver<?> resolver = new HistoryParameterResolverFactory()
                    .createInstance(method, method.getParameters(), 1);

            // then — a resolver is produced and yields a (root, unbound) History from the processing context
            assertThat(resolver).isNotNull();
            assertThat(resolver.resolveParameterValue(processingContext).join()).isInstanceOf(History.class);
        }

        @Test
        void historyParameterResolverFactoryIgnoresNonHistoryParameters() {
            // given — the reflective @Decide method whose first parameter is the command payload
            Method method = openHandler.unwrap(Method.class).orElseThrow();

            // when — the factory is asked to resolve the command parameter (index 0)
            ParameterResolver<?> resolver = new HistoryParameterResolverFactory()
                    .createInstance(method, method.getParameters(), 0);

            // then — the factory declines, leaving the payload resolution to the framework
            assertThat(resolver).isNull();
        }
    }

    @Nested
    class Accept {

        @Test
        void acceptedDecisionAppendsTheEventAndCompletesWithoutValueWhenNoResultIsDeclared() {
            // given — an empty history for the requested account; opening is allowed

            // when — dispatch the OpenAccount command via the wrapped handler
            Object result = invoke(openHandler, new OpenAccounts(), new OpenAccount("a1"));

            // then — the handler completed with no return value
            assertThat(result).isNull();
            // and — the enhancer appended an AccountOpened through the EventStoreTransaction
            assertThat(payloadsOf(AccountOpened.class))
                    .singleElement()
                    .satisfies(o -> assertThat(o.accountId()).isEqualTo("a1"));
        }
    }

    @Nested
    class Reject {

        @Test
        void rejectedDecisionThrowsCommandExecutionExceptionWithReasonWithoutAppending() {
            // given — the account already exists, so a second open must be rejected
            seed(new AccountOpened("a1"), Set.of(new Tag("account", "a1")));

            // when / then — the History read observes the prior AccountOpened and the decision rejects
            assertThatThrownBy(() -> invoke(openHandler, new OpenAccounts(), new OpenAccount("a1")))
                    .isInstanceOf(CommandExecutionException.class)
                    .hasMessageContaining("account already exists");
            // and — nothing was appended for the rejected command (the seeded event is not captured here)
            assertThat(payloadsOf(AccountOpened.class)).isEmpty();
        }
    }

    @Nested
    class ParameterResolution {

        @Test
        void historyIsInjectedAndScopedReadsObserveOnlyTheNarrowedScope() {
            // given — an AccountOpened for a different account so the "a1" scope is observed as empty
            seed(new AccountOpened("other"), Set.of(new Tag("account", "other")));

            // when — the handler body narrows history.of("account", "a1") and reads it; without the parameter
            //        resolver wiring this would fail before any business logic runs
            Object result = invoke(openHandler, new OpenAccounts(), new OpenAccount("a1"));

            // then — the decision accepts because "a1" has no AccountOpened, proving the read was scope-narrowed
            assertThat(result).isNull();
            assertThat(payloadsOf(AccountOpened.class))
                    .singleElement()
                    .satisfies(o -> assertThat(o.accountId()).isEqualTo("a1"));
        }
    }

    // ----------------------------------------------------------------------
    // Invocation helper
    // ----------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "rawtypes"})
    private @Nullable Object invoke(MessageHandlingMember handler, Object target, Object commandPayload) {
        CommandMessage command = commandFor(commandPayload);
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
                              .map(em -> em.payload())
                              .filter(type::isInstance)
                              .map(p -> (E) p)
                              .toList();
    }

    // ----------------------------------------------------------------------
    // Test fixtures — a self-contained "open account" decision over the @Decide / History surface.
    // ----------------------------------------------------------------------

    record OpenAccount(String accountId) {

    }

    record AccountOpened(@EventTag(key = "account") String accountId) {

    }

    /**
     * A minimal decision fixture: a single {@link Decide @Decide} method reading a narrowed {@link History} and
     * either accepting with an {@code AccountOpened} or rejecting when the account already exists. Kept inside this
     * test class so the focus stays on the {@code @Decide} / {@link History} wiring.
     */
    public static class OpenAccounts {

        @Decide
        public Decision open(OpenAccount cmd, History history) {
            History account = history.of("account", cmd.accountId());
            if (account.has(AccountOpened.class)) {
                return Decision.reject("account already exists");
            }
            return Decision.accept(new AccountOpened(cmd.accountId()));
        }
    }
}

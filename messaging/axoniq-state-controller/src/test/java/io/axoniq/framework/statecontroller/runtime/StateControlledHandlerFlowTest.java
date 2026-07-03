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

import io.axoniq.framework.statecontroller.conditions.Condition;
import io.axoniq.framework.statecontroller.History;
import io.axoniq.framework.statecontroller.Outcome;
import io.axoniq.framework.statecontroller.sample.banking.AccountClosed;
import io.axoniq.framework.statecontroller.sample.banking.Accounts;
import io.axoniq.framework.statecontroller.sample.banking.MoneyDeposited;
import io.axoniq.framework.statecontroller.sample.banking.MoneyWithdrawn;
import io.axoniq.framework.statecontroller.sample.banking.Withdraw;
import org.axonframework.common.configuration.ComponentNotFoundException;
import org.axonframework.eventsourcing.eventstore.AppendCondition;
import org.axonframework.eventsourcing.eventstore.ConsistencyMarker;
import org.axonframework.eventsourcing.eventstore.EventStorageEngine.AppendTransaction;
import org.axonframework.eventsourcing.eventstore.EventStore;
import org.axonframework.eventsourcing.eventstore.GenericTaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.StorageEngineBackedEventStore;
import org.axonframework.eventsourcing.eventstore.TaggedEventMessage;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.messaging.commandhandling.CommandExecutionException;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
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
import org.axonframework.messaging.eventhandling.GenericEventMessage;
import org.axonframework.messaging.eventhandling.SimpleEventBus;
import org.axonframework.messaging.eventstreaming.Tag;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.axoniq.framework.statecontroller.Outcome.accept;
import static io.axoniq.framework.statecontroller.Outcome.reject;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end flow test for the annotation-less dispatch path: plain {@link CommandHandler @CommandHandler}
 * methods opt into the State Controller purely through their signature — a {@link History} parameter and an
 * {@link Outcome} (or {@code CompletableFuture<Outcome>}) return type. The {@link OutcomeHandlerEnhancer} and
 * {@link HistoryParameterResolverFactory} are discovered via ServiceLoader by
 * {@link AnnotatedHandlerInspector#inspectType(Class)}, exactly as in a real configuration.
 * <p>
 * The {@code Withdraw} flow exercises the imperative {@link Condition#resolve() resolve()} style through the
 * {@link Accounts} sample; the {@code RegisterAccount} flow exercises the declarative
 * {@code combine(...).resolveAsync()} style with a {@code CompletableFuture<Outcome>} return type.
 */
class StateControlledHandlerFlowTest {

    private InMemoryEventStorageEngine engine;
    private EventStore eventStore;
    private ProcessingContext processingContext;
    private List<EventMessage> capturedAppends;
    private MessageHandlingMember<? super Accounts> withdrawHandler;
    private MessageHandlingMember<? super RegisterAccounts> registerHandler;

    @BeforeEach
    void setUp() {
        engine = new InMemoryEventStorageEngine();
        eventStore = new StorageEngineBackedEventStore(engine, new SimpleEventBus(), e -> Set.of());
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
                throw new ComponentNotFoundException(type, name);
            }
        });
        capturedAppends = new ArrayList<>();
        // The enhancer uses EventAppender.forContext(pc), which appends through the same EventStoreTransaction.
        // Capturing here gives us a single point of truth for what the enhancer told the framework to write.
        eventStore.transaction(processingContext).onAppend(capturedAppends::add);
        withdrawHandler = handlerFor(Accounts.class, Withdraw.class);
        registerHandler = handlerFor(RegisterAccounts.class, RegisterAccount.class);
    }

    private <T> MessageHandlingMember<? super T> handlerFor(Class<T> type, Class<?> payloadType) {
        var inspector = AnnotatedHandlerInspector.inspectType(type);
        return inspector.getHandlers(type)
                        .stream()
                        .filter(h -> h.canHandleType(payloadType))
                        .findFirst()
                        .orElseThrow(() -> new AssertionError(
                                "no handler for " + payloadType.getSimpleName() + " on " + type.getSimpleName()));
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
    class Accept {

        @Test
        void acceptedOutcomeAppendsEventsAndCompletesWithoutValueWhenNoResultIsDeclared() {
            // given — a healthy account with sufficient funds
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(200)), Set.of(new Tag("account", "a1")));

            // when — dispatch the Withdraw command via the wrapped handler
            Object result = invoke(withdrawHandler, new Accounts(), new Withdraw("a1", BigDecimal.valueOf(50)));

            // then — the handler completed with no return value
            assertThat(result).isNull();
            // and — the enhancer appended a MoneyWithdrawn through the EventStoreTransaction
            assertThat(payloadsOf(MoneyWithdrawn.class))
                    .singleElement()
                    .satisfies(w -> assertThat(w.amount()).isEqualByComparingTo("50"));
        }

        @Test
        void asyncAcceptedOutcomeWithReturningSurfacesTheResultValueToTheCaller() {
            // given — empty stream; register accepts immediately

            // when — the async CompletableFuture<Outcome> handler flows through the same enhancer
            Object result = invoke(registerHandler, new RegisterAccounts(),
                                   new RegisterAccount("a-new"));

            // then — accept(...).returning("ACC-a-new") flows back through the enhancer
            assertThat(result).isEqualTo("ACC-a-new");
            assertThat(payloadsOf(AccountOpened.class))
                    .singleElement()
                    .satisfies(o -> assertThat(o.accountId()).isEqualTo("a-new"));
        }
    }

    @Nested
    class Reject {

        @Test
        void rejectedOutcomeThrowsCommandExecutionExceptionWithReason() {
            // given — an account with insufficient funds
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)), Set.of(new Tag("account", "a1")));

            // when / then
            assertThatThrownBy(() -> invoke(withdrawHandler,
                                            new Accounts(),
                                            new Withdraw("a1", BigDecimal.valueOf(100))))
                    .isInstanceOf(CommandExecutionException.class)
                    .hasMessageContaining("insufficient funds");
            // and — no MoneyWithdrawn event was appended
            assertThat(payloadsOf(MoneyWithdrawn.class)).isEmpty();
        }

        @Test
        void asyncRejectedOutcomeAppendsAuditEventsThroughTheSameAppender() {
            // given — a closed account; the async register handler rejects with an audit event
            seed(new AccountClosed("blocked"), Set.of(new Tag("account", "blocked")));

            // when / then
            CommandExecutionException ex = assertThatThrownBy(() -> invoke(registerHandler,
                                                                           new RegisterAccounts(),
                                                                           new RegisterAccount("blocked")))
                    .isInstanceOf(CommandExecutionException.class)
                    .hasMessageContaining("account is closed")
                    .extracting(t -> (CommandExecutionException) t)
                    .actual();

            // then — the audit event was appended through the EventAppender before the exception fired
            assertThat(payloadsOf(AuditedRejection.class))
                    .singleElement()
                    .satisfies(a -> {
                        assertThat(a.accountId()).isEqualTo("blocked");
                        assertThat(a.reason()).isEqualTo("closed");
                    });
            // and — the exception surfaces the audit events through getDetails()
            assertThat(ex.<List<Object>>getDetails().orElse(List.of()))
                    .singleElement()
                    .isInstanceOf(AuditedRejection.class);
        }
    }

    @Nested
    class ParameterResolution {

        @Test
        void historyIsInjectedAndScopedReadsWork() {
            // given — events for a different account so the handler observes an empty scope for "a1"
            seed(new MoneyDeposited("other", BigDecimal.valueOf(500)), Set.of(new Tag("account", "other")));

            // when — the handler's body reads history.of("account", "a1") to evaluate balance; without the
            //        parameter resolver wiring this would fail before any business logic runs
            assertThatThrownBy(() -> invoke(withdrawHandler,
                                            new Accounts(),
                                            new Withdraw("a1", BigDecimal.valueOf(1))))
                    .isInstanceOf(CommandExecutionException.class)
                    .hasMessageContaining("insufficient funds");
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
    // Test fixtures
    // ----------------------------------------------------------------------

    record RegisterAccount(String accountId) {

    }

    record AccountOpened(String accountId) {

    }

    record AuditedRejection(String accountId, String reason) {

    }

    /**
     * A second state-controlled handler covering the {@code CompletableFuture<Outcome>} return type, the
     * declarative {@code combine(...).resolveAsync()} style, the {@link Accept} returning-value case, and the
     * {@link Reject} audit-events case. Kept inside this test class so the focus stays on the wiring.
     */
    public static class RegisterAccounts {

        @CommandHandler
        public CompletableFuture<Outcome> register(RegisterAccount cmd, History history) {
            History account = history.of("account", cmd.accountId());
            var closed = account.has(AccountClosed.class);
            var exists = account.has(AccountOpened.class);

            Condition<Outcome> decision = closed.combine(exists, (isClosed, doesExist) -> {
                if (isClosed) {
                    return reject("account is closed")
                            .recording(new AuditedRejection(cmd.accountId(), "closed"));
                }
                if (doesExist) {
                    return reject("already exists");
                }
                return accept(new AccountOpened(cmd.accountId())).returning("ACC-" + cmd.accountId());
            });
            return decision.resolveAsync();
        }
    }
}

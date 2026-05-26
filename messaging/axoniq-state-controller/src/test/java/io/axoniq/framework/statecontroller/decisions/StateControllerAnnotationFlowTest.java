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

import io.axoniq.framework.statecontroller.eventstream.EventStream;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 3 end-to-end test: pins the annotation discovery + handler-enhancer + parameter-resolver flow.
 * <p>
 * Specifically, this exercises the wiring that makes {@code @StateController} methods first-class command
 * handlers: {@link AnnotatedHandlerInspector} discovers the method through its meta-annotated
 * {@code @CommandHandler}, the {@link StateControllerHandlerEnhancer} (loaded via ServiceLoader) wraps the
 * resulting {@link MessageHandlingMember}, and the {@link DecisionContextParameterResolverFactory} (also loaded
 * via ServiceLoader) injects a {@link DecisionContext} resolved against the surrounding
 * {@link ProcessingContext}. On invocation, the enhancer translates the returned {@link Decision} into event
 * appends or a {@link CommandExecutionException} so the command caller observes outcomes through standard
 * AF5 semantics.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
class StateControllerAnnotationFlowTest {

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
        withdrawHandler = singleHandlerOf(Accounts.class);
        registerHandler = singleHandlerOf(RegisterAccounts.class);
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
    class Accept {

        @Test
        void acceptedDecisionAppendsEventsAndCompletesWithoutValueWhenNoResultIsDeclared() {
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
        void acceptedDecisionWithReturningSurfacesTheResultValueToTheCaller() {
            // given — empty stream; register accepts immediately

            // when
            Object result = invoke(registerHandler, new RegisterAccounts(),
                                   new RegisterAccount("a-new"));

            // then — Decision.emit(...).returning("ACC-a-new") flows back through the enhancer
            assertThat(result).isEqualTo("ACC-a-new");
            assertThat(payloadsOf(AccountOpened.class))
                    .singleElement()
                    .satisfies(o -> assertThat(o.accountId()).isEqualTo("a-new"));
        }
    }

    @Nested
    class Reject {

        @Test
        void rejectedDecisionThrowsCommandExecutionExceptionWithReason() {
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
        void rejectedDecisionAppendsAuditEventsThroughTheSameAppender() {
            // given — a closed account; the register handler rejects with an audit event
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
        void decisionContextIsInjectedAndScopedReadsWork() {
            // given — events for a different account so the handler observes an empty scope for "a1"
            seed(new MoneyDeposited("other", BigDecimal.valueOf(500)), Set.of(new Tag("account", "other")));

            // when — the handler's body reads ctx.scope("account", "a1") to evaluate balance; without the
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
     * A second state controller covering the {@link Accept} returning-value case and the {@link Reject}
     * audit-events case. Kept inside this test class so the focus stays on the wiring.
     */
    public static class RegisterAccounts {

        @StateController
        public Decision register(RegisterAccount cmd, DecisionContext ctx) {
            EventStream account = ctx.scope("account", cmd.accountId());
            // Declare all conditions before forcing any — per the Phase 2 contract.
            var closed = account.contains(AccountClosed.class);
            var exists = account.contains(AccountOpened.class);

            if (closed.isTrue()) {
                return Decision.reject("account is closed")
                               .recording(new AuditedRejection(cmd.accountId(), "closed"));
            }
            if (exists.isTrue()) {
                return Decision.reject("already exists");
            }
            return Decision.emit(new AccountOpened(cmd.accountId())).returning("ACC-" + cmd.accountId());
        }
    }
}

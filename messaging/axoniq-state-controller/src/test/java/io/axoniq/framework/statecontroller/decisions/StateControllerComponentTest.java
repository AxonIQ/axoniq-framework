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
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.NoHandlerForCommandException;
import org.axonframework.messaging.core.ApplicationContext;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Phase 3 declarative path test: pins the {@link StateControllerComponent} fluent registration API. The
 * observable semantics from a command caller's perspective are identical to the annotation flow exercised by
 * {@link StateControllerAnnotationFlowTest}; this suite verifies that the declarative builder routes commands,
 * processes decisions, surfaces results, throws on reject, and integrates correctly with custom
 * {@link MessageTypeResolver}s.
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
class StateControllerComponentTest {

    private InMemoryEventStorageEngine engine;
    private EventStore eventStore;
    private ProcessingContext processingContext;
    private MessageTypeResolver typeResolver;
    private List<EventMessage> capturedAppends;

    @BeforeEach
    void setUp() {
        engine = new InMemoryEventStorageEngine();
        eventStore = new StorageEngineBackedEventStore(engine, new SimpleEventBus(), e -> Set.of());
        typeResolver = new ClassBasedMessageTypeResolver();
        processingContext = newProcessingContext(typeResolver);
        capturedAppends = new ArrayList<>();
        eventStore.transaction(processingContext).onAppend(capturedAppends::add);
    }

    private ProcessingContext newProcessingContext(MessageTypeResolver resolver) {
        return new StubProcessingContext(new ApplicationContext() {
            @SuppressWarnings("unchecked")
            @Override
            public <C> C component(Class<C> type, @Nullable String name) {
                if (type == EventStore.class || type == EventSink.class) {
                    return (C) eventStore;
                }
                if (type == MessageTypeResolver.class) {
                    return (C) resolver;
                }
                throw new ComponentNotFoundException(type, name);
            }
        });
    }

    private <P> void seed(P payload, Set<Tag> tags) {
        var message = new GenericEventMessage(new MessageType(payload.getClass()), payload);
        TaggedEventMessage<?> tagged = new GenericTaggedEventMessage<>(message, tags);
        @SuppressWarnings("unchecked")
        AppendTransaction<ConsistencyMarker> tx = (AppendTransaction<ConsistencyMarker>)
                engine.appendEvents(AppendCondition.none(), null, List.of(tagged)).join();
        tx.commit().thenCompose(tx::afterCommit).join();
    }

    private @Nullable Object dispatch(StateControllerComponent component, Object commandPayload) {
        return dispatch(component, commandPayload, processingContext);
    }

    private @Nullable Object dispatch(StateControllerComponent component,
                                      Object commandPayload,
                                      ProcessingContext pc) {
        CommandMessage command = new GenericCommandMessage(new MessageType(commandPayload.getClass()),
                                                           commandPayload);
        MessageStream.Single<CommandResultMessage> stream = component.handle(command, pc);
        CompletableFuture<? extends MessageStream.Entry<? extends CommandResultMessage>> future =
                stream.first().asCompletableFuture();
        try {
            MessageStream.Entry<? extends CommandResultMessage> entry = future.join();
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
    // Decision functions shared by tests
    // ----------------------------------------------------------------------

    private static Decision withdraw(Withdraw cmd, DecisionContext ctx) {
        EventStream account = ctx.scope("account", cmd.accountId());
        var closed = account.contains(AccountClosed.class);
        var balance = account.sum(MoneyDeposited.class, MoneyDeposited::amount)
                             .minus(account.sum(MoneyWithdrawn.class, MoneyWithdrawn::amount));
        if (closed.isTrue()) {
            return Decision.reject("account closed");
        }
        if (balance.isLessThan(cmd.amount()).isTrue()) {
            return Decision.reject("insufficient funds");
        }
        return Decision.emit(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
    }

    private static Decision register(RegisterAccount cmd, DecisionContext ctx) {
        EventStream account = ctx.scope("account", cmd.accountId());
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

    @Nested
    class Accept {

        @Test
        void acceptedDecisionAppendsEventsAndYieldsNoResultWhenNoReturningSet() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(200)),
                 Set.of(new Tag("account", "a1")));
            var component = new StateControllerComponent("accounts")
                    .decide(Withdraw.class, StateControllerComponentTest::withdraw);

            // when
            Object result = dispatch(component, new Withdraw("a1", BigDecimal.valueOf(50)));

            // then
            assertThat(result).isNull();
            assertThat(payloadsOf(MoneyWithdrawn.class))
                    .singleElement()
                    .satisfies(w -> assertThat(w.amount()).isEqualByComparingTo("50"));
        }

        @Test
        void acceptedDecisionWithReturningSurfacesTheResultValueToTheCaller() {
            // given
            var component = new StateControllerComponent("accounts")
                    .decide(RegisterAccount.class, StateControllerComponentTest::register);

            // when
            Object result = dispatch(component, new RegisterAccount("a-new"));

            // then
            assertThat(result).isEqualTo("ACC-a-new");
            assertThat(payloadsOf(AccountOpened.class))
                    .singleElement()
                    .satisfies(o -> assertThat(o.accountId()).isEqualTo("a-new"));
        }

        @Test
        void acceptedDecisionWithExplicitReturningOfNullYieldsNullJustLikeNoReturning() {
            // given — explicitly setting result to null is documented as a way to clear a previously-set
            //         result; the dispatch path must surface null identically to the "no returning set" case
            var component = new StateControllerComponent("accounts")
                    .decide(RegisterAccount.class,
                            (cmd, ctx) -> Decision.emit(new AccountOpened(cmd.accountId())).returning(null));

            // when
            Object result = dispatch(component, new RegisterAccount("a-new"));

            // then
            assertThat(result).isNull();
            assertThat(payloadsOf(AccountOpened.class))
                    .singleElement()
                    .satisfies(o -> assertThat(o.accountId()).isEqualTo("a-new"));
        }
    }

    @Nested
    class Reject {

        @Test
        void rejectedDecisionThrowsCommandExecutionExceptionWithReason() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(10)),
                 Set.of(new Tag("account", "a1")));
            var component = new StateControllerComponent("accounts")
                    .decide(Withdraw.class, StateControllerComponentTest::withdraw);

            // when / then
            assertThatThrownBy(() -> dispatch(component, new Withdraw("a1", BigDecimal.valueOf(100))))
                    .isInstanceOf(CommandExecutionException.class)
                    .hasMessageContaining("insufficient funds");
            assertThat(payloadsOf(MoneyWithdrawn.class)).isEmpty();
        }

        @Test
        void rejectedDecisionAppendsAuditEventsThroughTheSameAppender() {
            // given
            seed(new AccountClosed("blocked"), Set.of(new Tag("account", "blocked")));
            var component = new StateControllerComponent("accounts")
                    .decide(RegisterAccount.class, StateControllerComponentTest::register);

            // when / then
            CommandExecutionException ex = (CommandExecutionException)
                    assertThatThrownBy(() -> dispatch(component, new RegisterAccount("blocked")))
                            .isInstanceOf(CommandExecutionException.class)
                            .hasMessageContaining("account is closed")
                            .actual();
            assertThat(payloadsOf(AuditedRejection.class))
                    .singleElement()
                    .satisfies(a -> {
                        assertThat(a.accountId()).isEqualTo("blocked");
                        assertThat(a.reason()).isEqualTo("closed");
                    });
            assertThat(ex.<List<Object>>getDetails().orElse(List.of()))
                    .singleElement()
                    .isInstanceOf(AuditedRejection.class);
        }
    }

    @Nested
    class Routing {

        @Test
        void supportedCommandsReflectsRegisteredCommandClasses() {
            // given / when
            var component = new StateControllerComponent("accounts")
                    .decide(Withdraw.class, StateControllerComponentTest::withdraw)
                    .decide(RegisterAccount.class, StateControllerComponentTest::register);

            // then
            assertThat(component.supportedCommands())
                    .containsExactlyInAnyOrder(
                            new QualifiedName(Withdraw.class.getName()),
                            new QualifiedName(RegisterAccount.class.getName())
                    );
        }

        @Test
        void unsupportedCommandFailsWithNoHandlerForCommandException() {
            // given — component only knows about Withdraw
            var component = new StateControllerComponent("accounts")
                    .decide(Withdraw.class, StateControllerComponentTest::withdraw);

            // when / then
            assertThatThrownBy(() -> dispatch(component, new RegisterAccount("nope")))
                    .isInstanceOf(NoHandlerForCommandException.class);
        }

        @Test
        void multipleDecisionsRoutedByCommandQualifiedName() {
            // given
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)),
                 Set.of(new Tag("account", "a1")));
            var component = new StateControllerComponent("accounts")
                    .decide(Withdraw.class, StateControllerComponentTest::withdraw)
                    .decide(RegisterAccount.class, StateControllerComponentTest::register);

            // when — dispatch each command type and confirm independent routing (separate ProcessingContexts so
            // each command runs against a fresh DecisionContext, mirroring real CommandBus behaviour)
            Object regResult = dispatch(component,
                                        new RegisterAccount("brand-new"),
                                        newProcessingContext(typeResolver));
            Object withdrawResult = dispatch(component,
                                             new Withdraw("a1", BigDecimal.valueOf(40)),
                                             newProcessingContext(typeResolver));

            // then
            assertThat(regResult).isEqualTo("ACC-brand-new");
            assertThat(withdrawResult).isNull();
        }
    }

    @Nested
    class TypeResolverWiring {

        @Test
        void customMessageTypeResolverDeterminesRoutingQualifiedName() {
            // given — a resolver that maps Withdraw to a non-default name "banking.Withdraw"
            QualifiedName custom = new QualifiedName("banking", "Withdraw");
            MessageTypeResolver customResolver = clazz -> clazz == Withdraw.class
                    ? Optional.of(new MessageType(custom))
                    : Optional.of(new MessageType(clazz));
            var component = new StateControllerComponent("accounts", customResolver)
                    .decide(Withdraw.class, StateControllerComponentTest::withdraw);

            // then — supportedCommands() carries the custom name
            assertThat(component.supportedCommands()).containsExactly(custom);

            // and — a command published under the custom QualifiedName routes to the registered decision
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(200)),
                 Set.of(new Tag("account", "a1")));
            CommandMessage command = new GenericCommandMessage(new MessageType(custom),
                                                               new Withdraw("a1", BigDecimal.valueOf(20)));
            MessageStream.Single<CommandResultMessage> stream = component.handle(command, processingContext);
            // forces evaluation
            stream.first().asCompletableFuture().join();
            assertThat(payloadsOf(MoneyWithdrawn.class)).hasSize(1);
        }
    }

    @Nested
    class SharedDecisionContext {

        @Test
        void annotationAndDeclarativePathsShareTheSameDecisionContextForOneProcessingContext() {
            // given — DecisionDispatch.decisionContextFor caches a DecisionContext on the ProcessingContext;
            //         both the declarative dispatch path and any subsequent direct lookup must observe the
            //         same instance
            seed(new MoneyDeposited("a1", BigDecimal.valueOf(100)),
                 Set.of(new Tag("account", "a1")));
            var component = new StateControllerComponent("accounts")
                    .decide(Withdraw.class, StateControllerComponentTest::withdraw);

            // when — pre-resolve via the dispatch helper, then dispatch a command
            DecisionContext preDispatch = DecisionDispatch.decisionContextFor(processingContext);
            dispatch(component, new Withdraw("a1", BigDecimal.valueOf(40)));
            DecisionContext postDispatch = DecisionDispatch.decisionContextFor(processingContext);

            // then — same instance throughout
            assertThat(postDispatch).isSameAs(preDispatch);
        }
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
}

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

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandHandler;
import org.axonframework.messaging.commandhandling.CommandHandlingComponent;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandResultMessage;
import org.axonframework.messaging.commandhandling.SimpleCommandHandlingComponent;
import org.axonframework.messaging.core.ClassBasedMessageTypeResolver;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.MessageTypeResolver;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Declarative {@link CommandHandlingComponent} for registering State Controller decisions without annotations.
 * <p>
 * Each call to {@link #decide(Class, BiFunction)} subscribes a typed decision function — a
 * {@code BiFunction<P, DecisionContext, Decision>} — for the given command payload class. At dispatch time, the
 * component:
 * <ul>
 *     <li>resolves the in-context {@link DecisionContext} via {@link DecisionDispatch#decisionContextFor},
 *         so all decisions for the same {@link ProcessingContext} share a single loading-context;</li>
 *     <li>invokes the decision function with the command's payload and the shared {@code DecisionContext};</li>
 *     <li>routes the returned {@link Decision} through {@link DecisionDispatch#apply}, which appends events
 *         on {@link Decision.Accept Accept} (returning the optional
 *         {@link Decision.Accept#result() result value}) and throws a
 *         {@link org.axonframework.messaging.commandhandling.CommandExecutionException CommandExecutionException}
 *         on {@link Decision.Reject Reject} after appending any audit events.</li>
 * </ul>
 * The component is a drop-in {@link CommandHandlingComponent}: subscribe it directly to a
 * {@link org.axonframework.messaging.commandhandling.CommandBus CommandBus}, or contribute it to a
 * {@link org.axonframework.messaging.commandhandling.configuration.CommandHandlingModule CommandHandlingModule}
 * via {@code commandHandlingComponent(...)} for declarative wiring. From a command caller's perspective the
 * observable semantics are identical to the annotation-based {@code @StateController} path.
 * <p>
 * The {@link MessageTypeResolver} supplied at construction is used to map registered command classes to their
 * {@link QualifiedName} for {@link #supportedCommands()} and routing. Pass a custom resolver here if your
 * commands carry {@code @Message(name = ...)} (or {@code @Event(name = ...)}) overrides; otherwise the default
 * {@link ClassBasedMessageTypeResolver} maps each class to its fully-qualified name.
 *
 * <h3>Example</h3>
 * <pre>{@code
 * StateControllerComponent accounts = new StateControllerComponent("accounts")
 *         .decide(Withdraw.class, (cmd, ctx) -> {
 *             var account = ctx.scope("account", cmd.accountId());
 *             var closed  = account.contains(AccountClosed.class);
 *             var balance = account.sum(MoneyDeposited.class, MoneyDeposited::amount)
 *                                  .minus(account.sum(MoneyWithdrawn.class, MoneyWithdrawn::amount));
 *             if (closed.isTrue())                            return Decision.reject("closed");
 *             if (balance.isLessThan(cmd.amount()).isTrue())  return Decision.reject("insufficient funds");
 *             return Decision.emit(new MoneyWithdrawn(cmd.accountId(), cmd.amount()));
 *         });
 *
 * commandBus.subscribe(accounts);
 * }</pre>
 *
 * @author Allard Buijze
 * @since 5.2.0
 */
public final class StateControllerComponent implements CommandHandlingComponent {

    private final String name;
    private final MessageTypeResolver typeResolver;
    private final SimpleCommandHandlingComponent delegate;

    /**
     * Creates a {@code StateControllerComponent} with the default {@link ClassBasedMessageTypeResolver}.
     *
     * @param name display name for the component, used in {@link ComponentDescriptor diagnostics}
     */
    public StateControllerComponent(String name) {
        this(name, new ClassBasedMessageTypeResolver());
    }

    /**
     * Creates a {@code StateControllerComponent} with the given {@link MessageTypeResolver}, used to resolve
     * each registered command class to its {@link QualifiedName} at registration time.
     *
     * @param name         display name for the component, used in {@link ComponentDescriptor diagnostics}
     * @param typeResolver the resolver used to map command classes to {@link QualifiedName} entries on
     *                     {@link #supportedCommands()}
     */
    public StateControllerComponent(String name, MessageTypeResolver typeResolver) {
        this.name = Objects.requireNonNull(name, "name must not be null");
        this.typeResolver = Objects.requireNonNull(typeResolver, "typeResolver must not be null");
        this.delegate = SimpleCommandHandlingComponent.create("StateControllerComponent[" + name + "]");
    }

    /**
     * Subscribes a decision function for commands of type {@code commandType}.
     * <p>
     * On dispatch, the function receives the command's deserialized payload and a {@link DecisionContext}
     * resolved from the current {@link ProcessingContext}. Its returned {@link Decision} is processed by
     * {@link DecisionDispatch#apply}, so the observable side effects (events appended, result returned, or
     * rejection thrown) are identical to those of an equivalent {@link StateController @StateController}
     * method.
     *
     * @param commandType the payload class of the command this decision handles
     * @param decision    the decision function, taking the payload and a {@link DecisionContext}
     * @param <P>         the command payload type
     * @return this component, for fluent registration
     */
    public <P> StateControllerComponent decide(Class<P> commandType,
                                               BiFunction<? super P, DecisionContext, Decision> decision) {
        Objects.requireNonNull(commandType, "commandType must not be null");
        Objects.requireNonNull(decision, "decision must not be null");
        QualifiedName qualifiedName = typeResolver.resolveOrThrow(commandType).qualifiedName();
        delegate.subscribe(qualifiedName, handlerFor(commandType, decision));
        return this;
    }

    private static <P> CommandHandler handlerFor(Class<P> commandType,
                                                 BiFunction<? super P, DecisionContext, Decision> decision) {
        return (command, processingContext) -> {
            try {
                P payload = commandType.cast(command.payload());
                DecisionContext decisionContext = DecisionDispatch.decisionContextFor(processingContext);
                Decision outcome = decision.apply(payload, decisionContext);
                Object result = DecisionDispatch.apply(outcome, processingContext);
                if (result == null) {
                    return MessageStream.<CommandResultMessage>empty().cast();
                }
                CommandResultMessage resultMessage =
                        new GenericCommandResultMessage(new MessageType(result.getClass()), result);
                return MessageStream.just(resultMessage).cast();
            } catch (RuntimeException failure) {
                // Includes the CommandExecutionException raised by DecisionDispatch.apply on Reject (rejection
                // reason as message, audit events as details), as well as programmer errors (NPE,
                // ClassCastException, ...) raised inside the decision function. The caller distinguishes them
                // by the exception type observed on the failed MessageStream.
                return MessageStream.<CommandResultMessage>failed(failure).cast();
            }
        };
    }

    @Override
    public MessageStream.Single<CommandResultMessage> handle(CommandMessage command,
                                                             ProcessingContext processingContext) {
        return delegate.handle(command, processingContext);
    }

    @Override
    public Set<QualifiedName> supportedCommands() {
        return delegate.supportedCommands();
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeProperty("name", name);
        descriptor.describeProperty("typeResolver", typeResolver);
        descriptor.describeWrapperOf(delegate);
    }
}

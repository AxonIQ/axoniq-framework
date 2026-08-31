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

package io.axoniq.framework.messaging.commandhandling.distributed;

import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link CommandBusConnector} that offers a "local shortcut".
 * <p>
 * This shortcut triggers for commands matching a {@link LocalCommandDispatchPredicate}, and for which the local segment
 * subscribed a handler, the command is handed to the local segment directly instead of being
 * {@link #dispatch(CommandMessage, ProcessingContext) dispatched} through the wrapped connector. Furthermore, the
 * shortcut only activates when the local segment can actually handle the command, tracked through the
 * {@link #subscribe(QualifiedName, int)}/{@link #unsubscribe(QualifiedName)} calls this connector observes.
 * <p>
 * Short-cut commands are handled through the same {@link Handler} as any other {@code CommandBusConnector}. As such,
 * they are queued onto and executed by the same bounded, priority-ordered worker pool as commands arriving from remote
 * segments. A burst of locally-preferred commands is therefore subject to the same back-pressure and cannot flood the
 * local segment beyond what it would already accept from remote traffic.
 *
 * @author Allard Buijze
 * @see LocalCommandDispatchPredicate
 * @see LocalShortcutCommandBusConnectorConfigurationEnhancer
 * @since 5.4.0
 */
public class LocalShortcutCommandBusConnector extends DelegatingCommandBusConnector {

    private final LocalCommandDispatchPredicate shortcutPredicate;
    private final Set<QualifiedName> subscriptions = ConcurrentHashMap.newKeySet();

    private volatile @Nullable Handler localHandler;

    /**
     * Initialize a connector with the given {@code delegate}, using the given {@code shortcutPredicate} to decide
     * whether to shortcut a command yes or no.
     *
     * @param delegate          the {@link CommandBusConnector} to delegate to when not dispatching locally
     * @param shortcutPredicate the predicate deciding whether a command should be dispatched to the local segment
     *                          directly
     */
    public LocalShortcutCommandBusConnector(CommandBusConnector delegate,
                                            LocalCommandDispatchPredicate shortcutPredicate) {
        super(delegate);
        this.shortcutPredicate = Objects.requireNonNull(
                shortcutPredicate, "The LocalCommandDispatchPredicate must not be null."
        );
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        Handler handler = this.localHandler;

        if (handler != null
                && subscriptions.contains(command.type().qualifiedName())
                && shortcutPredicate.shouldDispatchLocally(command, processingContext)) {
            return dispatchLocally(command, handler);
        }

        return super.dispatch(command, processingContext);
    }

    private static CompletableFuture<CommandResultMessage> dispatchLocally(CommandMessage command, Handler handler) {
        CompletableFuture<CommandResultMessage> result = new CompletableFuture<>();
        handler.handle(command, new ResultCallback() {
            @Override
            public void onSuccess(CommandResultMessage resultMessage) {
                result.complete(resultMessage);
            }

            @Override
            public void onError(Throwable cause) {
                result.completeExceptionally(cause);
            }
        });
        return result;
    }

    @Override
    public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
        subscriptions.add(commandName);
        return super.subscribe(commandName, loadFactor);
    }

    @Override
    public boolean unsubscribe(QualifiedName commandName) {
        subscriptions.remove(commandName);
        return super.unsubscribe(commandName);
    }

    @Override
    public void onIncomingCommand(Handler handler) {
        this.localHandler = handler;
        super.onIncomingCommand(handler);
    }

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
        descriptor.describeProperty("shortcutPredicate", shortcutPredicate);
        descriptor.describeProperty("subscriptions", subscriptions);
    }
}

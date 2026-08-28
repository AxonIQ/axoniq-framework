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

import org.jspecify.annotations.Nullable;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link CommandBusConnector} that offers a "local shortcut": for commands matching a
 * {@link LocalCommandDispatchPredicate}, and for which the local segment subscribed a handler, the command is handed to
 * the local segment directly instead of being {@link #dispatch(CommandMessage, ProcessingContext) dispatched} through
 * the wrapped connector.
 * <p>
 * The shortcut reuses the very same local-handling path that the {@link DistributedCommandBus} exposes to incoming,
 * remotely-routed commands: the {@link Handler} registered through {@link #onIncomingCommand(Handler)}. A short-cut
 * command therefore behaves exactly as if it had been routed to this segment - same handler invocation, same result
 * handling - only without leaving the JVM. Because this connector wraps the payload-converting connector (rather than
 * the other way around), the shortcut also avoids the payload serialization round-trip.
 * <p>
 * Because short-cut commands are handled through this same {@link Handler}, they are queued onto and executed by the
 * same bounded, priority-ordered worker pool as commands arriving from remote segments. A burst of locally-preferred
 * commands is therefore subject to the same back-pressure and cannot flood the local segment beyond what it would
 * already accept from remote traffic.
 * <p>
 * The shortcut only kicks in when the local segment can actually handle the command, tracked through the
 * {@link #subscribe(QualifiedName, int)}/{@link #unsubscribe(QualifiedName)} calls this connector observes. When the
 * command's {@link CommandMessage#type() type} is not locally subscribed, the command is routed through the wrapped
 * connector as usual, regardless of the predicate. This prevents a locally-preferred command that this node does not
 * handle from failing instead of being routed to a segment that does.
 *
 * @author Allard Buijze
 * @see LocalCommandDispatchPredicate
 * @see LocalShortcutCommandBusConnectorConfigurationEnhancer
 * @since 5.4.0
 */
public class LocalShortcutCommandBusConnector extends DelegatingCommandBusConnector {

    private final LocalCommandDispatchPredicate localDispatchPredicate;
    private final Set<QualifiedName> localSubscriptions = ConcurrentHashMap.newKeySet();

    private volatile @Nullable Handler localHandler;

    /**
     * Initialize the connector to delegate to the given {@code delegate}, taking a local shortcut for commands accepted
     * by the given {@code localDispatchPredicate} that are also handled by the local segment.
     *
     * @param delegate               the {@link CommandBusConnector} to delegate to when not dispatching locally
     * @param localDispatchPredicate the predicate deciding whether a command should be dispatched to the local segment
     *                               directly
     */
    public LocalShortcutCommandBusConnector(CommandBusConnector delegate,
                                            LocalCommandDispatchPredicate localDispatchPredicate) {
        super(delegate);
        this.localDispatchPredicate =
                Objects.requireNonNull(localDispatchPredicate, "The localDispatchPredicate must not be null.");
    }

    @Override
    public CompletableFuture<@Nullable CommandResultMessage> dispatch(CommandMessage command,
                                                                      @Nullable ProcessingContext processingContext) {
        Handler handler = this.localHandler;
        if (handler != null
                && localSubscriptions.contains(command.type().qualifiedName())
                && localDispatchPredicate.shouldDispatchLocally(command, processingContext)) {
            return dispatchLocally(command, handler);
        }
        return super.dispatch(command, processingContext);
    }

    private static CompletableFuture<@Nullable CommandResultMessage> dispatchLocally(CommandMessage command, Handler handler) {
        CompletableFuture<@Nullable CommandResultMessage> result = new CompletableFuture<>();
        handler.handle(command, new ResultCallback() {
            @Override
            public void onSuccess(@Nullable CommandResultMessage resultMessage) {
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
        localSubscriptions.add(commandName);
        return super.subscribe(commandName, loadFactor);
    }

    @Override
    public boolean unsubscribe(QualifiedName commandName) {
        localSubscriptions.remove(commandName);
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
        descriptor.describeProperty("localDispatchPredicate", localDispatchPredicate);
        descriptor.describeProperty("localSubscriptions", localSubscriptions);
    }
}

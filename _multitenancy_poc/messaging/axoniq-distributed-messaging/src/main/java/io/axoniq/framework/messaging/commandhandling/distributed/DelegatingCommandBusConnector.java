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
import java.util.concurrent.CompletableFuture;

/**
 * A {@link CommandBusConnector} implementation that wraps another {@link CommandBusConnector} and delegates all calls
 * to it. This can be used to add additional functionality through decoration to a {@link CommandBusConnector} without
 * having to implement all methods again.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
public abstract class DelegatingCommandBusConnector implements CommandBusConnector {

    protected final CommandBusConnector delegate;

    /**
     * Initialize the WrappedConnector to delegate all calls to the given {@code delegate}.
     *
     * @param delegate The {@link CommandBusConnector} to delegate all calls to.
     */
    protected DelegatingCommandBusConnector(CommandBusConnector delegate) {
        this.delegate = Objects.requireNonNull(delegate, "The delegate must not be null.");
    }

    @Override
    public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                            @Nullable ProcessingContext processingContext) {
        return delegate.dispatch(command, processingContext);
    }

    // region [Connector]
    @Override
    public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
        return delegate.subscribe(commandName, loadFactor);
    }

    @Override
    public boolean unsubscribe(QualifiedName commandName) {
        return delegate.unsubscribe(commandName);
    }

    @Override
    public void onIncomingCommand(Handler handler) {
        delegate.onIncomingCommand(handler);
    }

    // endregion

    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        descriptor.describeWrapperOf(delegate);
    }
}
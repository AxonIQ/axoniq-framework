/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package org.axonframework.messaging.commandhandling.tracing;

import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.common.BuilderUtils;
import org.axonframework.messaging.tracing.Span;
import org.axonframework.messaging.tracing.SpanFactory;

/**
 * Default implementation of the {@link CommandBusSpanFactory}. Can be configured to include the command handling of a
 * distributed command in the same trace or not (true by default).
 *
 * @author Mitchell Herrijgers
 * @since 4.9.0
 */
public class DefaultCommandBusSpanFactory implements CommandBusSpanFactory {

    private final SpanFactory spanFactory;
    private final boolean distributedInSameTrace;

    /**
     * Creates a new {@code DefaultCommandBusSpanFactory} using the provided {@code builder}.
     *
     * @param builder The builder to build the {@code DefaultCommandBusSpanFactory} from.
     */
    protected DefaultCommandBusSpanFactory(Builder builder) {
        builder.validate();
        this.spanFactory = builder.builderSpanFactory;
        this.distributedInSameTrace = builder.distributedInSameTrace;
    }

    @Override
    public Span createDispatchCommandSpan(CommandMessage commandMessage, boolean distributed) {
        if (distributed) {
            return spanFactory.createDispatchSpan(() -> "CommandBus.dispatchDistributedCommand", commandMessage);
        }
        return spanFactory.createInternalSpan(() -> "CommandBus.dispatchCommand", commandMessage);
    }

    @Override
    public Span createHandleCommandSpan(CommandMessage commandMessage, boolean distributed) {
        if (distributed) {
            if (distributedInSameTrace) {
                return spanFactory.createChildHandlerSpan(() -> "CommandBus.handleDistributedCommand", commandMessage);
            }
            return spanFactory.createLinkedHandlerSpan(() -> "CommandBus.handleDistributedCommand", commandMessage);
        }
        return spanFactory.createChildHandlerSpan(() -> "CommandBus.handleCommand", commandMessage);
    }

    @Override
    public CommandMessage propagateContext(CommandMessage commandMessage) {
        return spanFactory.propagateContext(commandMessage);
    }

    /**
     * Creates a Builder to be able to create a {@code DefaultCommandBusSpanFactory}. The default values are:
     * <ul>
     *     <li>{@code distributedCommandInSameTrace} defaults to {@code true}</li>
     * </ul>
     * The {@code spanFactory} is a required field and should be provided.
     *
     * @return a Builder to be able to create a {@code DefaultCommandBusSpanFactory}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder class to instantiate a {@link DefaultCommandBusSpanFactory}. The default values are:
     * <ul>
     *     <li>{@code distributedCommandInSameTrace} defaults to {@code true}</li>
     * </ul>
     * The {@code spanFactory} is a required field and should be provided.
     */
    public static class Builder {

        private SpanFactory builderSpanFactory;
        private boolean distributedInSameTrace = true;

        /**
         * Sets the {@link SpanFactory} to use to create the spans. This is a required field.
         *
         * @param spanFactory The {@link SpanFactory} to use to create the spans.
         * @return The current Builder instance, for fluent interfacing.
         */
        public Builder spanFactory(SpanFactory spanFactory) {
            BuilderUtils.assertNonNull(spanFactory, "spanFactory may not be null");
            this.builderSpanFactory = spanFactory;
            return this;
        }

        /**
         * Sets whether the {@link CommandMessage}s should be handled in the same trace as the dispatching span.
         *
         * @param distributedInSameTrace whether the {@link CommandMessage CommandsMessages} should be handled in the
         *                               same trace as the dispatching span.
         * @return The current Builder instance, for fluent interfacing.
         */
        public Builder distributedInSameTrace(boolean distributedInSameTrace) {
            this.distributedInSameTrace = distributedInSameTrace;
            return this;
        }

        /**
         * Validates whether the fields contained in this builder are set accordingly.
         */
        protected void validate() {
            BuilderUtils.assertNonNull(builderSpanFactory, "spanFactory may not be null");
        }

        /**
         * Initializes a {@link DefaultCommandBusSpanFactory} as specified through this Builder.
         *
         * @return The {@link DefaultCommandBusSpanFactory} as specified through this Builder.
         */
        public DefaultCommandBusSpanFactory build() {
            return new DefaultCommandBusSpanFactory(this);
        }
    }
}

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
import org.axonframework.messaging.core.unitofwork.ProcessingContext;

/**
 * Predicate deciding whether a {@link CommandMessage} that is about to be dispatched through a
 * {@link CommandBusConnector} should be handled by the local segment directly - taking a "local shortcut" - instead of
 * being routed by the connector, which may hand the command to a different segment.
 * <p>
 * Register an implementation as a component to activate the {@link LocalShortcutCommandBusConnector}. The predicate is
 * consulted for every outgoing dispatch, but only takes effect when the local segment actually subscribed a handler for
 * the command; when it did not, the command is routed as usual regardless of this predicate's outcome.
 * <p>
 * Both the {@code command} and the (nullable) {@link ProcessingContext} of the dispatch are provided, allowing the
 * decision to be based on the command's payload, {@link CommandMessage#type() type}, metadata, or a flag placed on the
 * {@code ProcessingContext} by the dispatcher.
 *
 * @author Allard Buijze
 * @see LocalShortcutCommandBusConnector
 * @since 5.3.0
 */
@FunctionalInterface
public interface LocalCommandDispatchPredicate {

    /**
     * Indicates whether the given {@code command} should be dispatched to the local segment directly, rather than being
     * routed through the connector.
     *
     * @param command the command message about to be dispatched
     * @param context the processing context active for the dispatch, or {@code null} when the command is dispatched
     *                outside of an existing processing context
     * @return {@code true} to attempt to dispatch the command to the local segment, {@code false} to route it through
     * the connector as usual
     */
    boolean shouldDispatchLocally(CommandMessage command, @Nullable ProcessingContext context);
}

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

package io.axoniq.framework.messaging.queryhandling.distributed;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.jspecify.annotations.Nullable;

/**
 * Predicate deciding whether a {@link QueryMessage} that is about to be dispatched through a {@link QueryBusConnector}
 * should be handled by the local segment directly - taking a "local shortcut" - instead of being routed by the
 * connector, which may hand the query to a different segment.
 * <p>
 * Register an implementation as a component to activate the {@link LocalShortcutQueryBusConnector}. The predicate is
 * consulted for every outgoing direct (point-to-point) query, but only takes effect when the local segment actually
 * subscribed a handler for the query. When it did not, the query is routed as usual regardless of this predicate's
 * outcome. Subscription queries are never shortcut and never consult this predicate as their update registrations must
 * be coordinated across all nodes.
 * <p>
 * Both the {@code query} and the (nullable) {@link ProcessingContext} of the dispatch are provided, allowing the
 * decision to be based on the query's payload, {@link QueryMessage#type() type}, metadata, or a flag placed on the
 * {@code ProcessingContext} by the dispatcher.
 *
 * @author Allard Buijze
 * @see LocalShortcutQueryBusConnector
 * @since 5.4.0
 */
@FunctionalInterface
public interface LocalQueryDispatchPredicate {

    /**
     * Indicates whether the given {@code query} should be dispatched to the local segment directly, rather than being
     * routed through the connector.
     *
     * @param query   the query message about to be dispatched
     * @param context the processing context active for the dispatch, or {@code null} when the query is dispatched
     *                outside of an existing processing context
     * @return {@code true} to attempt to dispatch the query to the local segment, {@code false} to route it through the
     * connector as usual
     */
    boolean shouldDispatchLocally(QueryMessage query, @Nullable ProcessingContext context);
}

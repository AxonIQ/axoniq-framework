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

package org.axonframework.messaging.queryhandling;

import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.QualifiedName;

import java.util.Set;

/**
 * Interface describing a registry of {@link QueryHandler query handlers}.
 *
 * @param <S> The type of the registry itself, used for fluent interfacing.
 * @author Allard Buijze
 * @author Gerard Klijs
 * @author Milan Savic
 * @author Mitchell Herrijgers
 * @author Sara Pellegrini
 * @author Steven van Beelen
 * @since 5.0.0
 */
public interface QueryHandlerRegistry<S extends QueryHandlerRegistry<S>> {

    /**
     * Subscribe the given {@code handler} for {@link QueryMessage queries} and {@link QueryResponseMessage response} of
     * the given {@code names}.
     * <p>
     * If a subscription already exists for any {@link QualifiedName name} in the given set, the behavior is undefined.
     * Implementations may throw an exception to refuse duplicate subscription or alternatively decide whether the
     * existing or new {@code handler} gets the subscription.
     *
     * @param names        The names of the queries the given {@code queryHandler} can handle.
     * @param queryHandler The handler instance that handles {@link QueryMessage queries} for the given names.
     * @return This registry for fluent interfacing.
     */
    default S subscribe(Set<QualifiedName> names,
                        QueryHandler queryHandler) {
        names.forEach(name -> subscribe(name, queryHandler));
        //noinspection unchecked
        return (S) this;
    }

    /**
     * Subscribe the given {@code queryHandler} for {@link QueryMessage queries} and
     * {@link QueryResponseMessage response} of the given {@code queryName}.
     * <p>
     * If a subscription already exists for the {@code queryName}, the behavior is undefined. Implementations may throw
     * an exception to refuse duplicate subscription or alternatively decide whether the existing or new {@code handler}
     * gets the subscription.
     *
     * @param queryName  The fully qualified name of the query
     * @param queryHandler The handler instance that handles {@link QueryMessage queries} for the given queryName.
     * @return This registry for fluent interfacing.
     */
    S subscribe(QualifiedName queryName,
                QueryHandler queryHandler);

    /**
     * Subscribe the given {@code handlingComponent} with this registry.
     * <p>
     * Typically invokes {@link #subscribe(Set, QueryHandler)}, using the
     * {@link QueryHandlingComponent#supportedQueries()} as the set of compatible {@link MessageType handler names}
     * the component in question can deal with.
     *
     * @param handlingComponent The query handling component instance to subscribe with this registry.
     * @return This registry for fluent interfacing.
     */
    default S subscribe(QueryHandlingComponent handlingComponent) {
        return subscribe(handlingComponent.supportedQueries(), handlingComponent);
    }
}

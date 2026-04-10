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

package org.axonframework.messaging.eventhandling.deadletter.jpa;

import jakarta.persistence.EntityManager;
import jakarta.persistence.TypedQuery;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.annotation.Internal;
import org.axonframework.common.tx.TransactionalExecutor;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Enables iterating through a JPA query using paging while lazily mapping the results when necessary. Paging is taken
 * care of automatically, fetching the next page when the items run out to iterate through.
 * <p>
 * Do not use this for paging when you care about concurrent deletes. If you loaded a page, delete an item from it, and
 * load the next, you will miss an item during iteration.
 * <p>
 * The {@link #iterator()} function can be called multiple times to loop through the items, restarting the query from
 * the start.
 *
 * @param <T> The query result type.
 * @param <R> The mapped result type.
 * @author Mitchell Herrijgers
 * @since 4.6.0
 */
@Internal
class PagingJpaQueryIterable<T, R> implements Iterable<R> {

    private static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofSeconds(30);

    private final int pageSize;
    private final Duration queryTimeout;
    private final TransactionalExecutor<EntityManager> executor;
    private final Function<EntityManager, TypedQuery<T>> queryFunction;
    private final Function<T, R> lazyMappingFunction;

    /**
     * Constructs a new {@link Iterable} using the provided {@code queryFunction} to construct queries when a new page
     * needs to be fetched. Items are lazily mapped by the provided {@code lazyMappingFunction} when iterating.
     * <p>
     * Uses a default query timeout of 30 seconds per page fetch.
     *
     * @param pageSize            The size of the pages.
     * @param executor            The {@link TransactionalExecutor} to use when fetching items.
     * @param queryFunction       A function that, given an {@link EntityManager}, produces a {@link TypedQuery}. Will
     *                            be invoked for each page.
     * @param lazyMappingFunction The mapping function to map items to the desired representation.
     */
    public PagingJpaQueryIterable(int pageSize,
                                  TransactionalExecutor<EntityManager> executor,
                                  Function<EntityManager, TypedQuery<T>> queryFunction,
                                  Function<T, R> lazyMappingFunction
    ) {
        this(pageSize, DEFAULT_QUERY_TIMEOUT, executor, queryFunction, lazyMappingFunction);
    }

    /**
     * Constructs a new {@link Iterable} using the provided {@code queryFunction} to construct queries when a new page
     * needs to be fetched. Items are lazily mapped by the provided {@code lazyMappingFunction} when iterating.
     *
     * @param pageSize            The size of the pages.
     * @param queryTimeout        The maximum time to wait for each page query to complete.
     * @param executor            The {@link TransactionalExecutor} to use when fetching items.
     * @param queryFunction       A function that, given an {@link EntityManager}, produces a {@link TypedQuery}. Will
     *                            be invoked for each page.
     * @param lazyMappingFunction The mapping function to map items to the desired representation.
     */
    public PagingJpaQueryIterable(int pageSize,
                                  Duration queryTimeout,
                                  TransactionalExecutor<EntityManager> executor,
                                  Function<EntityManager, TypedQuery<T>> queryFunction,
                                  Function<T, R> lazyMappingFunction
    ) {
        this.pageSize = pageSize;
        this.queryTimeout = queryTimeout;
        this.executor = executor;
        this.queryFunction = queryFunction;
        this.lazyMappingFunction = lazyMappingFunction;
    }

    /**
     * The {@link Iterator} that loops through the provided query's pages until it runs out of items.
     */
    public class PagingIterator implements Iterator<R> {

        private final Deque<T> queue = new ArrayDeque<>();
        private int page = 0;

        @Override
        public boolean hasNext() {
            refreshPageIfNecessary();
            return !queue.isEmpty();
        }

        @Override
        public R next() {
            refreshPageIfNecessary();
            T pop = queue.pop();
            if (pop == null) {
                throw new NoSuchElementException();
            }
            return lazyMappingFunction.apply(pop);
        }

        private void refreshPageIfNecessary() {
            if (!queue.isEmpty()) {
                return;
            }
            FutureUtils.joinAndUnwrap(
                    executor.accept(em -> queryFunction.apply(em)
                                                       .setMaxResults(pageSize)
                                                       .setFirstResult(page * pageSize)
                                                       .getResultList()
                                                       .forEach(queue::offerLast)),
                    queryTimeout
            );
            page++;
        }
    }

    @Override
    public Iterator<R> iterator() {
        return new PagingIterator();
    }
}

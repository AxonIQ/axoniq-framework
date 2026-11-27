/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import com.mongodb.client.ClientSession;
import org.axonframework.common.FutureUtils;
import org.axonframework.common.tx.TransactionalExecutor;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * An {@link Iterable} that lazily retrieves its elements from MongoDB page by page.
 * <p>
 * Every page is retrieved through the given {@link TransactionalExecutor}, so no cursor or session is held in between
 * pages. A page is retrieved when the iterator needs it, which blocks the calling thread for at most the query timeout.
 * The elements of a page are mapped when they are returned. The iterator is exhausted once a page holds fewer elements
 * than the page size.
 * <p>
 * This class is internal, as it only exists to share the paging between the iterations of the
 * {@link MongoSequencedDeadLetterQueue}.
 *
 * @param <T> the type of the elements as they are retrieved from MongoDB
 * @param <R> the type of the elements this iterable returns
 * @author Steven van Beelen
 * @since 5.5.0
 */
class PagingMongoIterable<T, R> implements Iterable<R> {

    private static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofSeconds(30);

    private final int pageSize;
    private final Duration queryTimeout;
    private final TransactionalExecutor<ClientSession> executor;
    private final PageQuery<T> pageQuery;
    private final Function<T, R> lazyMappingFunction;

    /**
     * Constructs a {@code PagingMongoIterable} with the default query timeout of 30 seconds.
     *
     * @param pageSize            the maximum number of elements per page
     * @param executor            the {@link TransactionalExecutor} to retrieve every page with
     * @param pageQuery           the {@link PageQuery} retrieving a page
     * @param lazyMappingFunction the function mapping an element when it is returned
     */
    PagingMongoIterable(int pageSize,
                        TransactionalExecutor<ClientSession> executor,
                        PageQuery<T> pageQuery,
                        Function<T, R> lazyMappingFunction) {
        this(pageSize, DEFAULT_QUERY_TIMEOUT, executor, pageQuery, lazyMappingFunction);
    }

    /**
     * Constructs a {@code PagingMongoIterable}.
     *
     * @param pageSize            the maximum number of elements per page
     * @param queryTimeout        the longest time to wait for a page
     * @param executor            the {@link TransactionalExecutor} to retrieve every page with
     * @param pageQuery           the {@link PageQuery} retrieving a page
     * @param lazyMappingFunction the function mapping an element when it is returned
     */
    PagingMongoIterable(int pageSize,
                        Duration queryTimeout,
                        TransactionalExecutor<ClientSession> executor,
                        PageQuery<T> pageQuery,
                        Function<T, R> lazyMappingFunction) {
        this.pageSize = pageSize;
        this.queryTimeout = queryTimeout;
        this.executor = executor;
        this.pageQuery = pageQuery;
        this.lazyMappingFunction = lazyMappingFunction;
    }

    @Override
    public Iterator<R> iterator() {
        return new PagingIterator();
    }

    /**
     * Retrieves a page of elements.
     *
     * @param <T> the type of the elements
     */
    @FunctionalInterface
    interface PageQuery<T> {

        /**
         * Retrieves the page of at most {@code pageSize} elements that follows the {@code lastElement}.
         *
         * @param session     the {@link ClientSession} to retrieve the page with
         * @param lastElement the last element of the previous page, or {@code null} when the first page is requested
         * @param pageSize    the maximum number of elements to retrieve
         * @return the elements of the page, in order
         */
        List<T> fetch(ClientSession session, @Nullable T lastElement, int pageSize);
    }

    private class PagingIterator implements Iterator<R> {

        private final Deque<T> page = new ArrayDeque<>();
        private @Nullable T lastElement = null;
        private boolean exhausted = false;

        @Override
        public boolean hasNext() {
            refreshPageIfNecessary();
            return !page.isEmpty();
        }

        @Override
        public R next() {
            refreshPageIfNecessary();
            T element = page.poll();
            if (element == null) {
                throw new NoSuchElementException();
            }
            return lazyMappingFunction.apply(element);
        }

        private void refreshPageIfNecessary() {
            if (!page.isEmpty() || exhausted) {
                return;
            }
            T previous = lastElement;
            List<T> elements = FutureUtils.joinAndUnwrap(
                    executor.apply(session -> pageQuery.fetch(session, previous, pageSize)),
                    queryTimeout
            );
            if (elements.size() < pageSize) {
                exhausted = true;
            }
            page.addAll(elements);
            if (!elements.isEmpty()) {
                lastElement = elements.getLast();
            }
        }
    }
}

/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.deadletter;

import com.mongodb.client.ClientSession;
import org.axonframework.common.function.ThrowingFunction;
import org.axonframework.common.tx.TransactionalExecutor;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates the paging of the {@link PagingMongoIterable} against an in-memory source of elements.
 *
 * @author Steven van Beelen
 */
class PagingMongoIterableTest {

    private final AtomicInteger fetches = new AtomicInteger();
    private final AtomicInteger mappings = new AtomicInteger();

    /**
     * An executor that runs the function right away. The page query of these tests does not use the session.
     */
    private final TransactionalExecutor<ClientSession> executor = new TransactionalExecutor<>() {
        @Override
        public <R> CompletableFuture<R> apply(ThrowingFunction<ClientSession, R, Exception> function) {
            try {
                return CompletableFuture.completedFuture(function.apply(null));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
        }
    };

    private PagingMongoIterable<Integer, String> iterableOver(int elementCount, int pageSize) {
        List<Integer> source = IntStream.range(0, elementCount).boxed().toList();
        return new PagingMongoIterable<>(
                pageSize,
                Duration.ofSeconds(5),
                executor,
                (session, lastElement, size) -> {
                    fetches.incrementAndGet();
                    int from = lastElement == null ? 0 : lastElement + 1;
                    return new ArrayList<>(source.subList(Math.min(from, source.size()),
                                                          Math.min(from + size, source.size())));
                },
                element -> {
                    mappings.incrementAndGet();
                    return "element-" + element;
                }
        );
    }

    private List<String> collect(PagingMongoIterable<Integer, String> iterable) {
        List<String> result = new ArrayList<>();
        iterable.forEach(result::add);
        return result;
    }

    @Test
    void returnsNothingForAnEmptySource() {
        // given / when
        List<String> result = collect(iterableOver(0, 3));

        // then
        assertThat(result).isEmpty();
        assertThat(fetches).hasValue(1);
    }

    @Test
    void returnsAllElementsInOrderAcrossPages() {
        // given / when
        List<String> result = collect(iterableOver(7, 3));

        // then
        assertThat(result).containsExactly("element-0", "element-1", "element-2", "element-3", "element-4",
                                           "element-5", "element-6");
        // two full pages and a partial one ends the iteration
        assertThat(fetches).hasValue(3);
    }

    @Test
    void returnsAllElementsWhenTheSourceIsAMultipleOfThePageSize() {
        // given / when
        List<String> result = collect(iterableOver(6, 3));

        // then
        assertThat(result).hasSize(6);
        // the last page is full, so one more fetch finds out nothing follows
        assertThat(fetches).hasValue(3);
    }

    @Test
    void retrievesPagesAndMapsElementsLazily() {
        // given
        PagingMongoIterable<Integer, String> iterable = iterableOver(10, 4);

        // when
        var iterator = iterable.iterator();

        // then
        assertThat(fetches).hasValue(0);
        assertThat(iterator.next()).isEqualTo("element-0");
        assertThat(fetches).hasValue(1);
        assertThat(mappings).hasValue(1);
    }

    @Test
    void everyIteratorStartsAtTheFirstPage() {
        // given
        PagingMongoIterable<Integer, String> iterable = iterableOver(4, 2);

        // when / then
        assertThat(collect(iterable)).hasSize(4);
        assertThat(collect(iterable)).hasSize(4);
    }

    @Test
    void nextOnAnExhaustedIteratorThrowsNoSuchElementException() {
        // given
        var iterator = iterableOver(1, 3).iterator();
        iterator.next();

        // when / then
        assertThat(iterator.hasNext()).isFalse();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void failureOfAPageQueryIsRethrownToTheCaller() {
        // given
        PagingMongoIterable<Integer, String> iterable = new PagingMongoIterable<>(
                2,
                executor,
                (session, lastElement, size) -> {
                    throw new IllegalStateException("page query failed");
                },
                element -> "element-" + element
        );

        // when / then
        assertThatThrownBy(() -> iterable.iterator().hasNext()).isInstanceOf(IllegalStateException.class);
    }
}

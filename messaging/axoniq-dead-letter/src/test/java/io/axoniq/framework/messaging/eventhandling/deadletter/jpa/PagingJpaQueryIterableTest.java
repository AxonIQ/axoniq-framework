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

package io.axoniq.framework.messaging.eventhandling.deadletter.jpa;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.Persistence;
import org.axonframework.common.jpa.EntityManagerExecutor;
import org.axonframework.common.tx.TransactionalExecutor;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.*;

class PagingJpaQueryIterableTest {

    // We use the jpatest which includes the simple TestJpaEntry as entity
    private final EntityManagerFactory emf = Persistence.createEntityManagerFactory("jpatest");
    private final EntityManager entityManager = emf.createEntityManager();
    private final TransactionalExecutor<EntityManager> executor = new EntityManagerExecutor(() -> entityManager);
    private EntityTransaction transaction;

    @BeforeEach
    void setUpJpa() {
        transaction = entityManager.getTransaction();
        transaction.begin();
    }

    @AfterEach
    void rollback() {
        transaction.rollback();
    }

    @Test
    void queriesJustOneItemAsOnePage() {
        entityManager.persist(new TestJpaEntry("1"));

        PagingJpaQueryIterable<TestJpaEntry, String> iterable = new PagingJpaQueryIterable<>(
                10,
                executor,
                em -> em.createQuery("select t from TestJpaEntry t", TestJpaEntry.class),
                TestJpaEntry::getId);

        List<String> result = StreamSupport.stream(iterable.spliterator(), false).toList();
        assertThat(result).hasSize(1);
        assertThat(result.getFirst()).isEqualTo("1");
    }

    @Test
    void queriesMultiplePages() {
        List<String> wantedIds = IntStream.range(0, 102).mapToObj(i -> "" + i).toList();
        wantedIds.forEach(item -> {
            entityManager.persist(new TestJpaEntry(item));
        });

        PagingJpaQueryIterable<TestJpaEntry, String> iterable = new PagingJpaQueryIterable<>(
                10,
                executor,
                em -> em.createQuery("select t from TestJpaEntry t", TestJpaEntry.class),
                TestJpaEntry::getId);

        List<String> result = StreamSupport.stream(iterable.spliterator(), false).toList();
        assertThat(result).hasSameSizeAs(wantedIds);
        wantedIds.forEach(id -> {
            assertThat(result).contains(id);
        });
    }

    @Test
    void throwsExceptionWhenNoItemPresent() {
        PagingJpaQueryIterable<TestJpaEntry, String> iterable = new PagingJpaQueryIterable<>(
                10,
                executor,
                em -> em.createQuery("select t from TestJpaEntry t", TestJpaEntry.class),
                TestJpaEntry::getId);
        Iterator<String> iterator = iterable.iterator();
        assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
    }

    @Nested
    class TimeoutBehavior {

        @Test
        void throwsTimeoutExceptionWhenQueryHangs() {
            // given
            TransactionalExecutor<EntityManager> hangingExecutor = new TransactionalExecutor<>() {
                @Override
                public <R> @NonNull CompletableFuture<R> apply(
                        org.axonframework.common.function.@NonNull ThrowingFunction<EntityManager, R, Exception> function
                ) {
                    return new CompletableFuture<>(); // never completes
                }
            };

            PagingJpaQueryIterable<TestJpaEntry, String> iterable = new PagingJpaQueryIterable<>(
                    10,
                    Duration.ofMillis(50),
                    hangingExecutor,
                    em -> em.createQuery("select t from TestJpaEntry t", TestJpaEntry.class),
                    TestJpaEntry::getId
            );
            Iterator<String> iterator = iterable.iterator();

            // when / then
            assertThatThrownBy(iterator::hasNext)
                    .isInstanceOf(TimeoutException.class)
                    .hasMessageContaining("Future did not complete within");
        }
    }
}

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

package io.axoniq.framework.springboot.messaging.deadletter.jpa;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.Converter;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkFactory;
import org.axonframework.messaging.core.unitofwork.transaction.TransactionManager;
import org.axonframework.messaging.core.unitofwork.transaction.jpa.JpaTransactionalExecutorProvider;
import io.axoniq.framework.messaging.deadletter.DeadLetter;
import io.axoniq.framework.messaging.deadletter.GenericDeadLetter;
import org.axonframework.messaging.eventhandling.EventMessage;
import org.axonframework.messaging.eventhandling.conversion.EventConverter;
import io.axoniq.framework.messaging.eventhandling.deadletter.AbstractDeadLetteringEventIT;
import io.axoniq.framework.messaging.eventhandling.deadletter.SequencedDeadLetterQueueFactory;
import io.axoniq.framework.messaging.eventhandling.deadletter.jpa.DeadLetterEntry;
import io.axoniq.framework.messaging.eventhandling.deadletter.jpa.DeadLetterEventEntry;
import io.axoniq.framework.messaging.eventhandling.deadletter.jpa.EventMessageDeadLetterJpaConverter;
import io.axoniq.framework.messaging.eventhandling.deadletter.jpa.JpaDeadLetter;
import io.axoniq.framework.messaging.eventhandling.deadletter.jpa.JpaSequencedDeadLetterQueue;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jmx.JmxAutoConfiguration;
import org.springframework.boot.autoconfigure.web.reactive.function.client.WebClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.jmx.support.RegistrationPolicy;
import org.springframework.test.annotation.DirtiesContext;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.axonframework.messaging.eventhandling.EventTestUtils.asEventMessage;
import static org.assertj.core.api.Assertions.*;

/**
 * A Spring Boot-idiomatic implementation of the {@link AbstractDeadLetteringEventIT} validating the
 * {@link JpaSequencedDeadLetterQueue} with full Spring Boot auto-configuration.
 * <p>
 * This test leverages Spring Boot auto-configuration for JPA infrastructure ({@code DataSource},
 * {@code EntityManagerFactory}, {@code PlatformTransactionManager}) instead of manually wiring these beans. The
 * embedded HSQLDB database is auto-detected by Spring Boot.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 */
@SpringBootTest(properties = {
        "axon.axonserver.enabled=false",
        "axon.postgresql.enabled=false",
        "spring.main.banner-mode=off"
})
@SpringBootConfiguration
@EnableAutoConfiguration(exclude = {
        JmxAutoConfiguration.class,
        WebClientAutoConfiguration.class
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
@Disabled("TODO #460")
class SpringBootJpaDeadLetteringIT extends AbstractDeadLetteringEventIT {

    @Autowired
    private EntityManagerFactory entityManagerFactory;
    @Autowired
    private UnitOfWorkFactory unitOfWorkFactory;
    @Autowired
    private Converter converter;
    @Autowired
    private EventConverter eventConverter;
    @Autowired
    private SequencedDeadLetterQueueFactory deadLetterQueueFactory;
    @Autowired
    private Configuration configuration;

    private JpaSequencedDeadLetterQueue<EventMessage> jpaDeadLetterQueue;

    @Override
    protected JpaSequencedDeadLetterQueue<EventMessage> buildDeadLetterQueue() {
        jpaDeadLetterQueue = (JpaSequencedDeadLetterQueue<EventMessage>) deadLetterQueueFactory
                .create(PROCESSING_GROUP, configuration);
        return jpaDeadLetterQueue;
    }

    /**
     * Delegates to the auto-configured {@link TransactionManager} (a
     * {@link org.axonframework.extension.spring.messaging.unitofwork.SpringTransactionManager}) which automatically
     * registers {@link JpaTransactionalExecutorProvider#SUPPLIER_KEY} on the
     * {@link org.axonframework.messaging.core.unitofwork.ProcessingContext}, exercising the same code path as
     * production.
     */
    @Override
    protected UnitOfWorkFactory buildUnitOfWorkFactory() {
        return unitOfWorkFactory;
    }

    /**
     * Clears stale dead-letter entries from the database before each test method. Although
     * {@code @DirtiesContext(AFTER_EACH_TEST_METHOD)} recreates the Spring context (and thus the embedded HSQLDB), an
     * explicit truncation ensures a clean DLQ table even if context recreation doesn't happen for inherited
     * {@code @Nested} test classes.
     * <p>
     * Uses JPQL (entity name) instead of native SQL because Spring Boot's default
     * {@code CamelCaseToUnderscoresNamingStrategy} maps the entity {@code DeadLetterEntry} to the table
     * {@code dead_letter_entry}.
     */
    @BeforeEach
    void cleanDatabase() {
        EntityManager em = entityManagerFactory.createEntityManager();
        em.getTransaction().begin();
        em.createQuery("DELETE FROM DeadLetterEntry").executeUpdate();
        em.getTransaction().commit();
        em.close();
    }

    /**
     * Verifies that dead letters inserted out of sequence-index order (reverse: 63 → 0) are returned by
     * {@link JpaSequencedDeadLetterQueue#deadLetterSequence} in correct ascending sequence-index order (0 → 63).
     * <p>
     * This test bypasses the normal DLQ enqueue API and directly persists {@link DeadLetterEntry} entities via JPA,
     * exercising the database ordering clause ({@code ORDER BY sequenceIndex}) independently of the enqueue path.
     */
    @Test
    void deadLetterSequenceReturnsMatchingEnqueuedLettersInInsertOrder() {
        String aggregateId = UUID.randomUUID().toString();
        Map<Integer, GenericDeadLetter<EventMessage>> insertedLetters = new TreeMap<>();

        Iterator<DeadLetter<? extends EventMessage>> resultIterator =
                jpaDeadLetterQueue.deadLetterSequence(aggregateId, null).join().iterator();
        assertThat(resultIterator.hasNext()).isFalse();

        IntStream.range(0, 64)
                 .boxed()
                 .sorted(Collections.reverseOrder())
                 .forEach(i -> {
                     GenericDeadLetter<EventMessage> letter =
                             new GenericDeadLetter<>(aggregateId, asEventMessage(i));
                     insertLetterAtIndex(aggregateId, letter, i);
                     insertedLetters.put(i, letter);
                 });

        resultIterator = jpaDeadLetterQueue.deadLetterSequence(aggregateId, null).join().iterator();
        for (Map.Entry<Integer, GenericDeadLetter<EventMessage>> entry : insertedLetters.entrySet()) {
            Integer sequenceIndex = entry.getKey();
            Supplier<String> assertMessageSupplier = () -> "Failed asserting event [" + sequenceIndex + "]";
            assertThat(resultIterator.hasNext()).as(assertMessageSupplier).isTrue();

            GenericDeadLetter<EventMessage> expected = entry.getValue();
            DeadLetter<? extends EventMessage> result = resultIterator.next();
            assertThat(result).as(assertMessageSupplier).isInstanceOf(JpaDeadLetter.class);
            JpaDeadLetter<? extends EventMessage> actual = (JpaDeadLetter<? extends EventMessage>) result;

            assertThat(actual.getSequenceIdentifier()).as(assertMessageSupplier).isEqualTo(expected.getSequenceIdentifier());
            // The JPA DLQ stores payloads as serialized byte[]; convert back for comparison.
            Object actualPayload = eventConverter.convert(actual.message().payload(),
                                                          expected.message().payload().getClass());
            assertThat(actualPayload).as(assertMessageSupplier).isEqualTo(expected.message().payload());
            assertThat(result.cause()).as(assertMessageSupplier).isEmpty();
            assertThat(actual.diagnostics()).as(assertMessageSupplier).isEqualTo(expected.diagnostics());
            assertThat(actual.getIndex()).as(assertMessageSupplier).isEqualTo(sequenceIndex.longValue());
        }
    }

    /**
     * Directly persists a {@link DeadLetterEntry} at the given {@code index} within the sequence identified by
     * {@code aggregateId}. This bypasses the DLQ's enqueue API to control the exact sequence index for ordering tests.
     */
    private void insertLetterAtIndex(String aggregateId, DeadLetter<EventMessage> letter, int index) {
        EntityManager em = entityManagerFactory.createEntityManager();
        em.getTransaction().begin();
        EventMessageDeadLetterJpaConverter jpaConverter = new EventMessageDeadLetterJpaConverter();
        DeadLetterEventEntry eventEntry = jpaConverter.convert(letter.message(), null, eventConverter, converter);
        DeadLetterEntry deadLetter = new DeadLetterEntry(
                PROCESSING_GROUP,
                aggregateId,
                index,
                eventEntry,
                letter.enqueuedAt(),
                letter.lastTouched(),
                letter.cause().orElse(null),
                letter.diagnostics(),
                converter
        );
        em.persist(deadLetter);
        em.getTransaction().commit();
        em.close();
    }
}

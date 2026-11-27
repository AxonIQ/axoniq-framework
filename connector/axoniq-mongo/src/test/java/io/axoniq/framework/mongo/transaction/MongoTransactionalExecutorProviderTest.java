/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.mongo.transaction;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import io.axoniq.framework.mongo.util.MongoMocks;
import org.axonframework.common.tx.TransactionalExecutor;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.*;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link MongoTransactionalExecutorProvider}.
 *
 * @author Steven van Beelen
 */
class MongoTransactionalExecutorProviderTest {

    private MongoClient mongoClient;
    private MongoTransactionalExecutorProvider testSubject;

    @BeforeEach
    void setUp() {
        mongoClient = MongoMocks.client();
        testSubject = new MongoTransactionalExecutorProvider(mongoClient);
    }

    @Nested
    class WithoutProcessingContext {

        @Test
        void providesAnExecutorWithASessionOfItsOwn() throws Exception {
            // given
            TransactionalExecutor<ClientSession> executor = testSubject.getTransactionalExecutor(null);

            // when
            Boolean hasTransaction = executor.apply(ClientSession::hasActiveTransaction).get(10, TimeUnit.SECONDS);

            // then
            assertThat(hasTransaction).isFalse();
        }

        @Test
        void startsANewSessionForEveryInvocation() throws Exception {
            // given
            TransactionalExecutor<ClientSession> executor = testSubject.getTransactionalExecutor(null);

            // when
            ClientSession first = executor.apply(session -> session).get(10, TimeUnit.SECONDS);
            ClientSession second = executor.apply(session -> session).get(10, TimeUnit.SECONDS);

            // then
            assertThat(first).isNotSameAs(second);
        }
    }

    @Nested
    class WithProcessingContext {

        @Test
        void providesTheExecutorBoundToTheProcessingContext() throws Exception {
            // given
            var context = MongoSessionContexts.withSession(mongoClient);

            // when
            TransactionalExecutor<ClientSession> executor = testSubject.getTransactionalExecutor(context);
            ClientSession first = executor.apply(session -> session).get(10, TimeUnit.SECONDS);
            ClientSession second = testSubject.getTransactionalExecutor(context)
                                              .apply(session -> session).get(10, TimeUnit.SECONDS);

            // then
            assertThat(first).isSameAs(second);
        }

        @Test
        void findsNoExecutorWhenNoneIsBoundToTheProcessingContext() {
            // given / when / then
            assertThat(testSubject.findTransactionalExecutor(new StubProcessingContext())).isNull();
        }

        @Test
        void failsToGetAnExecutorWhenNoneIsBoundToTheProcessingContext() {
            // given / when / then
            assertThatThrownBy(() -> testSubject.getTransactionalExecutor(new StubProcessingContext()))
                    .isInstanceOf(IllegalStateException.class);
        }
    }
}

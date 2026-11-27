/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot;

import io.axoniq.framework.mongo.deadletter.MongoSequencedDeadLetterQueueConfiguration;
import io.axoniq.framework.mongo.token.MongoTokenStoreConfiguration;
import org.junit.jupiter.api.*;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link MongoProperties}.
 *
 * @author Steven van Beelen
 */
class MongoPropertiesTest {

    private final ApplicationContextRunner contextRunner =
            new ApplicationContextRunner().withUserConfiguration(PropertiesConfig.class);

    @Nested
    class DeadLetterQueue {

        @Test
        void defaultsDescribeTheDefaultConfigurationOfTheQueue() {
            // given / when
            MongoSequencedDeadLetterQueueConfiguration result =
                    new MongoProperties().getDeadLetterQueue().toConfiguration();

            // then
            assertThat(result).isEqualTo(MongoSequencedDeadLetterQueueConfiguration.DEFAULT);
        }

        @Test
        void bindsEveryProperty() {
            contextRunner.withPropertyValues("axon.mongo.dead-letter-queue.max-sequences=5",
                                             "axon.mongo.dead-letter-queue.max-sequence-size=6",
                                             "axon.mongo.dead-letter-queue.query-page-size=7",
                                             "axon.mongo.dead-letter-queue.claim-duration=2m",
                                             "axon.mongo.dead-letter-queue.ensure-indexes=false",
                                             "axon.mongo.dead-letter-queue.allow-disk-use=false")
                         .run(context -> {
                             // given / when
                             MongoSequencedDeadLetterQueueConfiguration result =
                                     context.getBean(MongoProperties.class).getDeadLetterQueue().toConfiguration();

                             // then
                             assertThat(result.maxSequences()).isEqualTo(5);
                             assertThat(result.maxSequenceSize()).isEqualTo(6);
                             assertThat(result.queryPageSize()).isEqualTo(7);
                             assertThat(result.claimDuration()).isEqualTo(Duration.ofMinutes(2));
                             assertThat(result.ensureIndexes()).isFalse();
                             assertThat(result.allowDiskUse()).isFalse();
                         });
        }

        @Test
        void keepsTheDefaultConverter() {
            // given / when
            MongoSequencedDeadLetterQueueConfiguration result =
                    new MongoProperties().getDeadLetterQueue().toConfiguration();

            // then
            assertThat(result.converter()).isSameAs(MongoSequencedDeadLetterQueueConfiguration.DEFAULT.converter());
        }
    }

    @Nested
    class TokenStore {

        private static final Duration DEFAULT_CLAIM_TIMEOUT = Duration.ofSeconds(10);

        @Test
        void defaultsDescribeTheDefaultConfigurationOfTheStore() {
            // given / when
            MongoTokenStoreConfiguration result =
                    new MongoProperties().getTokenStore().toConfiguration(DEFAULT_CLAIM_TIMEOUT);

            // then
            assertThat(result).isEqualTo(MongoTokenStoreConfiguration.DEFAULT);
        }

        @Test
        void usesTheGivenClaimTimeout() {
            // given / when
            MongoTokenStoreConfiguration result =
                    new MongoProperties().getTokenStore().toConfiguration(Duration.ofSeconds(42));

            // then
            assertThat(result.claimTimeout()).isEqualTo(Duration.ofSeconds(42));
        }

        @Test
        void bindsEveryProperty() {
            contextRunner.withPropertyValues("axon.mongo.token-store.node-id=node-1",
                                             "axon.mongo.token-store.ensure-indexes=false")
                         .run(context -> {
                             // given / when
                             MongoTokenStoreConfiguration result = context.getBean(MongoProperties.class)
                                                                          .getTokenStore()
                                                                          .toConfiguration(DEFAULT_CLAIM_TIMEOUT);

                             // then
                             assertThat(result.claimTimeout()).isEqualTo(DEFAULT_CLAIM_TIMEOUT);
                             assertThat(result.nodeId()).isEqualTo("node-1");
                             assertThat(result.ensureIndexes()).isFalse();
                         });
        }
    }

    @Configuration
    @EnableConfigurationProperties(MongoProperties.class)
    static class PropertiesConfig {

    }
}

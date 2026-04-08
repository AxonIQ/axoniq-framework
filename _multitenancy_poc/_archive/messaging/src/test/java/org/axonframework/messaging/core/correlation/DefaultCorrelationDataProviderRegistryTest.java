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

package org.axonframework.messaging.core.correlation;

import org.axonframework.common.configuration.Configuration;
import org.junit.jupiter.api.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Test class validating the {@link DefaultCorrelationDataProviderRegistry}.
 *
 * @author Steven van Beelen
 */
class DefaultCorrelationDataProviderRegistryTest {

    private CorrelationDataProviderRegistry testSubject;

    private Configuration config;

    @BeforeEach
    void setUp() {
        testSubject = new DefaultCorrelationDataProviderRegistry();

        config = mock(Configuration.class);
    }

    @Test
    void registeredCorrelationDataProvidersCanBeRetrieved() {
        CorrelationDataProvider testProvider = new MessageOriginProvider();
        testSubject.registerProvider(c -> testProvider);

        List<CorrelationDataProvider> providers = testSubject.correlationDataProviders(config);
        assertThat(providers).size().isEqualTo(1);
        assertThat(providers).contains(testProvider);
    }

    @Test
    void registeredCorrelationDataProvidersAreCreatedOnlyOnce() {
        AtomicInteger counter = new AtomicInteger(0);
        testSubject.registerProvider(c -> {
            counter.incrementAndGet();
            return new MessageOriginProvider();
        });

        testSubject.correlationDataProviders(config);
        testSubject.correlationDataProviders(config);
        testSubject.correlationDataProviders(config);
        assertThat(counter).hasValue(1);
    }
}
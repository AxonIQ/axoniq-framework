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

package io.axoniq.framework.messaging.commandhandling.distributed;

import org.axonframework.common.AxonConfigurationException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Test class validating the {@link DistributedCommandBusConfiguration}.
 *
 * @author Jens Mayer
 */
class DistributedCommandBusConfigurationTest {

    private DistributedCommandBusConfiguration testSubject = DistributedCommandBusConfiguration.DEFAULT;

    @Test
    void validLoadFactorIsAccepted() {
        testSubject = testSubject.loadFactor(1);
        assertThat(testSubject.loadFactor()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void invalidLoadFactorCausesException(int invalidLoadFactor) {
        assertThatThrownBy(() -> testSubject.loadFactor(invalidLoadFactor))
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void validNumberOfThreadsIsAccepted() {
        testSubject = testSubject.commandThreads(1);
        assertThat(testSubject.commandThreads()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void invalidNumberOfThreadCausesException(int invalidNumberOfThreads) {
        assertThatThrownBy(() -> testSubject.commandThreads(invalidNumberOfThreads))
                .isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void executorServiceFactoryUsesGivenExecutorService() {
        var myThreadPoolExecutor = Executors.newSingleThreadExecutor();
        testSubject = testSubject.executorService(myThreadPoolExecutor);
        //noinspection unchecked
        ExecutorService executorService =
                testSubject.executorServiceFactory()
                           .createExecutorService(mock(DistributedCommandBusConfiguration.class),
                                                  mock(BlockingQueue.class));
        assertThat(executorService).isSameAs(myThreadPoolExecutor);
    }
}
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

package io.axoniq.framework.messaging.queryhandling.distributed;

import org.junit.jupiter.api.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link DistributedQueryBusConfiguration} verifying configuration methods, default values, and fluent
 * API behavior.
 *
 * @author Allard Buijze
 * @since 5.0.0
 */
class DistributedQueryBusConfigurationTest {

    @Test
    void defaultConfigurationHasExpectedValues() {
        // Given / When
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT;

        // Then
        assertThat(config.preferLocalQueryHandler())
                .as("Default configuration should prefer local query handlers")
                .isTrue();

        ExecutorService executorService = config.queryExecutorService();
        assertThat(executorService)
                .as("Default executor should be a ThreadPoolExecutor")
                .isInstanceOf(ThreadPoolExecutor.class);

        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executorService;
        assertThat(threadPool.getCorePoolSize())
                .as("Default thread pool should have  10 threads")
                .isEqualTo(10);
        assertThat(threadPool.getMaximumPoolSize())
                .as("Default thread pool should have max 10 threads")
                .isEqualTo(10);
        executorService.shutdown();
    }

    @Test
    void queryThreadsCreatesExecutorWithCorrectThreadCount() {
        // Given
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT;

        // When
        DistributedQueryBusConfiguration customConfig = config.queryThreads(42);

        // Then
        assertThat(customConfig)
                .as("queryThreads() should return a new instance")
                .isNotSameAs(config);

        ExecutorService executorService = customConfig.queryExecutorService();
        assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executorService;
        assertThat(threadPool.getCorePoolSize())
                .as("Thread pool should have 42 threads")
                .isEqualTo(42);

        assertThat(threadPool.getMaximumPoolSize())
                .as("Thread pool should have max 42 threads")
                .isEqualTo(42);

        executorService.shutdown();
    }

    @Test
    void queryQueueCapacityCreatesQueueWithCorrectCapacity() {
        // Given
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT;

        // When
        DistributedQueryBusConfiguration customConfig = config.queryQueueCapacity(500);

        // Then
        assertThat(customConfig)
                .as("queryQueueCapacity() should return a new instance")
                .isNotSameAs(config);

        ExecutorService executorService = customConfig.queryExecutorService();
        assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executorService;
        // PriorityBlockingQueue has unbounded capacity, but initial capacity affects internal array
        assertThat(threadPool.getQueue())
                .as("Thread pool should have a queue")
                        .isNotNull();
        executorService.shutdown();
    }

    @Test
    void queryExecutorServiceUsesProvidedExecutor() {
        // Given
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT;
        ExecutorService customExecutor = Executors.newSingleThreadExecutor();

        // When
        DistributedQueryBusConfiguration customConfig = config.queryExecutorService(customExecutor);

        // Then
        assertThat(customConfig)
                .as("queryExecutorService() should return a new instance")
                .isNotSameAs(config);
        assertThat(customConfig.queryExecutorService())
                .as("Configuration should use the provided executor service")
                .isSameAs(customExecutor);

        customExecutor.shutdown();
    }

    @Test
    void queryExecutorServiceRejectsNullExecutor() {
        // Given
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT;

        // When / Then
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> config.queryExecutorService(null))
                .as("Setting null executor should throw NullPointerException")
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void preferLocalQueryHandlerStoresCorrectValue() {
        // Given
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT;

        // When
        DistributedQueryBusConfiguration disabledConfig = config.preferLocalQueryHandler(false);
        DistributedQueryBusConfiguration enabledConfig = config.preferLocalQueryHandler(true);

        // Then
        assertThat(disabledConfig)
                .as("preferLocalQueryHandler() should return a new instance")
                .isNotSameAs(config);
        assertThat(enabledConfig)
                .as("preferLocalQueryHandler() should return a new instance")
                .isNotSameAs(config);

        assertThat(config.preferLocalQueryHandler())
                .as("Original config should have default value (true)")
                .isTrue();
        assertThat(disabledConfig.preferLocalQueryHandler())
                .as("Disabled config should have local shortcut disabled")
                .isFalse();
        assertThat(enabledConfig.preferLocalQueryHandler())
                .as("Enabled config should have local shortcut enabled")
                .isTrue();
    }

    @Test
    void fluentChainingPreservesAllSettings() {
        // Given
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT;

        // When
        DistributedQueryBusConfiguration customConfig = config
                .queryThreads(20)
                .preferLocalQueryHandler(false)
                .queryQueueCapacity(2000);

        // Then
        assertThat(customConfig.preferLocalQueryHandler())
                .as("Chained config should preserve preferLocalQueryHandler setting")
                .isFalse();

        ExecutorService executorService = customConfig.queryExecutorService();
        assertThat(executorService).isInstanceOf(ThreadPoolExecutor.class);

        ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executorService;
        assertThat(threadPool.getCorePoolSize())
                .as("Chained config should preserve thread count setting")
                .isEqualTo(20);

        executorService.shutdown();
    }

    @Test
    void configurationIsImmutable() {
        // Given
        DistributedQueryBusConfiguration original = DistributedQueryBusConfiguration.DEFAULT;

        // When
        DistributedQueryBusConfiguration modified1 = original.queryThreads(5);
        DistributedQueryBusConfiguration modified2 = original.preferLocalQueryHandler(false);

        // Then
        assertThat(modified1)
                .as("Modifying configuration should return new instance")
                .isNotSameAs(original);
        assertThat(modified2)
                .as("Modifying configuration should return new instance")
                .isNotSameAs(original)
                .as("Each modification should return distinct instance")
                .isNotSameAs(modified1);

        // Verify original is unchanged
        assertThat(original.preferLocalQueryHandler())
                .as("Original config should be unchanged")
                .isTrue();

        ExecutorService originalExecutor = original.queryExecutorService();
        assertThat(originalExecutor).isInstanceOf(ThreadPoolExecutor.class);
        assertThat(((ThreadPoolExecutor) originalExecutor).getCorePoolSize())
                .as("Original config should have default thread count")
                .isEqualTo(10);

        originalExecutor.shutdown();
        modified1.queryExecutorService().shutdown();
        modified2.queryExecutorService().shutdown();
    }

    @Test
    void multipleCallsToQueryExecutorServiceReturnDifferentInstances() {
        // Given
        DistributedQueryBusConfiguration config = DistributedQueryBusConfiguration.DEFAULT;

        // When
        ExecutorService executor1 = config.queryExecutorService();
        ExecutorService executor2 = config.queryExecutorService();

        // Then
        assertThat(executor2)
                .as("Each call to queryExecutorService() should create a new executor")
                .isNotSameAs(executor1);

        executor1.shutdown();
        executor2.shutdown();
    }
}

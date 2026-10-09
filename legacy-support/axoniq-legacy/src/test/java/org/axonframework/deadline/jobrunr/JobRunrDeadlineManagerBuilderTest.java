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

package org.axonframework.deadline.jobrunr;

import org.axonframework.common.AxonConfigurationException;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.deadline.TestScopeDescriptor;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.jobrunr.scheduling.JobScheduler;
import org.jobrunr.storage.InMemoryStorageProvider;
import org.junit.jupiter.api.*;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link JobRunrDeadlineManager.Builder}.
 */
class JobRunrDeadlineManagerBuilderTest {

    private static final String TEST_DEADLINE_NAME = "deadline-name";

    private final JobScheduler jobScheduler = new JobScheduler(new InMemoryStorageProvider());
    private final ScopeAwareProvider scopeAwareProvider = scope -> Stream.empty();

    private JobRunrDeadlineManager.Builder completeBuilder() {
        return JobRunrDeadlineManager.builder()
                                     .scopeAwareProvider(scopeAwareProvider)
                                     .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY)
                                     .jobScheduler(jobScheduler)
                                     .converter(new JacksonConverter());
    }

    @Test
    void whenAllPropertiesAreSetCreatesManager() {
        // when / then
        assertThat(completeBuilder().build()).isNotNull();
    }

    @Test
    void validateNeedsTheUnitOfWorkFactory() {
        // given
        JobRunrDeadlineManager.Builder builder = JobRunrDeadlineManager.builder()
                                                                       .scopeAwareProvider(scopeAwareProvider)
                                                                       .jobScheduler(jobScheduler)
                                                                       .converter(new JacksonConverter());

        // when / then
        assertThatThrownBy(builder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void validateNeedsTheConverter() {
        // given
        JobRunrDeadlineManager.Builder builder =
                JobRunrDeadlineManager.builder()
                                      .scopeAwareProvider(scopeAwareProvider)
                                      .jobScheduler(jobScheduler)
                                      .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY);

        // when / then
        assertThatThrownBy(builder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void settingANullComponentThrows() {
        // given
        JobRunrDeadlineManager.Builder builder = JobRunrDeadlineManager.builder();

        // when / then
        assertThatThrownBy(() -> builder.jobScheduler(null)).isInstanceOf(AxonConfigurationException.class);
        assertThatThrownBy(() -> builder.scopeAwareProvider(null)).isInstanceOf(AxonConfigurationException.class);
        assertThatThrownBy(() -> builder.unitOfWorkFactory(null)).isInstanceOf(AxonConfigurationException.class);
        assertThatThrownBy(() -> builder.converter(null)).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void cancelAllIsNotSupported() {
        // given
        JobRunrDeadlineManager manager = completeBuilder().build();

        // when / then
        assertThatThrownBy(() -> manager.cancelAll(TEST_DEADLINE_NAME))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void cancelAllWithinScopeIsNotSupported() {
        // given
        JobRunrDeadlineManager manager = completeBuilder().build();

        // when / then
        assertThatThrownBy(() -> manager.cancelAllWithinScope(
                TEST_DEADLINE_NAME, new TestScopeDescriptor("aggregate-type", "aggregate-identifier")
        )).isInstanceOf(UnsupportedOperationException.class);
    }
}

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

package org.axonframework.deadline.dbscheduler;

import com.github.kagkarlsson.scheduler.Scheduler;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.ScopeAwareProvider;
import org.axonframework.messaging.core.unitofwork.UnitOfWorkTestUtils;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.jupiter.api.*;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.axonframework.common.util.DbSchedulerTestUtil.getScheduler;

/**
 * Test class validating the {@link DbSchedulerDeadlineManager.Builder}.
 */
class DbSchedulerDeadlineManagerBuilderTest {

    private final ScopeAwareProvider scopeAwareProvider = scope -> Stream.empty();
    private Scheduler scheduler;

    @BeforeEach
    void setUp() {
        JDBCDataSource dataSource = new JDBCDataSource();
        dataSource.setUrl("jdbc:hsqldb:mem:builderTest");
        dataSource.setUser("sa");
        scheduler = getScheduler(dataSource, DbSchedulerDeadlineManager.binaryTask(() -> null));
    }

    @Test
    void whenAllPropertiesAreSetCreatesManager() {
        // when
        DbSchedulerDeadlineManager manager = DbSchedulerDeadlineManager.builder()
                                                                       .scopeAwareProvider(scopeAwareProvider)
                                                                       .unitOfWorkFactory(
                                                                               UnitOfWorkTestUtils.SIMPLE_FACTORY
                                                                       )
                                                                       .scheduler(scheduler)
                                                                       .converter(new JacksonConverter())
                                                                       .build();

        // then
        assertThat(manager).isNotNull();
    }

    @Test
    void validateNeedsTheUnitOfWorkFactory() {
        // given
        DbSchedulerDeadlineManager.Builder builder = DbSchedulerDeadlineManager.builder()
                                                                               .scopeAwareProvider(scopeAwareProvider)
                                                                               .scheduler(scheduler)
                                                                               .converter(new JacksonConverter());

        // when / then
        assertThatThrownBy(builder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @Test
    void validateNeedsTheConverter() {
        // given
        DbSchedulerDeadlineManager.Builder builder =
                DbSchedulerDeadlineManager.builder()
                                          .scopeAwareProvider(scopeAwareProvider)
                                          .scheduler(scheduler)
                                          .unitOfWorkFactory(UnitOfWorkTestUtils.SIMPLE_FACTORY);

        // when / then
        assertThatThrownBy(builder::build).isInstanceOf(AxonConfigurationException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void settingANullComponentThrows() {
        // given
        DbSchedulerDeadlineManager.Builder builder = DbSchedulerDeadlineManager.builder();

        // when / then
        assertThatThrownBy(() -> builder.scheduler(null)).isInstanceOf(AxonConfigurationException.class);
        assertThatThrownBy(() -> builder.scopeAwareProvider(null)).isInstanceOf(AxonConfigurationException.class);
        assertThatThrownBy(() -> builder.unitOfWorkFactory(null)).isInstanceOf(AxonConfigurationException.class);
        assertThatThrownBy(() -> builder.converter(null)).isInstanceOf(AxonConfigurationException.class);
    }
}

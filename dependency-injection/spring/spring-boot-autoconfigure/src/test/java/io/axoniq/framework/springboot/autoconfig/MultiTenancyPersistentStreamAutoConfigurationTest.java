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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.axonserver.connector.event.DefaultPersistentStreamEventSourceFactory;
import io.axoniq.framework.axonserver.connector.event.PersistentStreamEventSourceFactory;
import io.axoniq.framework.messaging.multitenancy.axonserver.eventstreaming.MultiTenantPersistentStreamEventSourceFactory;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link MultiTenancyPersistentStreamAutoConfiguration}.
 *
 * @author Jakob Hatzl
 */
class MultiTenancyPersistentStreamAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MultiTenancyPersistentStreamAutoConfiguration.class,
                                                     PersistentStreamAutoConfiguration.class));

    @Test
    void contributesTheMultiTenantFactoryByDefault() {
        // when no multi-tenancy or Axon Server property is set
        contextRunner.run(context -> {
            // then persistent streams fan out across tenants without any configuration
            assertThat(context).hasSingleBean(PersistentStreamEventSourceFactory.class);
            assertThat(context.getBean(PersistentStreamEventSourceFactory.class))
                    .isInstanceOf(MultiTenantPersistentStreamEventSourceFactory.class);
        });
    }

    @Test
    void fallsBackToTheDefaultFactoryWhenMultiTenancyIsDisabled() {
        // given multi-tenancy is switched off
        contextRunner.withPropertyValues("axon.multitenancy.enabled=false")
                     // when the context starts
                     .run(context -> {
                         // then the single-tenant factory of the persistent stream auto-configuration takes over
                         assertThat(context).hasSingleBean(PersistentStreamEventSourceFactory.class);
                         assertThat(context.getBean(PersistentStreamEventSourceFactory.class))
                                 .isInstanceOf(DefaultPersistentStreamEventSourceFactory.class);
                     });
    }

    @Test
    void fallsBackToTheDefaultFactoryWhenAxonServerIsDisabled() {
        // given Axon Server is disabled, so tenants, being Axon Server contexts, cannot exist
        contextRunner.withPropertyValues("axon.axonserver.enabled=false")
                     // when the context starts
                     .run(context -> {
                         // then the single-tenant factory takes over
                         assertThat(context).hasSingleBean(PersistentStreamEventSourceFactory.class);
                         assertThat(context.getBean(PersistentStreamEventSourceFactory.class))
                                 .isInstanceOf(DefaultPersistentStreamEventSourceFactory.class);
                     });
    }

    @Test
    void contributesNoFactoryWhenTheEventStoreIsDisabled() {
        // given the Axon Server event store is switched off, so there are no persistent streams to consume
        contextRunner.withPropertyValues("axon.axonserver.event-store.enabled=false")
                     // when the context starts
                     .run(context -> assertThat(context).doesNotHaveBean(PersistentStreamEventSourceFactory.class));
    }

    @Test
    void letsAnApplicationSuppliedFactoryTakePrecedence() {
        // given the application declares a factory of its own
        PersistentStreamEventSourceFactory custom = mock(PersistentStreamEventSourceFactory.class);

        // when the context starts
        contextRunner.withBean(PersistentStreamEventSourceFactory.class, () -> custom)
                     // then neither auto-configured factory is contributed
                     .run(context -> {
                         assertThat(context).hasSingleBean(PersistentStreamEventSourceFactory.class);
                         assertThat(context.getBean(PersistentStreamEventSourceFactory.class)).isSameAs(custom);
                     });
    }
}

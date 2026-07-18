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

import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Test class validating the {@link LocalShortcutQueryBusConnectorConfigurationEnhancer}.
 *
 * @author Allard Buijze
 */
class LocalShortcutQueryBusConnectorConfigurationEnhancerTest {

    private final QueryBusConnector mockConnector = mock(QueryBusConnector.class);
    private final LocalQueryDispatchPredicate predicate = (query, context) -> true;

    @Test
    void decoratesConnectorWhenExplicitPredicateIsPresent() {
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(QueryBusConnector.class, c -> mockConnector);
                                       cr.registerComponent(LocalQueryDispatchPredicate.class, c -> predicate);
                                       cr.registerEnhancer(new LocalShortcutQueryBusConnectorConfigurationEnhancer());
                                   })
                                   .build();

        QueryBusConnector connector = config.getComponent(QueryBusConnector.class);
        assertThat(connector).isInstanceOf(LocalShortcutQueryBusConnector.class);
    }

    @Test
    void decoratesConnectorWhenPreferLocalQueryHandlerDefaultsOn() {
        // No predicate and no explicit configuration: the default preferLocalQueryHandler (true) installs the shortcut.
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(QueryBusConnector.class, c -> mockConnector);
                                       cr.registerEnhancer(new LocalShortcutQueryBusConnectorConfigurationEnhancer());
                                   })
                                   .build();

        QueryBusConnector connector = config.getComponent(QueryBusConnector.class);
        assertThat(connector).isInstanceOf(LocalShortcutQueryBusConnector.class);
    }

    @Test
    @SuppressWarnings("removal")
    void doesNotDecorateConnectorWhenPreferLocalQueryHandlerDisabledAndNoPredicate() {
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(QueryBusConnector.class, c -> mockConnector);
                                       cr.registerComponent(
                                               DistributedQueryBusConfiguration.class,
                                               c -> DistributedQueryBusConfiguration.DEFAULT.preferLocalQueryHandler(false)
                                       );
                                       cr.registerEnhancer(new LocalShortcutQueryBusConnectorConfigurationEnhancer());
                                   })
                                   .build();

        QueryBusConnector connector = config.getComponent(QueryBusConnector.class);
        assertThat(connector).isSameAs(mockConnector);
    }

    @Test
    @SuppressWarnings("removal")
    void explicitPredicateWinsRegardlessOfPreferLocalQueryHandlerDisabled() {
        // A registered predicate is authoritative: preferLocalQueryHandler(false) does not veto it.
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(QueryBusConnector.class, c -> mockConnector);
                                       cr.registerComponent(LocalQueryDispatchPredicate.class, c -> predicate);
                                       cr.registerComponent(
                                               DistributedQueryBusConfiguration.class,
                                               c -> DistributedQueryBusConfiguration.DEFAULT.preferLocalQueryHandler(false)
                                       );
                                       cr.registerEnhancer(new LocalShortcutQueryBusConnectorConfigurationEnhancer());
                                   })
                                   .build();

        QueryBusConnector connector = config.getComponent(QueryBusConnector.class);
        assertThat(connector).isInstanceOf(LocalShortcutQueryBusConnector.class);
    }

    @Test
    void noEnhancementsWhenNoQueryBusConnectorPresent() {
        Configuration config =
                MessagingConfigurer.create()
                                   .componentRegistry(cr -> {
                                       cr.registerComponent(LocalQueryDispatchPredicate.class, c -> predicate);
                                       cr.registerEnhancer(new LocalShortcutQueryBusConnectorConfigurationEnhancer());
                                   })
                                   .build();

        assertThat(config.hasComponent(QueryBusConnector.class)).isFalse();
    }
}

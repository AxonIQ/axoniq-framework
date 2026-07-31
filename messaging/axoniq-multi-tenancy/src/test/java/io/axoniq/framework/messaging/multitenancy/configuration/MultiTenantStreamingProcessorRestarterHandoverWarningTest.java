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

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.api.MetadataBasedTenantResolver;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantRouter;
import io.axoniq.framework.messaging.multitenancy.eventsourcing.MultiTenantEventStorageEngine;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.test.appender.ListAppender;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.eventsourcing.eventstore.inmemory.InMemoryEventStorageEngine;
import org.axonframework.eventsourcing.snapshot.inmemory.InMemorySnapshotStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the diagnostic the {@link MultiTenantStreamingProcessorRestarter} emits when nothing hands it a
 * tenant-routing engine to follow.
 * <p>
 * That case leaves the restarter inert: a tenant added at runtime re-opens no stream. Since the only signal an operator
 * gets is the log line, the log line itself is asserted here, in both directions.
 *
 * @author Laura Devriendt
 */
class MultiTenantStreamingProcessorRestarterHandoverWarningTest {

    private static final String WARNING_FRAGMENT = "does not re-open the streams";

    private final StubTenantProvider tenantProvider = new StubTenantProvider();
    private final InMemoryEventStorageEngine tenantEngine = new InMemoryEventStorageEngine();

    private Logger restarterLogger;
    private Level previousLevel;
    private ListAppender appender;

    @BeforeEach
    void attachAppender() {
        restarterLogger = (Logger) LogManager.getLogger(MultiTenantStreamingProcessorRestarter.class);
        previousLevel = restarterLogger.getLevel();
        restarterLogger.setLevel(Level.WARN);
        appender = new ListAppender("MultiTenantStreamingProcessorRestarterHandoverWarning");
        appender.start();
        restarterLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        restarterLogger.removeAppender(appender);
        appender.stop();
        restarterLogger.setLevel(previousLevel);
    }

    @Test
    void warnsAtStartupWhenNothingHandedOverARoutingEngine() {
        AxonConfiguration configuration = configurationWithoutARoutingEngine();
        configuration.start();
        try {
            assertThat(warnings()).hasSize(1);
            assertThat(warnings().getFirst().getMessage().getFormattedMessage()).contains(WARNING_FRAGMENT);
        } finally {
            configuration.shutdown();
        }
    }

    @Test
    void staysSilentWhenAnEngineWasHandedOver() {
        AxonConfiguration configuration = configurationWithoutARoutingEngine();
        MultiTenantEventStorageEngine routingEngine =
                new MultiTenantEventStorageEngine(tenant -> tenantEngine,
                                                  tenant -> new InMemorySnapshotStore(),
                                                  new TenantRouter(new MetadataBasedTenantResolver(), tenantProvider));
        configuration.getComponent(MultiTenantStreamingProcessorRestarter.class).follow(routingEngine);
        configuration.start();
        try {
            assertThat(warnings()).isEmpty();
        } finally {
            configuration.shutdown();
        }
    }

    /**
     * Builds a configuration with multi-tenancy on but no enhancer registering a tenant-routing engine, so nothing
     * hands one to the restarter unless a test does so itself.
     *
     * @return a configuration whose restarter follows nothing of its own accord
     */
    private AxonConfiguration configurationWithoutARoutingEngine() {
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> {
                                      registry.disableEnhancerScanning()
                                              .registerComponent(TenantProvider.class, config -> tenantProvider);
                                      MultiTenancyConfigurationDefaults.registerStreamingProcessorRestarter(registry);
                                  })
                                  .build();
    }

    private List<LogEvent> warnings() {
        return appender.getEvents()
                       .stream()
                       .filter(event -> event.getLevel() == Level.WARN)
                       .toList();
    }
}

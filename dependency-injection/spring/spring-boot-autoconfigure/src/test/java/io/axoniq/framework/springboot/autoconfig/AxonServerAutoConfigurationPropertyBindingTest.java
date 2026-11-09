/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.framework.springboot.util.GrpcServerStub;
import io.axoniq.framework.springboot.util.TcpUtils;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@code axon.axonserver.*} properties are bound onto the
 * {@link AxonServerConfiguration} bean that is available in the Spring Boot application context.
 * <p>
 * These tests describe the externally visible binding contract: property keys, defaults, nested settings, and the
 * single {@code AxonServerConfiguration} bean the rest of the framework injects.
 *
 * @author Steven van Beelen
 */
class AxonServerAutoConfigurationPropertyBindingTest {

    private static final String PREFIX = "axon.axonserver.";

    private ApplicationContextRunner testContext;

    @BeforeEach
    void setUp() {
        testContext = new ApplicationContextRunner()
                .withUserConfiguration(PlainContext.class)
                .withPropertyValues(
                        "axon.tracing.enabled=false",
                        "axon.workflow.enabled=false",
                        "axon.postgresql.enabled=false",
                        "axon.multitenancy.enabled=false",
                        "axon.eventstorage.jpa.polling-interval=0"
                );
    }

    @Nested
    class WithAxonServerDisabled {

        @Test
        void bindsAllTopLevelProperties() {
            // given / when
            testContext.withPropertyValues(
                    PREFIX + "enabled=false",
                    PREFIX + "servers=my-server:8124",
                    PREFIX + "client-id=my-client-id",
                    PREFIX + "component-name=my-component",
                    PREFIX + "token=my-token",
                    PREFIX + "context=my-context",
                    PREFIX + "ssl-enabled=true",
                    PREFIX + "cert-file=/certs/ca.pem",
                    PREFIX + "client-cert-file=/certs/client.pem",
                    PREFIX + "client-key-file=/certs/client.key",
                    PREFIX + "permits=111",
                    PREFIX + "nr-of-new-permits=22",
                    PREFIX + "new-permits-threshold=33",
                    PREFIX + "shortcut-queries-to-local-handlers=true",
                    PREFIX + "command-threads=7",
                    PREFIX + "query-threads=8",
                    PREFIX + "query-response-threads=9",
                    PREFIX + "processors-notification-rate=1234",
                    PREFIX + "processors-notification-initial-delay=4321",
                    PREFIX + "keep-alive-timeout=6000",
                    PREFIX + "keep-alive-time=2000",
                    PREFIX + "snapshot-prefetch=3",
                    PREFIX + "max-message-size=1048576",
                    PREFIX + "max-message-size-warning-threshold=0.5",
                    PREFIX + "command-load-factor=50",
                    PREFIX + "connect-timeout=7000",
                    PREFIX + "reconnect-interval=3000",
                    PREFIX + "force-read-from-leader=true",
                    PREFIX + "force-reconnect-through-servers=false",
                    PREFIX + "connection-management-thread-pool-size=4",
                    PREFIX + "auto-persistent-streams-enabled=true"
            ).run(context -> {
                // then
                assertThat(context).hasSingleBean(AxonServerConfiguration.class);
                AxonServerConfiguration result = context.getBean(AxonServerConfiguration.class);

                assertThat(result.isEnabled()).isFalse();
                assertThat(result.getServers()).isEqualTo("my-server:8124");
                assertThat(result.getClientId()).isEqualTo("my-client-id");
                assertThat(result.getComponentName()).isEqualTo("my-component");
                assertThat(result.getToken()).isEqualTo("my-token");
                assertThat(result.getContext()).isEqualTo("my-context");
                assertThat(result.isSslEnabled()).isTrue();
                assertThat(result.getCertFile()).isEqualTo("/certs/ca.pem");
                assertThat(result.getClientCertFile()).isEqualTo("/certs/client.pem");
                assertThat(result.getClientKeyFile()).isEqualTo("/certs/client.key");
                assertThat(result.getPermits()).isEqualTo(111);
                assertThat(result.getNrOfNewPermits()).isEqualTo(22);
                assertThat(result.getNewPermitsThreshold()).isEqualTo(33);
                assertThat(result.isShortcutQueriesToLocalHandlers()).isTrue();
                assertThat(result.getCommandThreads()).isEqualTo(7);
                assertThat(result.getQueryThreads()).isEqualTo(8);
                assertThat(result.getQueryResponseThreads()).isEqualTo(9);
                assertThat(result.getProcessorsNotificationRate()).isEqualTo(1234);
                assertThat(result.getProcessorsNotificationInitialDelay()).isEqualTo(4321);
                assertThat(result.getKeepAliveTimeout()).isEqualTo(6000);
                assertThat(result.getKeepAliveTime()).isEqualTo(2000);
                assertThat(result.getSnapshotPrefetch()).isEqualTo(3);
                assertThat(result.getMaxMessageSize()).isEqualTo(1048576);
                assertThat(result.getMaxMessageSizeWarningThreshold()).isEqualTo(0.5);
                assertThat(result.getCommandLoadFactor()).isEqualTo(50);
                assertThat(result.getConnectTimeout()).isEqualTo(7000);
                assertThat(result.getReconnectInterval()).isEqualTo(3000);
                assertThat(result.isForceReadFromLeader()).isTrue();
                assertThat(result.isForceReconnectThroughServers()).isFalse();
                assertThat(result.getConnectionManagementThreadPoolSize()).isEqualTo(4);
                assertThat(result.isAutoPersistentStreamsEnabled()).isTrue();
            });
        }

        @Test
        void bindsFlowControlSettings() {
            // given / when
            testContext.withPropertyValues(
                    PREFIX + "enabled=false",
                    PREFIX + "event-flow-control.permits=11",
                    PREFIX + "event-flow-control.nr-of-new-permits=12",
                    PREFIX + "event-flow-control.new-permits-threshold=13",
                    PREFIX + "query-flow-control.permits=21",
                    PREFIX + "query-flow-control.nr-of-new-permits=22",
                    PREFIX + "query-flow-control.new-permits-threshold=23",
                    PREFIX + "command-flow-control.permits=31",
                    PREFIX + "command-flow-control.nr-of-new-permits=32",
                    PREFIX + "command-flow-control.new-permits-threshold=33"
            ).run(context -> {
                // then
                AxonServerConfiguration result = context.getBean(AxonServerConfiguration.class);

                assertThat(result.getEventFlowControl().getPermits()).isEqualTo(11);
                assertThat(result.getEventFlowControl().getNrOfNewPermits()).isEqualTo(12);
                assertThat(result.getEventFlowControl().getNewPermitsThreshold()).isEqualTo(13);
                assertThat(result.getQueryFlowControl().getPermits()).isEqualTo(21);
                assertThat(result.getQueryFlowControl().getNrOfNewPermits()).isEqualTo(22);
                assertThat(result.getQueryFlowControl().getNewPermitsThreshold()).isEqualTo(23);
                assertThat(result.getCommandFlowControl().getPermits()).isEqualTo(31);
                assertThat(result.getCommandFlowControl().getNrOfNewPermits()).isEqualTo(32);
                assertThat(result.getCommandFlowControl().getNewPermitsThreshold()).isEqualTo(33);
            });
        }

        @Test
        void bindsHeartbeatEventStoreAndEventHandlingSettings() {
            // given / when
            testContext.withPropertyValues(
                    PREFIX + "enabled=false",
                    PREFIX + "heartbeat.enabled=false",
                    PREFIX + "heartbeat.interval=2500",
                    PREFIX + "heartbeat.timeout=7500",
                    PREFIX + "event-store.enabled=false",
                    PREFIX + "eventhandling.processors.my-processor.load-balancing-strategy=threadNumber",
                    PREFIX + "eventhandling.processors.my-processor.automatic-balancing=true"
            ).run(context -> {
                // then
                AxonServerConfiguration result = context.getBean(AxonServerConfiguration.class);

                assertThat(result.getHeartbeat().isEnabled()).isFalse();
                assertThat(result.getHeartbeat().getInterval()).isEqualTo(2500);
                assertThat(result.getHeartbeat().getTimeout()).isEqualTo(7500);
                assertThat(result.getEventStore().isEnabled()).isFalse();
                assertThat(result.getEventhandling().getProcessors()).containsOnlyKeys("my-processor");
                AxonServerConfiguration.Eventhandling.ProcessorSettings processor =
                        result.getEventhandling().getProcessors().get("my-processor");
                assertThat(processor.getLoadBalancingStrategy()).isEqualTo("threadNumber");
                assertThat(processor.isAutomaticBalancing()).isTrue();
            });
        }

        @Test
        void bindsPersistentStreamSettings() {
            // given / when
            // The persistent stream autoconfiguration would register event sources that require an Axon Server
            // connection. This test only verifies the binding, so it is excluded.
            testContext.withPropertyValues(
                    "spring.autoconfigure.exclude=" + PersistentStreamAutoConfiguration.class.getName(),
                    PREFIX + "enabled=false",
                    PREFIX + "persistent-streams.stream-one.name=my-stream",
                    PREFIX + "persistent-streams.stream-one.initial-segment-count=4",
                    PREFIX + "persistent-streams.stream-one.thread-count=2",
                    PREFIX + "persistent-streams.stream-one.batch-size=25",
                    PREFIX + "persistent-streams.stream-one.initial-position=HEAD",
                    PREFIX + "persistent-streams.stream-one.filter=payloadType == 'MyEvent'",
                    PREFIX + "persistent-streams.stream-one.sequencing-policy=PropertySequencingPolicy",
                    PREFIX + "persistent-streams.stream-one.sequencing-policy-parameters=orderId,customerId",
                    PREFIX + "persistent-streams.stream-two.name=other-stream"
            ).run(context -> {
                // then
                AxonServerConfiguration result = context.getBean(AxonServerConfiguration.class);

                assertThat(result.getPersistentStreams()).containsOnlyKeys("stream-one", "stream-two");
                AxonServerConfiguration.PersistentStreamSettings streamOne =
                        result.getPersistentStreams().get("stream-one");
                assertThat(streamOne.getName()).isEqualTo("my-stream");
                assertThat(streamOne.getInitialSegmentCount()).isEqualTo(4);
                assertThat(streamOne.getThreadCount()).isEqualTo(2);
                assertThat(streamOne.getBatchSize()).isEqualTo(25);
                assertThat(streamOne.getInitialPosition()).isEqualTo("HEAD");
                assertThat(streamOne.getFilter()).isEqualTo("payloadType == 'MyEvent'");
                assertThat(streamOne.getSequencingPolicy()).isEqualTo("PropertySequencingPolicy");
                assertThat(streamOne.getSequencingPolicyParameters()).containsExactly("orderId", "customerId");
                assertThat(result.getPersistentStreams().get("stream-two").getName()).isEqualTo("other-stream");
            });
        }

        @Test
        void bindsAutoPersistentStreamSettings() {
            // given / when
            testContext.withPropertyValues(
                    PREFIX + "enabled=false",
                    PREFIX + "auto-persistent-streams-enabled=true",
                    PREFIX + "auto-persistent-streams-settings.initial-segment-count=3",
                    PREFIX + "auto-persistent-streams-settings.thread-count=5",
                    PREFIX + "auto-persistent-streams-settings.batch-size=10",
                    PREFIX + "auto-persistent-streams-settings.initial-position=HEAD",
                    PREFIX + "auto-persistent-streams-settings.sequencing-policy=PropertySequencingPolicy",
                    PREFIX + "auto-persistent-streams-settings.sequencing-policy-parameters=aggregateId"
            ).run(context -> {
                // then
                AxonServerConfiguration.PersistentStreamSettings result =
                        context.getBean(AxonServerConfiguration.class).getAutoPersistentStreamsSettings();

                assertThat(result.getInitialSegmentCount()).isEqualTo(3);
                assertThat(result.getThreadCount()).isEqualTo(5);
                assertThat(result.getBatchSize()).isEqualTo(10);
                assertThat(result.getInitialPosition()).isEqualTo("HEAD");
                assertThat(result.getSequencingPolicy()).isEqualTo("PropertySequencingPolicy");
                assertThat(result.getSequencingPolicyParameters()).containsExactly("aggregateId");
            });
        }

        @Test
        void keepsDefaultsOfAxonServerConfigurationWhenNothingIsBound() {
            // given / when
            testContext.withPropertyValues(PREFIX + "enabled=false").run(context -> {
                // then
                assertThat(context).hasSingleBean(AxonServerConfiguration.class);
                AxonServerConfiguration expected = new AxonServerConfiguration();
                expected.setEnabled(false);

                assertThat(context.getBean(AxonServerConfiguration.class))
                        .usingRecursiveComparison()
                        .isEqualTo(expected);
            });
        }

        @Test
        void exposesSingleAxonServerConfigurationBeanForTypeLookupsThroughObjectProvider() {
            // given / when
            testContext.withPropertyValues(PREFIX + "enabled=false", PREFIX + "context=my-context").run(context -> {
                // then
                assertThat(context.getBeanProvider(AxonServerConfiguration.class).getObject().getContext())
                        .isEqualTo("my-context");
            });
        }
    }

    @Nested
    class WithAxonServerEnabled {

        @Test
        void bindsPropertiesOntoTheSingleBeanInsteadOfTheFrameworkDefault() {
            // given
            int port = TcpUtils.findFreePort();

            // when
            testContext.withUserConfiguration(StubContext.class).withPropertyValues(
                    PREFIX + "enabled=true",
                    PREFIX + "servers=" + GrpcServerStub.DEFAULT_HOST + ":" + port,
                    PREFIX + "context=my-context",
                    PREFIX + "token=my-token",
                    PREFIX + "command-threads=7",
                    PREFIX + "heartbeat.interval=2500",
                    PREFIX + "persistent-streams.stream-one.name=my-stream"
            ).run(context -> {
                // then
                // The AxonServerConfigurationEnhancer registers a default AxonServerConfiguration unless the
                // application context already holds one; bound properties must reach the one single bean.
                assertThat(context).hasSingleBean(AxonServerConfiguration.class);
                AxonServerConfiguration result = context.getBean(AxonServerConfiguration.class);

                assertThat(result.getServers()).isEqualTo(GrpcServerStub.DEFAULT_HOST + ":" + port);
                assertThat(result.getContext()).isEqualTo("my-context");
                assertThat(result.getToken()).isEqualTo("my-token");
                assertThat(result.getCommandThreads()).isEqualTo(7);
                assertThat(result.getHeartbeat().getInterval()).isEqualTo(2500);
                assertThat(result.getPersistentStreams()).containsOnlyKeys("stream-one");
            });
        }
    }

    @Configuration
    @EnableAutoConfiguration
    public static class PlainContext {

    }

    @Configuration
    public static class StubContext {

        @Bean(initMethod = "start", destroyMethod = "shutdown")
        public GrpcServerStub grpcServerStub(@Value("${axon.axonserver.servers}") String servers) {
            return new GrpcServerStub(Integer.parseInt(servers.split(":")[1]));
        }
    }
}

/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.springboot;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating the {@link AxonServerProperties}.
 *
 * @author Steven van Beelen
 */
class AxonServerPropertiesTest {

    @Nested
    class ToConfiguration {

        @Test
        void defaultsMatchTheDefaultsOfAxonServerConfiguration() {
            // given
            AxonServerProperties testSubject = new AxonServerProperties();

            // when
            AxonServerConfiguration result = testSubject.toConfiguration();

            // then
            assertThat(result).usingRecursiveComparison().isEqualTo(new AxonServerConfiguration());
        }

        @Test
        void copiesAllProvidedValues() {
            // given
            AxonServerProperties testSubject = new AxonServerProperties();
            testSubject.setEnabled(false);
            testSubject.setServers("my-server:8124");
            testSubject.setClientId("my-client-id");
            testSubject.setComponentName("my-component");
            testSubject.setToken("my-token");
            testSubject.setContext("my-context");
            testSubject.setSslEnabled(true);
            testSubject.setCertFile("ca.pem");
            testSubject.setClientCertFile("client.pem");
            testSubject.setClientKeyFile("client.key");
            testSubject.setPermits(111);
            testSubject.setNrOfNewPermits(22);
            testSubject.setNewPermitsThreshold(33);
            testSubject.setShortcutQueriesToLocalHandlers(true);
            testSubject.setCommandThreads(7);
            testSubject.setQueryThreads(8);
            testSubject.setProcessorsNotificationRate(1234);
            testSubject.setProcessorsNotificationInitialDelay(4321);
            testSubject.setKeepAliveTimeout(6000);
            testSubject.setKeepAliveTime(2000);
            testSubject.setSnapshotPrefetch(3);
            testSubject.setMaxMessageSize(1048576);
            testSubject.setMaxMessageSizeWarningThreshold(0.5);
            testSubject.setCommandLoadFactor(50);
            testSubject.setConnectTimeout(7000);
            testSubject.setReconnectInterval(3000);
            testSubject.setForceReadFromLeader(true);
            testSubject.setForceReconnectThroughServers(false);
            testSubject.setConnectionManagementThreadPoolSize(4);
            testSubject.setAutoPersistentStreamsEnabled(true);

            // when
            AxonServerConfiguration result = testSubject.toConfiguration();

            // then
            assertThat(result.isEnabled()).isFalse();
            assertThat(result.getServers()).isEqualTo("my-server:8124");
            assertThat(result.getClientId()).isEqualTo("my-client-id");
            assertThat(result.getComponentName()).isEqualTo("my-component");
            assertThat(result.getToken()).isEqualTo("my-token");
            assertThat(result.getContext()).isEqualTo("my-context");
            assertThat(result.isSslEnabled()).isTrue();
            assertThat(result.getCertFile()).isEqualTo("ca.pem");
            assertThat(result.getClientCertFile()).isEqualTo("client.pem");
            assertThat(result.getClientKeyFile()).isEqualTo("client.key");
            assertThat(result.getPermits()).isEqualTo(111);
            assertThat(result.getNrOfNewPermits()).isEqualTo(22);
            assertThat(result.getNewPermitsThreshold()).isEqualTo(33);
            assertThat(result.isShortcutQueriesToLocalHandlers()).isTrue();
            assertThat(result.getCommandThreads()).isEqualTo(7);
            assertThat(result.getQueryThreads()).isEqualTo(8);
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
        }

        @Test
        void leavesDerivedFlowControlDefaultsInPlaceWhenOnlyPermitsAreProvided() {
            // given
            AxonServerProperties testSubject = new AxonServerProperties();
            testSubject.setPermits(1000);

            // when
            AxonServerConfiguration result = testSubject.toConfiguration();

            // then
            assertThat(result.getPermits()).isEqualTo(1000);
            assertThat(result.getNewPermitsThreshold()).isEqualTo(500);
            assertThat(result.getNrOfNewPermits()).isEqualTo(500);
            assertThat(result.getEventFlowControl().getPermits()).isEqualTo(1000);
        }

        @Test
        void copiesSpecificFlowControlOnlyForTheMessageStreamsThatProvideIt() {
            // given
            AxonServerProperties testSubject = new AxonServerProperties();
            AxonServerProperties.FlowControlProperties eventFlowControl =
                    new AxonServerProperties.FlowControlProperties();
            eventFlowControl.setPermits(11);
            eventFlowControl.setNrOfNewPermits(12);
            eventFlowControl.setNewPermitsThreshold(13);
            testSubject.setEventFlowControl(eventFlowControl);

            // when
            AxonServerConfiguration result = testSubject.toConfiguration();

            // then
            assertThat(result.getEventFlowControl().getPermits()).isEqualTo(11);
            assertThat(result.getEventFlowControl().getNrOfNewPermits()).isEqualTo(12);
            assertThat(result.getEventFlowControl().getNewPermitsThreshold()).isEqualTo(13);
            assertThat(result.getQueryFlowControl().getPermits()).isEqualTo(result.getPermits());
            assertThat(result.getCommandFlowControl().getPermits()).isEqualTo(result.getPermits());
        }

        @Test
        void copiesHeartbeatEventStoreAndEventHandlingSettings() {
            // given
            AxonServerProperties testSubject = new AxonServerProperties();
            testSubject.getHeartbeat().setEnabled(false);
            testSubject.getHeartbeat().setInterval(2500);
            testSubject.getHeartbeat().setTimeout(7500);
            testSubject.getEventStore().setEnabled(false);
            AxonServerProperties.EventHandlingProperties.ProcessorProperties processor =
                    new AxonServerProperties.EventHandlingProperties.ProcessorProperties();
            processor.setLoadBalancingStrategy("threadNumber");
            processor.setAutomaticBalancing(true);
            testSubject.getEventhandling().getProcessors().put("my-processor", processor);

            // when
            AxonServerConfiguration result = testSubject.toConfiguration();

            // then
            assertThat(result.getHeartbeat().isEnabled()).isFalse();
            assertThat(result.getHeartbeat().getInterval()).isEqualTo(2500);
            assertThat(result.getHeartbeat().getTimeout()).isEqualTo(7500);
            assertThat(result.getEventStore().isEnabled()).isFalse();
            assertThat(result.getEventhandling().getProcessors()).containsOnlyKeys("my-processor");
            assertThat(result.getEventhandling().getProcessors().get("my-processor").getLoadBalancingStrategy())
                    .isEqualTo("threadNumber");
            assertThat(result.getEventhandling().getProcessors().get("my-processor").isAutomaticBalancing()).isTrue();
        }

        @Test
        void copiesPersistentStreamSettings() {
            // given
            AxonServerProperties testSubject = new AxonServerProperties();
            AxonServerProperties.PersistentStreamProperties stream =
                    new AxonServerProperties.PersistentStreamProperties();
            stream.setName("my-stream");
            stream.setInitialSegmentCount(4);
            stream.setThreadCount(2);
            stream.setBatchSize(25);
            stream.setInitialPosition("HEAD");
            stream.setFilter("payloadType == 'MyEvent'");
            stream.setSequencingPolicy("PropertySequencingPolicy");
            stream.setSequencingPolicyParameters(List.of("orderId", "customerId"));
            testSubject.getPersistentStreams().put("stream-one", stream);
            testSubject.getAutoPersistentStreamsSettings().setThreadCount(5);

            // when
            AxonServerConfiguration result = testSubject.toConfiguration();

            // then
            AxonServerConfiguration.PersistentStreamSettings resultStream =
                    result.getPersistentStreams().get("stream-one");
            assertThat(resultStream.getName()).isEqualTo("my-stream");
            assertThat(resultStream.getInitialSegmentCount()).isEqualTo(4);
            assertThat(resultStream.getThreadCount()).isEqualTo(2);
            assertThat(resultStream.getBatchSize()).isEqualTo(25);
            assertThat(resultStream.getInitialPosition()).isEqualTo("HEAD");
            assertThat(resultStream.getFilter()).isEqualTo("payloadType == 'MyEvent'");
            assertThat(resultStream.getSequencingPolicy()).isEqualTo("PropertySequencingPolicy");
            assertThat(resultStream.getSequencingPolicyParameters()).containsExactly("orderId", "customerId");
            assertThat(result.getAutoPersistentStreamsSettings().getThreadCount()).isEqualTo(5);
        }
    }

    /**
     * Guards that {@link AxonServerProperties} and its nested classes keep exposing every property of
     * {@link AxonServerConfiguration}. Without it, a setting added to only one of the two would silently not be
     * configurable through {@code axon.axonserver.*} in Spring Boot.
     */
    @Nested
    class PropertyParity {

        static Stream<Arguments> mirroredTypes() {
            return Stream.of(
                    Arguments.of(AxonServerConfiguration.class, AxonServerProperties.class),
                    Arguments.of(AxonServerConfiguration.FlowControlConfiguration.class,
                                 AxonServerProperties.FlowControlProperties.class),
                    Arguments.of(AxonServerConfiguration.HeartbeatConfiguration.class,
                                 AxonServerProperties.HeartbeatProperties.class),
                    Arguments.of(AxonServerConfiguration.Eventhandling.class,
                                 AxonServerProperties.EventHandlingProperties.class),
                    Arguments.of(AxonServerConfiguration.Eventhandling.ProcessorSettings.class,
                                 AxonServerProperties.EventHandlingProperties.ProcessorProperties.class),
                    Arguments.of(AxonServerConfiguration.EventStoreConfiguration.class,
                                 AxonServerProperties.EventStoreProperties.class),
                    Arguments.of(AxonServerConfiguration.PersistentStreamSettings.class,
                                 AxonServerProperties.PersistentStreamProperties.class)
            );
        }

        @ParameterizedTest
        @MethodSource("mirroredTypes")
        void propertiesExposeTheSamePropertyNamesAsTheirConfiguration(Class<?> configuration, Class<?> properties)
                throws IntrospectionException {
            // given / when / then
            assertThat(propertyNames(properties)).isEqualTo(propertyNames(configuration));
        }

        private static Set<String> propertyNames(Class<?> type) throws IntrospectionException {
            // The default flow control is derived on every call, and is not a property that can be bound.
            Set<String> derivedProperties = Set.of("class", "defaultFlowControlConfiguration");
            return Arrays.stream(Introspector.getBeanInfo(type).getPropertyDescriptors())
                         .filter(descriptor -> descriptor.getReadMethod() != null)
                         .map(PropertyDescriptor::getName)
                         .filter(name -> !derivedProperties.contains(name))
                         .collect(Collectors.toCollection(TreeSet::new));
        }
    }
}

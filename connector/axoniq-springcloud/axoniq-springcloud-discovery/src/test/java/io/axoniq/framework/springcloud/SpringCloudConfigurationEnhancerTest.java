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

package io.axoniq.framework.springcloud;

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.springcloud.discovery.RecordingCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.transport.IncomingCommandGateway;
import io.axoniq.framework.springcloud.transport.RecordingRemoteCommandDispatcher;
import io.axoniq.framework.springcloud.transport.RemoteCommandDispatcher;
import io.axoniq.framework.springcloud.utils.RecordingDiscoveryClient;
import io.axoniq.framework.springcloud.utils.TestServiceInstance;
import org.axonframework.common.configuration.ApplicationConfigurer;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.messaging.commandhandling.CommandMessage;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.core.QualifiedName;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.*;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests what {@link SpringCloudConfigurationEnhancer} contributes to a configuration, and how it reacts to another
 * connector already being configured.
 *
 * @author Allard Buijze
 */
class SpringCloudConfigurationEnhancerTest {

    private SpringCloudMemberRegistry registry;
    private IncomingCommandGateway gateway;
    private RemoteCommandDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        TestServiceInstance localInstance = TestServiceInstance.instance("university", "node-a", 8080);
        registry = new SpringCloudMemberRegistry(new RecordingDiscoveryClient().register("university", localInstance),
                                                 localInstance,
                                                 new RecordingCapabilityDiscoveryMode());
        gateway = new IncomingCommandGateway("node-a", null);
        dispatcher = new RecordingRemoteCommandDispatcher();
    }

    /**
     * Builds a configuration carrying the collaborators the Spring Boot autoconfiguration would have supplied, with
     * this enhancer applied on top.
     */
    private ApplicationConfigurer configurerWithCollaborators() {
        return MessagingConfigurer.create()
                                  .componentRegistry(componentRegistry -> componentRegistry
                                          .disableEnhancerScanning()
                                          .registerEnhancer(new SpringCloudConfigurationEnhancer())
                                          .registerComponent(SpringCloudMemberRegistry.class, c -> registry)
                                          .registerComponent(IncomingCommandGateway.class, c -> gateway)
                                          .registerComponent(RemoteCommandDispatcher.class, c -> dispatcher));
    }

    @Nested
    class WithTheCollaboratorsPresent {

        @Test
        void registersACommandBusConnector() {
            // given / when
            Configuration configuration = configurerWithCollaborators().build();

            // then
            assertThat(configuration.getOptionalComponent(CommandBusConnector.class)).isPresent();
        }
    }

    @Nested
    class WithoutTheCollaborators {

        @Test
        void registersNothingWhenNoRegistryIsPresent() {
            // given — an application that never added Spring Cloud messaging support
            Configuration configuration =
                    MessagingConfigurer.create()
                                       .componentRegistry(componentRegistry -> componentRegistry
                                               .disableEnhancerScanning()
                                               .registerEnhancer(new SpringCloudConfigurationEnhancer()))
                                       .build();

            // when / then
            assertThat(configuration.getOptionalComponent(CommandBusConnector.class)).isEmpty();
        }
    }

    @Nested
    class WithAnotherConnectorAlreadyConfigured {

        @Test
        void leavesTheConfiguredConnectorInPlace() {
            // given — a connector registered before this enhancer runs, as the Axon Server enhancer does
            StubCommandBusConnector configured = new StubCommandBusConnector();
            Configuration configuration =
                    configurerWithCollaborators()
                            .componentRegistry(componentRegistry -> componentRegistry.registerComponent(
                                    CommandBusConnector.class, c -> configured
                            ))
                            .build();

            // then — commands go through the connector that was configured, and no second one is added. The enhancer
            // deliberately does not treat this as a conflict: it is invoked again for every nested registry, by which
            // point the connector it registered itself is visible in the enclosing scope, so the two cases cannot be
            // told apart here.
            assertThat(configuration.getComponent(CommandBusConnector.class)).isSameAs(configured);
        }
    }

    /**
     * Stands in for another {@link CommandBusConnector} — Axon Server's, in practice — already being configured.
     */
    private static class StubCommandBusConnector implements CommandBusConnector {

        @Override
        public CompletableFuture<CommandResultMessage> dispatch(CommandMessage command,
                                                               @Nullable ProcessingContext processingContext) {
            throw new UnsupportedOperationException("Not invoked by these tests.");
        }

        @Override
        public CompletableFuture<Void> subscribe(QualifiedName commandName, int loadFactor) {
            throw new UnsupportedOperationException("Not invoked by these tests.");
        }

        @Override
        public boolean unsubscribe(QualifiedName commandName) {
            throw new UnsupportedOperationException("Not invoked by these tests.");
        }

        @Override
        public void onIncomingCommand(Handler handler) {
            // Nothing to do; this connector is never dispatched to.
        }

        @Override
        public void describeTo(ComponentDescriptor descriptor) {
            // Nothing to describe.
        }
    }
}

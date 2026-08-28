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

package io.axoniq.framework.springboot.springcloud.autoconfig;

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.springcloud.SpringCloudMemberRegistry;
import io.axoniq.framework.springcloud.discovery.CapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.discovery.MemberCapabilitiesController;
import io.axoniq.framework.springcloud.transport.IncomingCommandGateway;
import io.axoniq.framework.springcloud.transport.RemoteCommandDispatcher;
import io.axoniq.framework.springcloud.transport.SpringCloudCommandController;
import org.junit.jupiter.api.*;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests which beans {@link SpringCloudAutoConfiguration} contributes, and under which conditions.
 *
 * @author Allard Buijze
 */
class SpringCloudAutoConfigurationTest {

    private WebApplicationContextRunner contextRunner;

    @BeforeEach
    void setUp() {
        // A web application, as members reach each other over HTTP and the connector refuses to start without one.
        contextRunner = new WebApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(SpringCloudAutoConfiguration.class))
                .withUserConfiguration(DiscoveryConfiguration.class);
    }

    @Nested
    class WithDiscoveryAvailable {

        @Test
        void contributesTheConnectorsCollaborators() {
            contextRunner.run(context -> assertThat(context)
                    .hasSingleBean(CapabilityDiscoveryMode.class)
                    .hasSingleBean(SpringCloudMemberRegistry.class)
                    .hasSingleBean(RemoteCommandDispatcher.class)
                    .hasSingleBean(IncomingCommandGateway.class));
        }

        @Test
        void contributesBothEndpoints() {
            // The endpoints are how members reach each other, so neither is optional.
            contextRunner.run(context -> assertThat(context)
                    .hasSingleBean(SpringCloudCommandController.class)
                    .hasSingleBean(MemberCapabilitiesController.class));
        }

        @Test
        void namesThisMemberAfterItsRegistration() {
            contextRunner.run(context -> assertThat(context).hasSingleBean(IncomingCommandGateway.class));
        }

        @Test
        void leavesTheConnectorItselfToTheConfigurationEnhancer() {
            // The connector is registered through the ConfigurationEnhancer, not as a Spring bean, so that the
            // framework's own decoration of the CommandBus applies to it.
            contextRunner.run(context -> assertThat(context).doesNotHaveBean(CommandBusConnector.class));
        }
    }

    @Nested
    class WhenSwitchedOff {

        @Test
        void contributesNoneOfTheConnectorsBeans() {
            contextRunner.withPropertyValues("axon.springcloud.enabled=false")
                         .run(context -> assertThat(context)
                                 .doesNotHaveBean(SpringCloudMemberRegistry.class)
                                 .doesNotHaveBean(SpringCloudCommandController.class)
                                 .doesNotHaveBean(MemberCapabilitiesController.class));
        }

        @Test
        void stillContributesTheEnhancerThatDisablesTheConnector() {
            // The connector's ConfigurationEnhancer is service-loaded, so switching the connector off takes
            // disabling it explicitly rather than withholding a bean.
            contextRunner.withPropertyValues("axon.springcloud.enabled=false")
                         .run(context -> assertThat(context)
                                 .hasBean("disableSpringCloudConfigurationEnhancer"));
        }
    }

    @Nested
    class WithApplicationSuppliedBeans {

        @Test
        void keepsAnApplicationsOwnDiscoveryMode() {
            contextRunner.withUserConfiguration(CustomDiscoveryModeConfiguration.class)
                         .run(context -> assertThat(context)
                                 .hasSingleBean(CapabilityDiscoveryMode.class)
                                 .getBean(CapabilityDiscoveryMode.class)
                                 .isSameAs(context.getBean("customDiscoveryMode")));
        }

        @Test
        void keepsAnApplicationsOwnRestClient() {
            contextRunner.withUserConfiguration(CustomRestClientConfiguration.class)
                         .run(context -> assertThat(context)
                                 .getBean(SpringCloudAutoConfiguration.REST_CLIENT_BEAN)
                                 .isSameAs(context.getBean("axoniqSpringCloudRestClient")));
        }
    }

    @Nested
    class WithoutDiscovery {

        @Test
        void saysWhatIsMissingRatherThanDistributingNothing() {
            // The beans a discovery implementation supplies are resolved as the connector is built, not through a
            // bean-presence condition, so an application missing one is told what to add rather than left with
            // commands that are silently never distributed.
            new WebApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(SpringCloudAutoConfiguration.class))
                    .run(context -> assertThat(context)
                            .hasFailed()
                            .getFailure()
                            .rootCause()
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("Spring Cloud Discovery implementation")
                            .hasMessageContaining("axon.springcloud.enabled=false"));
        }

        @Test
        void staysOutOfTheWayWhenSwitchedOff() {
            // An application without a discovery implementation, that has switched the connector off, starts fine.
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(SpringCloudAutoConfiguration.class))
                    .withPropertyValues("axon.springcloud.enabled=false")
                    .run(context -> assertThat(context)
                            .hasNotFailed()
                            .doesNotHaveBean(SpringCloudMemberRegistry.class));
        }
    }

    @Nested
    class WithoutAWebApplication {

        @Test
        void refusesToStartRatherThanJoiningTheClusterUnreachable() {
            // Without a web application there is nothing to map the endpoints onto, while this member would still
            // register with discovery and publish its capabilities, leaving other members routing commands to an
            // address that refuses every connection.
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(SpringCloudAutoConfiguration.class))
                    .withUserConfiguration(DiscoveryConfiguration.class)
                    .run(context -> assertThat(context)
                            .hasFailed()
                            .getFailure()
                            .rootCause()
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("not a web application")
                            .hasMessageContaining("axon.springcloud.enabled=false"));
        }

        @Test
        void startsFineWhenTheConnectorIsSwitchedOff() {
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(SpringCloudAutoConfiguration.class))
                    .withUserConfiguration(DiscoveryConfiguration.class)
                    .withPropertyValues("axon.springcloud.enabled=false")
                    .run(context -> assertThat(context)
                            .hasNotFailed()
                            .doesNotHaveBean(SpringCloudCommandController.class)
                            .doesNotHaveBean(MemberCapabilitiesController.class));
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class DiscoveryConfiguration {

        private static final ServiceInstance INSTANCE =
                new DefaultServiceInstance("university-8080", "university", "localhost", 8080, false, Map.of());

        @Bean
        DiscoveryClient discoveryClient() {
            return new DiscoveryClient() {
                @Override
                public String description() {
                    return "test";
                }

                @Override
                public List<ServiceInstance> getInstances(String serviceId) {
                    return List.of(INSTANCE);
                }

                @Override
                public List<String> getServices() {
                    return List.of("university");
                }
            };
        }

        @Bean
        Registration registration() {
            return new Registration() {
                @Override
                public String getServiceId() {
                    return "university";
                }

                @Override
                public String getHost() {
                    return "localhost";
                }

                @Override
                public int getPort() {
                    return 8080;
                }

                @Override
                public boolean isSecure() {
                    return false;
                }

                @Override
                public URI getUri() {
                    return URI.create("http://localhost:8080");
                }

                @Override
                public Map<String, String> getMetadata() {
                    return Map.of();
                }
            };
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomDiscoveryModeConfiguration {

        @Bean
        CapabilityDiscoveryMode customDiscoveryMode() {
            return new io.axoniq.framework.springcloud.discovery.RestCapabilityDiscoveryMode(RestClient.create());
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class CustomRestClientConfiguration {

        @Bean(SpringCloudAutoConfiguration.REST_CLIENT_BEAN)
        RestClient axoniqSpringCloudRestClient() {
            return RestClient.create();
        }
    }
}

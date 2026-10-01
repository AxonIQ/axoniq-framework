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

import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.springboot.SpringCloudProperties;
import io.axoniq.framework.springcloud.command.IncomingCommandInvoker;
import io.axoniq.framework.springcloud.command.RemoteCommandDispatcher;
import io.axoniq.framework.springcloud.command.SpringCloudCommandController;
import io.axoniq.framework.springcloud.discovery.CapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.discovery.MemberCapabilitiesController;
import io.axoniq.framework.springcloud.discovery.RestCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.query.IncomingQueryInvoker;
import io.axoniq.framework.springcloud.query.RemoteQueryDispatcher;
import io.axoniq.framework.springcloud.query.SpringCloudQueryController;
import io.axoniq.framework.springcloud.shared.SpringCloudMemberRegistry;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.junit.jupiter.api.*;

import java.time.Duration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.ReactiveWebApplicationContextRunner;
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
 * @author Steven van Beelen
 */
class SpringCloudAutoConfigurationTest {

    private WebApplicationContextRunner contextRunner;

    @BeforeEach
    void setUp() {
        // A web application, as members reach each other over HTTP and the connector refuses to start without one.
        contextRunner = new WebApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(SpringCloudAutoConfiguration.class))
                .withUserConfiguration(DiscoveryConfiguration.class)
                // The converter the application converts its messages with, which the framework's own
                // autoconfiguration contributes and this connector writes the wire format with.
                .withBean(MessageConverter.class, () -> new DelegatingMessageConverter(new JacksonConverter()))
                .withPropertyValues("axon.springcloud.enabled=true");
    }

    @Nested
    class BindingProperties {

        @Test
        void bindsEveryPropertyTheConnectorExposes() {
            // given every property set away from its default
            contextRunner.withPropertyValues("axon.springcloud.command-endpoint=/custom/command",
                                             "axon.springcloud.query-endpoint=/custom/query",
                                             "axon.springcloud.capabilities-endpoint=/custom/capabilities",
                                             "axon.springcloud.command-reply-timeout=11s",
                                             "axon.springcloud.query-timeout=12m",
                                             "axon.springcloud.query-response-timeout=13m",
                                             "axon.springcloud.query-buffer-size=7",
                                             "axon.springcloud.capabilities-timeout=3s",
                                             "axon.springcloud.ignore-period=14s",
                                             "axon.springcloud.context-root-metadata-property-name=root")
                         // when / then each one reaches the properties the components are built from
                         .run(context -> {
                             SpringCloudProperties properties = context.getBean(SpringCloudProperties.class);
                             assertThat(properties.getCommandEndpoint()).isEqualTo("/custom/command");
                             assertThat(properties.getQueryEndpoint()).isEqualTo("/custom/query");
                             assertThat(properties.getCapabilitiesEndpoint()).isEqualTo("/custom/capabilities");
                             assertThat(properties.getCommandReplyTimeout()).isEqualTo(Duration.ofSeconds(11));
                             assertThat(properties.getQueryTimeout()).isEqualTo(Duration.ofMinutes(12));
                             assertThat(properties.getQueryResponseTimeout()).isEqualTo(Duration.ofMinutes(13));
                             assertThat(properties.getQueryBufferSize()).isEqualTo(7);
                             assertThat(properties.getCapabilitiesTimeout()).isEqualTo(Duration.ofSeconds(3));
                             assertThat(properties.getIgnorePeriod()).isEqualTo(Duration.ofSeconds(14));
                             assertThat(properties.getContextRootMetadataPropertyName()).isEqualTo("root");
                         });
        }

        @Test
        void defaultsTheEndpointsToWhatTheControllersServe() {
            // when nothing is configured
            contextRunner.run(context -> {
                SpringCloudProperties properties = context.getBean(SpringCloudProperties.class);

                // then the defaults are the paths the endpoints are actually mapped to, since members reaching each
                // other depends on the two agreeing
                assertThat(properties.getCommandEndpoint())
                        .isEqualTo(SpringCloudCommandController.DEFAULT_COMMAND_ENDPOINT);
                assertThat(properties.getQueryEndpoint())
                        .isEqualTo(SpringCloudQueryController.DEFAULT_QUERY_ENDPOINT);
                assertThat(properties.getCapabilitiesEndpoint())
                        .isEqualTo(RestCapabilityDiscoveryMode.DEFAULT_CAPABILITIES_ENDPOINT);
            });
        }

        @Test
        void servesInstancesFromTheRootWhenNoContextRootPropertyIsNamed() {
            // when
            contextRunner.run(context -> assertThat(context.getBean(SpringCloudProperties.class)
                                                           .getContextRootMetadataPropertyName()).isNull());
        }
    }

    @Nested
    class WithDiscoveryAvailable {

        @Test
        void contributesTheConnectorsCollaborators() {
            contextRunner.run(context -> assertThat(context)
                    .hasSingleBean(CapabilityDiscoveryMode.class)
                    .hasSingleBean(SpringCloudMemberRegistry.class)
                    .hasSingleBean(RemoteCommandDispatcher.class)
                    .hasSingleBean(IncomingCommandInvoker.class));
        }

        @Test
        void contributesEveryEndpoint() {
            // The endpoints are how members reach each other, so none of them is optional.
            contextRunner.run(context -> assertThat(context)
                    .hasSingleBean(SpringCloudCommandController.class)
                    .hasSingleBean(SpringCloudQueryController.class)
                    .hasSingleBean(MemberCapabilitiesController.class));
        }

        @Test
        void contributesTheQueryCollaborators() {
            contextRunner.run(context -> assertThat(context)
                    .hasSingleBean(IncomingQueryInvoker.class)
                    .hasSingleBean(RemoteQueryDispatcher.class));
        }

        @Test
        void saysWhatIsMissingWhenNothingCanConvertTheWireFormat() {
            // The connector writes what members send each other with the application's MessageConverter, so it
            // cannot be built without one rather than starting and failing every query.
            new WebApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(SpringCloudAutoConfiguration.class))
                    .withUserConfiguration(DiscoveryConfiguration.class)
                    .withPropertyValues("axon.springcloud.enabled=true")
                    .run(context -> assertThat(context)
                            .hasFailed()
                            .getFailure()
                            .hasMessageContaining(MessageConverter.class.getSimpleName()));
        }

        @Test
        void namesThisMemberAfterItsRegistration() {
            contextRunner.run(context -> assertThat(context).hasSingleBean(IncomingCommandInvoker.class));
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
                                 .isSameAs(CustomRestClientConfiguration.APPLICATION_REST_CLIENT));
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
                    .withBean(MessageConverter.class, () -> new DelegatingMessageConverter(new JacksonConverter()))
                    .withPropertyValues("axon.springcloud.enabled=true")
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
                    .withBean(MessageConverter.class, () -> new DelegatingMessageConverter(new JacksonConverter()))
                    .withPropertyValues("axon.springcloud.enabled=true")
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

    @Nested
    class OnAReactiveWebApplication {

        @Test
        void refusesToStartRatherThanAdvertisingWhatItCannotAnswer() {
            // given / when — the endpoints members reach each other on are Spring MVC endpoints, which a reactive
            // stack does not map, so such a member would advertise what it handles while answering nothing
            new ReactiveWebApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(SpringCloudAutoConfiguration.class))
                    .withUserConfiguration(DiscoveryConfiguration.class)
                    .withBean(MessageConverter.class, () -> new DelegatingMessageConverter(new JacksonConverter()))
                    .withPropertyValues("axon.springcloud.enabled=true")
                    .run(context -> assertThat(context)
                            .hasFailed()
                            .getFailure()
                            .rootCause()
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessageContaining("WebFlux")
                            .hasMessageContaining("axon.springcloud.enabled=false"));
        }

        @Test
        void startsFineWhenTheConnectorIsSwitchedOff() {
            new ReactiveWebApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(SpringCloudAutoConfiguration.class))
                    .withUserConfiguration(DiscoveryConfiguration.class)
                    .withPropertyValues("axon.springcloud.enabled=false")
                    .run(context -> assertThat(context).hasNotFailed());
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

        static final RestClient APPLICATION_REST_CLIENT = RestClient.create();

        @Bean(SpringCloudAutoConfiguration.REST_CLIENT_BEAN)
        RestClient axoniqSpringCloudRestClient() {
            return APPLICATION_REST_CLIENT;
        }
    }
}

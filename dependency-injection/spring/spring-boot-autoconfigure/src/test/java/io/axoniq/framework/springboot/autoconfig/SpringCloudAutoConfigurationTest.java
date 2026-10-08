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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.axoniq.framework.messaging.commandhandling.distributed.CommandBusConnector;
import io.axoniq.framework.springboot.SpringCloudProperties;
import io.axoniq.framework.springcloud.command.IncomingCommandInvoker;
import io.axoniq.framework.springcloud.command.RemoteCommandDispatcher;
import io.axoniq.framework.springcloud.command.SpringCloudCommandController;
import io.axoniq.framework.springcloud.discovery.CapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.discovery.MemberCapabilitiesController;
import io.axoniq.framework.springcloud.discovery.RestCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.query.IncomingQueryInvoker;
import io.axoniq.framework.springcloud.query.QueryDispatchResponse;
import io.axoniq.framework.springcloud.query.RemoteQueryDispatcher;
import io.axoniq.framework.springcloud.query.RemoteQueryDispatcher.SubscriptionListener;
import io.axoniq.framework.springcloud.query.SpringCloudQueryController;
import io.axoniq.framework.springcloud.routing.Member;
import io.axoniq.framework.springcloud.shared.SpringCloudMemberDiscovery;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageStream;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.axonframework.messaging.queryhandling.GenericQueryMessage;
import org.axonframework.messaging.queryhandling.QueryMessage;
import org.axonframework.messaging.queryhandling.QueryResponseMessage;
import org.junit.jupiter.api.*;
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
import org.springframework.context.annotation.Import;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
                                             "axon.springcloud.context-root-metadata-property-name=root",
                                             "axon.springcloud.capabilities-refresh-interval=15s")
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
                             assertThat(properties.getCapabilitiesRefreshInterval())
                                     .isEqualTo(Duration.ofSeconds(15));
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
        void refreshesCapabilitiesEveryThirtySecondsByDefault() {
            // when
            contextRunner.run(context -> assertThat(context.getBean(SpringCloudProperties.class)
                                                           .getCapabilitiesRefreshInterval())
                    .isEqualTo(Duration.ofSeconds(30)));
        }

        @Test
        void bindsTheRefreshIntervalInWhicheverUnitItIsGivenIn() {
            // when
            contextRunner.withPropertyValues("axon.springcloud.capabilities-refresh-interval=1m")
                         .run(context -> assertThat(context.getBean(SpringCloudProperties.class)
                                                           .getCapabilitiesRefreshInterval())
                                 .isEqualTo(Duration.ofSeconds(60)));
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
                    .hasSingleBean(SpringCloudMemberDiscovery.class)
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
    class WithoutARegistration {

        @Test
        void startsWithADiscoveryImplementationThatDoesNotRegisterTheApplication() {
            // given — a discovery implementation like Spring Cloud Kubernetes supplies a DiscoveryClient, but leaves
            // registration to the platform
            new WebApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(SpringCloudAutoConfiguration.class))
                    .withUserConfiguration(DiscoveryClientConfiguration.class)
                    .withBean(MessageConverter.class, () -> new DelegatingMessageConverter(new JacksonConverter()))
                    .withPropertyValues("axon.springcloud.enabled=true")
                    // when / then — members identify themselves through the capabilities endpoint instead
                    .run(context -> assertThat(context)
                            .hasNotFailed()
                            .doesNotHaveBean(Registration.class)
                            .hasSingleBean(SpringCloudMemberDiscovery.class));
        }
    }

    @Nested
    class WhenSwitchedOff {

        @Test
        void contributesNoneOfTheConnectorsBeans() {
            contextRunner.withPropertyValues("axon.springcloud.enabled=false")
                         .run(context -> assertThat(context)
                                 .doesNotHaveBean(SpringCloudMemberDiscovery.class)
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
        void runsDiscoveryOnAnApplicationsOwnExecutor() {
            contextRunner.withBean(SpringCloudAutoConfiguration.DISCOVERY_EXECUTOR_BEAN,
                                   ExecutorService.class,
                                   () -> CustomDiscoveryExecutor.EXECUTOR)
                         .run(context -> assertThat(context)
                                 .getBean(SpringCloudAutoConfiguration.DISCOVERY_EXECUTOR_BEAN)
                                 .isSameAs(CustomDiscoveryExecutor.EXECUTOR));
        }

        @Test
        void keepsAnApplicationsOwnRestClient() {
            contextRunner.withUserConfiguration(CustomRestClientConfiguration.class)
                         .run(context -> assertThat(context)
                                 .getBean(SpringCloudAutoConfiguration.REST_CLIENT_BEAN)
                                 .isSameAs(CustomRestClientConfiguration.APPLICATION_REST_CLIENT));
        }

        @Test
        void keepsAnApplicationsOwnQueryRestClient() {
            contextRunner.withUserConfiguration(CustomQueryRestClientConfiguration.class)
                         .run(context -> assertThat(context)
                                 .getBean(SpringCloudAutoConfiguration.QUERY_REST_CLIENT_BEAN)
                                 .isSameAs(CustomQueryRestClientConfiguration.APPLICATION_QUERY_REST_CLIENT));
        }
    }

    /**
     * Queries and subscriptions stream their responses for as long as the answering member has some, which for a
     * subscription is as long as the subscriber wants it. A client built for commands, with a read timeout and a
     * bounded pool of connections, would cut those streams off and let them crowd out every other message.
     */
    @Nested
    class StreamingQueriesToOtherMembers {

        private static final Duration SHORT_READ_TIMEOUT = Duration.ofMillis(300);
        private static final MessageType FIND_COURSE_TYPE = new MessageType("university.FindCourse", "1.0.0");
        private static final String QUERY_PAYLOAD = "{\"id\":\"course-1\"}";

        private final List<Headers> received = new CopyOnWriteArrayList<>();
        private HttpServer otherMember;

        @BeforeEach
        void startAnotherMember() throws IOException {
            otherMember = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
            otherMember.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            otherMember.createContext(SpringCloudQueryController.DEFAULT_QUERY_ENDPOINT,
                                      this::answerOnceTheReadTimeoutHasPassed);
            otherMember.createContext(SpringCloudQueryController.DEFAULT_QUERY_ENDPOINT
                                              + SpringCloudQueryController.SUBSCRIPTION_PATH,
                                      this::keepTheSubscriptionAlive);
            otherMember.start();
        }

        @AfterEach
        void stopTheOtherMember() {
            otherMember.stop(0);
        }

        @Test
        void keepsASubscriptionOpenLongerThanTheCommandReplyTimeout() {
            // given the connector's own client, which commands are given a reply timeout on
            contextRunner.withPropertyValues("axon.springcloud.command-reply-timeout=" + SHORT_READ_TIMEOUT.toMillis()
                                                     + "ms")
                         // when / then
                         .run(context -> assertSubscriptionOutlivesTheReadTimeout(
                                 context.getBean(RemoteQueryDispatcher.class)
                         ));
        }

        @Test
        void keepsASubscriptionOpenLongerThanTheReadTimeoutOfTheApplicationsClient() {
            // given an application client with a read timeout of its own
            contextRunner.withBean(RestClient.Builder.class,
                                   () -> RestClient.builder().requestFactory(readingFor(SHORT_READ_TIMEOUT)))
                         // when / then
                         .run(context -> assertSubscriptionOutlivesTheReadTimeout(
                                 context.getBean(RemoteQueryDispatcher.class)
                         ));
        }

        @Test
        void answersAQueryThatTakesLongerThanTheCommandReplyTimeout() {
            // given
            contextRunner.withPropertyValues("axon.springcloud.command-reply-timeout=" + SHORT_READ_TIMEOUT.toMillis()
                                                     + "ms")
                         .run(context -> {
                             // when a query whose answer takes longer than a command reply may
                             MessageStream<QueryResponseMessage> responses =
                                     context.getBean(RemoteQueryDispatcher.class).dispatch(otherMember(), query());

                             try {
                                 // then the query has its own deadline, so it is not cut off by the command's
                                 await().atMost(Duration.ofSeconds(5))
                                        .until(() -> responses.hasNextAvailable() || responses.error().isPresent());
                                 assertThat(responses.error()).isEmpty();
                                 assertThat(responses.next()).hasValueSatisfying(
                                         entry -> assertThat(entry.message().identifier()).isEqualTo("response-1")
                                 );
                             } finally {
                                 responses.close();
                             }
                         });
        }

        @Test
        void opensSubscriptionsPastTheApplicationsConnectionPoolButThroughItsInterceptors() {
            // given an application client whose transport has no connection left to lend, as a bounded pool held by
            // open subscriptions has, and which adds a header every inter-member request must carry
            ClientHttpRequestFactory exhaustedPool = (uri, method) -> {
                throw new IOException("No connection left in the pool.");
            };
            contextRunner.withBean(RestClient.Builder.class,
                                   () -> RestClient.builder()
                                                   .requestFactory(exhaustedPool)
                                                   .requestInterceptor((request, body, execution) -> {
                                                       request.getHeaders().add("X-Member-Token", "secret");
                                                       return execution.execute(request, body);
                                                   }))
                         .run(context -> {
                             // when
                             AtomicBoolean opened = new AtomicBoolean();
                             MessageStream<QueryResponseMessage> updates =
                                     context.getBean(RemoteQueryDispatcher.class)
                                            .openSubscriptionQueryUpdateStream(otherMember(), query(), 16,
                                                                               listeningFor(opened));

                             try {
                                 // then a subscription holds its connection for as long as it lasts, so it does not
                                 // take one from the pool every other message is sent over
                                 await().atMost(Duration.ofSeconds(5)).untilTrue(opened);
                                 assertThat(received).singleElement().satisfies(
                                         headers -> assertThat(headers.getFirst("X-Member-Token")).isEqualTo("secret")
                                 );
                             } finally {
                                 updates.close();
                             }
                         });
        }

        private static byte[] event(String type, String identifier) throws IOException {
            QueryDispatchResponse data = new QueryDispatchResponse(
                    identifier, "query-1", new MessageType("university.Course", "1.0.0").toString(),
                    "{\"name\":\"Axon 5\"}", Map.of()
            );
            return ("event: " + type + "\ndata: " + new ObjectMapper().writeValueAsString(data) + "\n\n")
                    .getBytes(StandardCharsets.UTF_8);
        }

        private void assertSubscriptionOutlivesTheReadTimeout(RemoteQueryDispatcher dispatcher) {
            MessageStream<QueryResponseMessage> updates = dispatcher.openSubscriptionQueryUpdateStream(
                    otherMember(), query(), 16, listeningFor(new AtomicBoolean())
            );
            try {
                // A subscription lasts as long as the subscriber wants it, and the keep-alives the member sends say
                // it is still there: neither leaves a read timeout anything to measure. So an update the member
                // emits well after that timeout still arrives.
                await().atMost(Duration.ofSeconds(5))
                       .until(() -> updates.hasNextAvailable() || updates.isCompleted());
                assertThat(updates.error()).isEmpty();
                assertThat(updates.next()).hasValueSatisfying(
                        entry -> assertThat(entry.message().identifier()).isEqualTo("update-1")
                );
            } finally {
                updates.close();
            }
        }

        private Member otherMember() {
            return new Member("node-b",
                              URI.create("http://localhost:" + otherMember.getAddress().getPort()),
                              false);
        }

        private static QueryMessage query() {
            return new GenericQueryMessage(
                    new GenericMessage("query-1", FIND_COURSE_TYPE, QUERY_PAYLOAD, Map.of()), null
            );
        }

        private static ClientHttpRequestFactory readingFor(Duration readTimeout) {
            JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory();
            requestFactory.setReadTimeout(readTimeout);
            return requestFactory;
        }

        private static SubscriptionListener listeningFor(AtomicBoolean opened) {
            return new SubscriptionListener() {
                @Override
                public void opened() {
                    opened.set(true);
                }

                @Override
                public void completed() {
                }
            };
        }

        /**
         * Answers a query with a single response, but only once a read timeout as short as
         * {@link #SHORT_READ_TIMEOUT} would have given up on it.
         */
        private void answerOnceTheReadTimeoutHasPassed(HttpExchange exchange) throws IOException {
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream body = exchange.getResponseBody()) {
                Thread.sleep(SHORT_READ_TIMEOUT.multipliedBy(3).toMillis());
                body.write(event("response", "response-1"));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * Holds a subscription open with a keep-alive well inside the read timeout, emitting one update once a read
         * timeout as short as {@link #SHORT_READ_TIMEOUT} would have given up on the subscription.
         */
        private void keepTheSubscriptionAlive(HttpExchange exchange) throws IOException {
            received.add(exchange.getRequestHeaders());
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            long updateDue = System.nanoTime() + SHORT_READ_TIMEOUT.multipliedBy(4).toNanos();
            boolean updated = false;
            try (OutputStream body = exchange.getResponseBody()) {
                while (!Thread.currentThread().isInterrupted()) {
                    if (!updated && System.nanoTime() > updateDue) {
                        body.write(event("update", "update-1"));
                        updated = true;
                    }
                    body.write(":keep-alive\n\n".getBytes(StandardCharsets.UTF_8));
                    body.flush();
                    Thread.sleep(SHORT_READ_TIMEOUT.dividedBy(3).toMillis());
                }
            } catch (IOException e) {
                // The subscriber released the subscription.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
                            .doesNotHaveBean(SpringCloudMemberDiscovery.class));
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
    static class DiscoveryClientConfiguration {

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

    }

    /**
     * A discovery implementation that registers the application, such as Eureka, supplying a {@link Registration}
     * along with its {@link DiscoveryClient}.
     */
    @Configuration(proxyBeanMethods = false)
    @Import(DiscoveryClientConfiguration.class)
    static class DiscoveryConfiguration {

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

    @Configuration(proxyBeanMethods = false)
    static class CustomQueryRestClientConfiguration {

        static final RestClient APPLICATION_QUERY_REST_CLIENT = RestClient.create();

        @Bean(SpringCloudAutoConfiguration.QUERY_REST_CLIENT_BEAN)
        RestClient axoniqSpringCloudQueryRestClient() {
            return APPLICATION_QUERY_REST_CLIENT;
        }
    }

    static class CustomDiscoveryExecutor {

        static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);
    }
}

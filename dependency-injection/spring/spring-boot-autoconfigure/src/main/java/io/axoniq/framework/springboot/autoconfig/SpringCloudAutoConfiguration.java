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

import io.axoniq.framework.springboot.SpringCloudProperties;
import io.axoniq.framework.springcloud.SpringCloudCommandBusConnector;
import io.axoniq.framework.springcloud.SpringCloudConfigurationEnhancer;
import io.axoniq.framework.springcloud.SpringCloudMemberRegistry;
import io.axoniq.framework.springcloud.discovery.CapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.discovery.IgnoreListingDiscoveryMode;
import io.axoniq.framework.springcloud.discovery.MemberCapabilitiesController;
import io.axoniq.framework.springcloud.discovery.RestCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.transport.HttpRemoteCommandDispatcher;
import io.axoniq.framework.springcloud.transport.IncomingCommandInvoker;
import io.axoniq.framework.springcloud.transport.RemoteCommandDispatcher;
import io.axoniq.framework.springcloud.transport.SpringCloudCommandController;
import org.axonframework.common.configuration.ComponentRegistry;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.conversion.MessageConverter;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnNotWebApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.http.client.ClientHttpRequestFactoryBuilder;
import org.springframework.boot.http.client.ClientHttpRequestFactorySettings;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

/**
 * Autoconfiguration for the Axoniq Framework Spring Cloud connector.
 * <p>
 * Registers the collaborators the connector needs, after which the service-loaded
 * {@link SpringCloudConfigurationEnhancer} registers the connector itself and the framework decorates the configured
 * {@code CommandBus} into a {@code DistributedCommandBus}.
 * <p>
 * These beans live here rather than in the {@code ConfigurationEnhancer} because two of them only work as Spring beans.
 * {@link SpringCloudMemberRegistry} learns about the cluster through {@code @EventListener} methods, which Spring
 * invokes only on beans it manages, and the two controllers are only mapped as endpoints if Spring MVC knows about
 * them. The rest follows those two.
 * <p>
 * Activates when a Spring Cloud {@link DiscoveryClient} and a {@link Registration} are available — that is, when the
 * application has chosen a discovery implementation of its own — and can be switched off with
 * {@code axon.springcloud.enabled=false}. As members reach each other over HTTP through Spring MVC controllers, it also
 * requires a servlet web application; see {@link NonWebApplicationGuard} and {@link ReactiveWebApplicationGuard}.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@AutoConfiguration(afterName = "io.axoniq.framework.springboot.autoconfig.AxonServerAutoConfiguration")
@ConditionalOnClass({SpringCloudCommandBusConnector.class, DiscoveryClient.class, RestClient.class})
@EnableConfigurationProperties(SpringCloudProperties.class)
public class SpringCloudAutoConfiguration {

    /**
     * The name of the {@link RestClient} bean used to send commands to other members of the cluster.
     */
    public static final String REST_CLIENT_BEAN = "axoniqSpringCloudRestClient";

    /**
     * The name of the {@link RestClient} bean used to ask other instances for their capabilities.
     */
    public static final String CAPABILITIES_REST_CLIENT_BEAN = "axoniqSpringCloudCapabilitiesRestClient";

    /**
     * The name of the {@link Executor} bean inter-member command dispatches run on.
     */
    public static final String DISPATCH_EXECUTOR_BEAN = "axoniqSpringCloudDispatchExecutor";

    /**
     * How long the connector's own clients are given to establish a connection.
     * <p>
     * Applies only to the clients this autoconfiguration builds itself; an application supplying its own
     * {@link RestClient.Builder} decides its own timeouts.
     */
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(2);

    /**
     * Bean creation method for a {@link ConfigurationEnhancer} that disables the
     * {@link SpringCloudConfigurationEnhancer} when {@code axon.springcloud.enabled} is set to {@code false}.
     * <p>
     * That enhancer is service-loaded, so switching the connector off takes disabling it explicitly rather than
     * withholding a bean. This method sits outside {@link ConnectorConfiguration} so that it is still reached when the
     * property has switched the connector's own beans off.
     *
     * @return an enhancer disabling the Spring Cloud configuration enhancer
     */
    @Bean
    @ConditionalOnProperty(name = "axon.springcloud.enabled", havingValue = "false")
    public ConfigurationEnhancer disableSpringCloudConfigurationEnhancer() {
        return new ConfigurationEnhancer() {
            @Override
            public void enhance(ComponentRegistry registry) {
                registry.disableEnhancer(SpringCloudConfigurationEnhancer.class);
            }

            @Override
            public int order() {
                return Integer.MIN_VALUE;
            }
        };
    }

    /**
     * Rejects an application that distributes commands over Spring Cloud without being able to receive any.
     * <p>
     * Members reach each other over HTTP, so a member is only reachable if it serves the connector's two endpoints.
     * Without a servlet web application context there is nothing to map them onto, while this member still registers
     * with discovery and publishes its capabilities — leaving the other members routing commands to an address that
     * refuses every connection. Failing at start-up says so, rather than leaving a share of the cluster's commands to
     * time out for as long as this member is a member.
     *
     * @author Allard Buijze
     * @since 5.4.0
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "axon.springcloud.enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnNotWebApplication
    public static class NonWebApplicationGuard {

        /**
         * Constructs a {@code NonWebApplicationGuard}, which is only ever reached when the connector is enabled in an
         * application that cannot serve its endpoints.
         *
         * @throws IllegalStateException always, as reaching this constructor is the misconfiguration it reports
         */
        public NonWebApplicationGuard() {
            throw new IllegalStateException(
                    "The Spring Cloud connector distributes commands over HTTP, but this application is not a web "
                            + "application, so other members cannot reach it. Add spring-boot-starter-web, or set "
                            + "axon.springcloud.enabled=false to handle commands locally instead."
            );
        }
    }

    /**
     * Rejects a reactive web application, which cannot serve the connector's endpoints either.
     * <p>
     * A reactive context is a web application, so {@link ConditionalOnNotWebApplication} does not catch it, yet the
     * connector's controllers are Spring MVC controllers relying on the servlet container's asynchronous handling.
     * Under WebFlux they are never mapped, leaving exactly the member that registers with discovery, advertises its
     * capabilities, and answers nothing. Reactive support means serving these endpoints as reactive handlers, which
     * this connector does not yet do; until it does, saying so at start-up beats distributing into a black hole.
     *
     * @author Allard Buijze
     * @since 5.4.0
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "axon.springcloud.enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
    public static class ReactiveWebApplicationGuard {

        /**
         * Constructs a {@code ReactiveWebApplicationGuard}, which is only ever reached when the connector is enabled in
         * a reactive web application.
         *
         * @throws IllegalStateException always, as reaching this constructor is the misconfiguration it reports
         */
        public ReactiveWebApplicationGuard() {
            throw new IllegalStateException(
                    "The Spring Cloud connector receives commands through Spring MVC controllers, which a reactive "
                            + "web application does not map, so other members cannot reach it. Use "
                            + "spring-boot-starter-web instead of spring-boot-starter-webflux, or set "
                            + "axon.springcloud.enabled=false to handle commands locally instead."
            );
        }
    }

    /**
     * The connector's own beans, registered unless {@code axon.springcloud.enabled} is set to {@code false}.
     * <p>
     * Nested so that the property switches these beans off while leaving
     * {@link #disableSpringCloudConfigurationEnhancer()} reachable — a class-level condition would switch that off too,
     * and the service-loaded enhancer would go on registering a connector regardless.
     *
     * @author Allard Buijze
     * @since 5.4.0
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "axon.springcloud.enabled", havingValue = "true", matchIfMissing = true)
    public static class ConnectorConfiguration {

        /**
         * Bean creation method for the {@link RestClient} used to send commands to other members.
         * <p>
         * Built from the application's own {@link RestClient.Builder} when it has one, so that whatever it configured
         * there — a load balancer, authentication, timeouts — applies to inter-member traffic as well. When the
         * application supplies no builder, the client is given timeouts of its own: without them a member that accepts
         * a connection and then answers nothing holds the sending thread for as long as the operating system allows,
         * outliving the dispatcher's own reply deadline, which bounds the returned future but not the exchange behind
         * it.
         *
         * @param builderProvider provides the application's {@link RestClient.Builder}, if it has one
         * @param properties      the connector's properties
         * @return the client used to send commands to other members of the cluster
         */
        @Bean(REST_CLIENT_BEAN)
        @ConditionalOnMissingBean(name = REST_CLIENT_BEAN)
        public RestClient axoniqSpringCloudRestClient(ObjectProvider<RestClient.Builder> builderProvider,
                                                      SpringCloudProperties properties) {
            RestClient.Builder applicationBuilder = builderProvider.getIfAvailable();
            return Objects.requireNonNullElseGet(
                                  applicationBuilder,
                                  () -> RestClient.builder()
                                                  .requestFactory(requestFactory(
                                                          CONNECT_TIMEOUT,
                                                          properties.getCommandReplyTimeout())
                                                  )
                          )
                          .build();
        }

        /**
         * Bean creation method for the {@link RestClient} used to ask other instances for their capabilities.
         * <p>
         * Separate from the client commands are sent with, because the two need opposite deadlines. A command may
         * legitimately take as long as the handler needs, while a capabilities request happens on every discovery
         * heartbeat and must not outlast it — one unresponsive instance would otherwise hold up the round that rebuilds
         * the routing ring for every member.
         *
         * @param properties the connector's properties
         * @return the client used to ask other instances for their capabilities
         */
        @Bean(CAPABILITIES_REST_CLIENT_BEAN)
        @ConditionalOnMissingBean(name = CAPABILITIES_REST_CLIENT_BEAN)
        public RestClient axoniqSpringCloudCapabilitiesRestClient(SpringCloudProperties properties) {
            Duration timeout = properties.getCapabilitiesTimeout();
            return RestClient.builder()
                             .requestFactory(requestFactory(timeout, timeout))
                             .build();
        }

        private static ClientHttpRequestFactory requestFactory(Duration connectTimeout, Duration readTimeout) {
            ClientHttpRequestFactorySettings settings =
                    ClientHttpRequestFactorySettings.defaults()
                                                    .withConnectTimeout(connectTimeout)
                                                    .withReadTimeout(readTimeout);
            return ClientHttpRequestFactoryBuilder.detect().build(settings);
        }

        /**
         * Bean creation method for the {@link CapabilityDiscoveryMode} used to learn what each discovered instance
         * handles.
         * <p>
         * Combines the REST mode — the only way capabilities can travel, since service instance metadata is fixed at
         * registration time — with the ignore list that keeps unrelated services from being asked on every heartbeat.
         *
         * @param restClient the client used to ask other instances for their capabilities
         * @param properties the connector's properties
         * @return the mode used to discover the capabilities of other members
         */
        @Bean
        @ConditionalOnMissingBean
        public CapabilityDiscoveryMode axoniqSpringCloudCapabilityDiscoveryMode(
                @Qualifier(CAPABILITIES_REST_CLIENT_BEAN) RestClient restClient,
                SpringCloudProperties properties
        ) {
            return new IgnoreListingDiscoveryMode(
                    new RestCapabilityDiscoveryMode(restClient, properties.getCapabilitiesEndpoint()),
                    properties.getIgnorePeriod()
            );
        }

        /**
         * Bean creation method for the {@link SpringCloudMemberRegistry} maintaining the routing ring.
         * <p>
         * An application can narrow which discovered instances are considered at all by contributing a
         * {@code Predicate<ServiceInstance>} bean. That is worth doing on a registry holding many services: instances
         * rejected by the predicate are never asked for their capabilities, which is cheaper than letting the ignore
         * list learn about them one heartbeat at a time. Every instance is considered when no such bean is present.
         *
         * @param discoveryClientProvider provides the client reporting the service instances making up the cluster
         * @param registrationProvider    provides the registration representing this application
         * @param discoveryMode           the mode used to discover the capabilities of other members
         * @param instanceFilterProvider  provides the application's filter deciding which instances are considered
         * @param properties              the connector's properties
         * @return the registry maintaining the routing ring
         */
        @Bean
        @ConditionalOnMissingBean
        public SpringCloudMemberRegistry axoniqSpringCloudMemberRegistry(
                ObjectProvider<DiscoveryClient> discoveryClientProvider,
                ObjectProvider<Registration> registrationProvider,
                CapabilityDiscoveryMode discoveryMode,
                ObjectProvider<Predicate<ServiceInstance>> instanceFilterProvider,
                SpringCloudProperties properties
        ) {
            return new SpringCloudMemberRegistry(required(discoveryClientProvider, DiscoveryClient.class),
                                                 required(registrationProvider, Registration.class),
                                                 discoveryMode,
                                                 instanceFilterProvider.getIfAvailable(() -> instance -> true),
                                                 properties.getContextRootMetadataPropertyName());
        }

        /**
         * Bean creation method for the {@link Executor} the blocking HTTP round trips to other members run on.
         * <p>
         * A virtual-thread-per-task executor, because the work on it is a blocking HTTP call and nothing else. Sizing a
         * platform thread pool for that would cap the commands this member can have in flight for no reason other than
         * the pool's own size.
         * <p>
         * Shut down with {@code shutdownNow} rather than the destroy method Spring would infer. That would be
         * {@link ExecutorService#close()}, which waits for every task to finish, and the connector's own shutdown has
         * already waited for the commands still in flight.
         *
         * @return the executor inter-member command dispatches run on
         */
        @Bean(destroyMethod = "shutdownNow", name = DISPATCH_EXECUTOR_BEAN)
        @ConditionalOnMissingBean(name = DISPATCH_EXECUTOR_BEAN)
        public ExecutorService axoniqSpringCloudDispatchExecutor() {
            return Executors.newVirtualThreadPerTaskExecutor();
        }

        /**
         * Bean creation method for the {@link RemoteCommandDispatcher} sending commands to other members.
         *
         * @param restClient        the client used to reach other members
         * @param executor          the executor the blocking HTTP round trips run on
         * @param properties        the connector's properties
         * @param converterProvider provides the {@link MessageConverter}, if one is available
         * @return the dispatcher sending commands to other members
         */
        @Bean
        @ConditionalOnMissingBean
        public RemoteCommandDispatcher axoniqSpringCloudRemoteCommandDispatcher(
                @Qualifier(REST_CLIENT_BEAN) RestClient restClient,
                @Qualifier(DISPATCH_EXECUTOR_BEAN) Executor executor,
                SpringCloudProperties properties,
                ObjectProvider<MessageConverter> converterProvider
        ) {
            return new HttpRemoteCommandDispatcher(restClient,
                                                   properties.getCommandEndpoint(),
                                                   executor,
                                                   converterProvider.getIfAvailable(),
                                                   properties.getCommandReplyTimeout());
        }

        /**
         * Bean creation method for the {@link IncomingCommandInvoker} handling commands sent by other members.
         *
         * @param registry          the registry naming this member in the failures the invoker reports
         * @param converterProvider provides the {@link MessageConverter}, if one is available
         * @return the invoker handling commands sent by other members
         */
        @Bean
        @ConditionalOnMissingBean
        public IncomingCommandInvoker axoniqSpringCloudIncomingCommandInvoker(
                SpringCloudMemberRegistry registry,
                ObjectProvider<MessageConverter> converterProvider
        ) {
            return new IncomingCommandInvoker(() -> registry.localMember().name(),
                                              converterProvider.getIfAvailable());
        }

        /**
         * Bean creation method for the controller receiving commands from other members.
         *
         * @param invoker the invoker handling commands sent by other members
         * @return the controller receiving commands from other members
         */
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
        public SpringCloudCommandController axoniqSpringCloudCommandController(IncomingCommandInvoker invoker) {
            return new SpringCloudCommandController(invoker);
        }

        /**
         * Bean creation method for the controller serving this application's capabilities to other members.
         *
         * @param discoveryMode the mode holding this application's own capabilities
         * @return the controller serving this application's capabilities
         */
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
        public MemberCapabilitiesController axoniqSpringCloudMemberCapabilitiesController(
                CapabilityDiscoveryMode discoveryMode
        ) {
            return new MemberCapabilitiesController(discoveryMode);
        }

        /**
         * Returns the bean the given {@code provider} supplies, explaining what is missing when it supplies none.
         * <p>
         * A {@code DiscoveryClient} and a {@code Registration} come from the discovery implementation the application
         * chose, and this connector cannot distribute anything without both. They are resolved here, as the bean is
         * built, rather than through {@code @ConditionalOnBean}: a bean-presence condition is evaluated in
         * autoconfiguration order, so a discovery implementation whose autoconfiguration happens to run after this one
         * would leave the condition unmet and this connector quietly absent. Resolving at construction is independent
         * of that order, and an application missing a discovery implementation is told so rather than left with
         * commands that are never distributed.
         *
         * @param provider the provider of the required bean
         * @param type     the type of the required bean, named in the failure when it is absent
         * @param <B>      the type of the required bean
         * @return the bean the given {@code provider} supplies
         * @throws IllegalStateException when the given {@code provider} supplies no bean
         */
        private static <B> B required(ObjectProvider<B> provider, Class<B> type) {
            B bean = provider.getIfAvailable();
            if (bean == null) {
                throw new IllegalStateException(
                        ("No %s bean is available, so the Spring Cloud connector cannot distribute messages. Add a "
                                + "Spring Cloud Discovery implementation, such as Eureka, Consul or Kubernetes, or "
                                + "set axon.springcloud.enabled=false to handle messages locally instead.")
                                .formatted(type.getSimpleName())
                );
            }
            return bean;
        }
    }
}

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

import io.axoniq.framework.springboot.springcloud.SpringCloudProperties;
import io.axoniq.framework.springcloud.SpringCloudCommandBusConnector;
import io.axoniq.framework.springcloud.SpringCloudConfigurationEnhancer;
import io.axoniq.framework.springcloud.SpringCloudMemberRegistry;
import io.axoniq.framework.springcloud.discovery.CapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.discovery.IgnoreListingDiscoveryMode;
import io.axoniq.framework.springcloud.discovery.MemberCapabilitiesController;
import io.axoniq.framework.springcloud.discovery.RestCapabilityDiscoveryMode;
import io.axoniq.framework.springcloud.transport.HttpRemoteCommandDispatcher;
import io.axoniq.framework.springcloud.transport.HttpRemoteQueryDispatcher;
import io.axoniq.framework.springcloud.transport.IncomingCommandGateway;
import io.axoniq.framework.springcloud.transport.IncomingQueryGateway;
import io.axoniq.framework.springcloud.transport.RemoteCommandDispatcher;
import io.axoniq.framework.springcloud.transport.RemoteQueryDispatcher;
import io.axoniq.framework.springcloud.transport.SpringCloudCommandController;
import io.axoniq.framework.springcloud.transport.SpringCloudQueryController;
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
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication.Type;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Autoconfiguration for the Axoniq Framework Spring Cloud connector.
 * <p>
 * Registers the collaborators the connector needs, after which the service-loaded
 * {@link SpringCloudConfigurationEnhancer} registers the connectors themselves and the framework decorates the
 * configured {@code CommandBus} and {@code QueryBus} into their distributed counterparts.
 * <p>
 * These beans live here rather than in the {@code ConfigurationEnhancer} because two of them only work as Spring
 * beans. {@link SpringCloudMemberRegistry} learns about the cluster through {@code @EventListener} methods, which
 * Spring invokes only on beans it manages, and the two controllers are only mapped as endpoints if Spring MVC knows
 * about them. The rest follows those two.
 * <p>
 * Activates when a Spring Cloud {@link DiscoveryClient} and a {@link Registration} are available — that is, when the
 * application has chosen a discovery implementation of its own — and can be switched off with
 * {@code axon.springcloud.enabled=false}. As members reach each other over HTTP, it also requires a web application;
 * see {@link NonWebApplicationGuard}.
 *
 * @author Allard Buijze
 * @since 5.4.0
 */
@AutoConfiguration(afterName = "io.axoniq.framework.springboot.autoconfig.AxonServerAutoConfiguration")
@ConditionalOnClass({SpringCloudCommandBusConnector.class, DiscoveryClient.class, RestClient.class})
@EnableConfigurationProperties(SpringCloudProperties.class)
public class SpringCloudAutoConfiguration {

    /**
     * The name of the {@link RestClient} bean used to reach other members of the cluster.
     */
    public static final String REST_CLIENT_BEAN = "axoniqSpringCloudRestClient";

    /**
     * The name of the {@link Executor} bean inter-member command and query dispatches run on.
     */
    public static final String DISPATCH_EXECUTOR_BEAN = "axoniqSpringCloudDispatchExecutor";

    /**
     * The name of the {@link ScheduledExecutorService} bean deciding when the deadline of a dispatched query has
     * passed, when a subscription has been silent for too long, and when a subscription being answered is due a
     * keep-alive.
     */
    public static final String QUERY_SCHEDULER_BEAN = "axoniqSpringCloudQueryScheduler";

    /**
     * The name of the {@link Executor} bean the keep-alive writes of subscriptions being answered run on.
     */
    public static final String KEEP_ALIVE_EXECUTOR_BEAN = "axoniqSpringCloudKeepAliveExecutor";

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
     * Rejects an application that distributes messages over Spring Cloud without being able to receive any.
     * <p>
     * Members reach each other over HTTP, so a member is only reachable if it serves the connector's endpoints. With
     * no servlet web application there is nothing to map them onto, while this member still registers with discovery
     * and publishes its capabilities — leaving the other members routing their share of the messages to an address
     * that cannot answer. Failing at start-up says so, rather than leaving those messages to time out for as long as
     * this member is a member.
     * <p>
     * Two conditions are needed to cover that, because "not a servlet web application" is both the application with
     * no web stack at all, guarded here, and the one built on WebFlux, guarded by
     * {@link ReactiveWebApplicationGuard}.
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
            throw unreachableMember("this application is not a web application");
        }
    }

    /**
     * Rejects an application that distributes messages over Spring Cloud from a WebFlux stack.
     * <p>
     * The endpoints members reach each other on are Spring MVC endpoints, and a query's responses are streamed with
     * an {@code SseEmitter}, which a reactive stack does not map. Such an application would register with discovery
     * and advertise what it handles while answering nothing, so it is rejected for the same reason an application
     * with no web stack is; see {@link NonWebApplicationGuard}.
     *
     * @author Allard Buijze
     * @since 5.4.0
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "axon.springcloud.enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnWebApplication(type = Type.REACTIVE)
    public static class ReactiveWebApplicationGuard {

        /**
         * Constructs a {@code ReactiveWebApplicationGuard}, which is only ever reached when the connector is enabled
         * in an application whose web stack cannot serve its endpoints.
         *
         * @throws IllegalStateException always, as reaching this constructor is the misconfiguration it reports
         */
        public ReactiveWebApplicationGuard() {
            throw unreachableMember("this application is built on WebFlux, whose stack does not serve them");
        }
    }

    /**
     * Returns the failure reported when this application would join the cluster unable to answer anything.
     *
     * @param reason what about this application leaves it unable to serve the connector's endpoints
     * @return the failure to report
     */
    private static IllegalStateException unreachableMember(String reason) {
        return new IllegalStateException(
                ("The Spring Cloud connector distributes messages over HTTP endpoints, but %s, so other members "
                        + "cannot reach it. Add spring-boot-starter-web, or set axon.springcloud.enabled=false to "
                        + "handle messages locally instead.").formatted(reason)
        );
    }

    /**
     * The connector's own beans, registered unless {@code axon.springcloud.enabled} is set to {@code false}.
     * <p>
     * Nested so that the property switches these beans off while leaving
     * {@link #disableSpringCloudConfigurationEnhancer()} reachable — a class-level condition would switch that off
     * too, and the service-loaded enhancer would go on registering a connector regardless.
     *
     * @author Allard Buijze
     * @since 5.4.0
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "axon.springcloud.enabled", havingValue = "true", matchIfMissing = true)
    public static class ConnectorConfiguration {

        /**
         * Bean creation method for the {@link RestClient} used to reach other members, both to ask for their
         * capabilities and to send them commands.
         * <p>
         * Built from the application's own {@link RestClient.Builder} when it has one, so that whatever it configured
         * there — a load balancer, authentication, timeouts — applies to inter-member traffic as well.
         *
         * @param builderProvider provides the application's {@link RestClient.Builder}, if it has one
         * @return the client used to reach other members of the cluster
         */
        @Bean(REST_CLIENT_BEAN)
        @ConditionalOnMissingBean(name = REST_CLIENT_BEAN)
        public RestClient axoniqSpringCloudRestClient(ObjectProvider<RestClient.Builder> builderProvider) {
            return builderProvider.getIfAvailable(RestClient::builder).build();
        }

        /**
         * Bean creation method for the {@link CapabilityDiscoveryMode} used to learn what each discovered instance
         * handles.
         * <p>
         * Combines the REST mode — the only way capabilities can travel, since service instance metadata is fixed at
         * registration time — with the ignore list that keeps unrelated services from being asked on every heartbeat.
         *
         * @param restClient the client used to reach other members
         * @param properties the connector's properties
         * @return the mode used to discover the capabilities of other members
         */
        @Bean
        @ConditionalOnMissingBean
        public CapabilityDiscoveryMode axoniqSpringCloudCapabilityDiscoveryMode(
                @Qualifier(REST_CLIENT_BEAN) RestClient restClient,
                SpringCloudProperties properties
        ) {
            return new IgnoreListingDiscoveryMode(
                    new RestCapabilityDiscoveryMode(restClient, properties.getCapabilitiesEndpoint()),
                    properties.getIgnoreListingExpireThreshold()
            );
        }

        /**
         * Bean creation method for the {@link SpringCloudMemberRegistry} maintaining the routing ring.
         *
         * @param discoveryClientProvider provides the client reporting the service instances making up the cluster
         * @param registrationProvider    provides the registration representing this application
         * @param discoveryMode           the mode used to discover the capabilities of other members
         * @param properties              the connector's properties
         * @return the registry maintaining the routing ring
         */
        @Bean
        @ConditionalOnMissingBean
        public SpringCloudMemberRegistry axoniqSpringCloudMemberRegistry(
                ObjectProvider<DiscoveryClient> discoveryClientProvider,
                ObjectProvider<Registration> registrationProvider,
                CapabilityDiscoveryMode discoveryMode,
                SpringCloudProperties properties
        ) {
            return new SpringCloudMemberRegistry(required(discoveryClientProvider, DiscoveryClient.class),
                                                 required(registrationProvider, Registration.class),
                                                 discoveryMode,
                                                 instance -> true,
                                                 properties.getContextRootMetadataPropertyName());
        }

        /**
         * Bean creation method for the {@link Executor} the blocking HTTP exchanges with other members run on.
         * <p>
         * A virtual-thread-per-task executor, because the work on it is a blocking HTTP call and nothing else. Sizing
         * a platform thread pool for that would cap the messages this member can have in flight for no reason other
         * than the pool's own size.
         * <p>
         * Shut down with {@code shutdownNow} rather than the destroy method Spring would infer. That would be
         * {@link ExecutorService#close()}, which waits for every task to finish — and a task here is the reading of a
         * query's response stream, which lasts as long as the answering member keeps it open. Shutting down the
         * application would then wait on members it is no longer talking to. The connector's own shutdown already
         * waits for queries still in flight, in the {@code OUTBOUND_QUERY_CONNECTORS} phase, so by the time this runs
         * the threads left are the ones nothing is waiting for.
         *
         * @return the executor inter-member dispatches run on
         */
        @Bean(destroyMethod = "shutdownNow", name = DISPATCH_EXECUTOR_BEAN)
        @ConditionalOnMissingBean(name = DISPATCH_EXECUTOR_BEAN)
        public ExecutorService axoniqSpringCloudDispatchExecutor() {
            return Executors.newVirtualThreadPerTaskExecutor();
        }

        /**
         * Bean creation method for the {@link ScheduledExecutorService} deciding when a query's deadline has passed,
         * when a subscription has been silent for too long, and when a subscription being answered is due a
         * keep-alive.
         * <p>
         * A cancelled deadline is removed from its queue rather than left to elapse, so a query answered promptly
         * does not keep its responses reachable until the deadline would have fired.
         * <p>
         * A single thread carries all of it, which it can because nothing scheduled here blocks: the work is
         * deciding that something is due, and the one due thing that blocks -- writing a keep-alive to a subscriber
         * -- is handed to {@link #axoniqSpringCloudKeepAliveExecutor()} instead.
         *
         * @return the scheduler query timing runs on
         */
        @Bean(destroyMethod = "shutdownNow", name = QUERY_SCHEDULER_BEAN)
        @ConditionalOnMissingBean(name = QUERY_SCHEDULER_BEAN)
        public ScheduledExecutorService axoniqSpringCloudQueryScheduler() {
            ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, task -> {
                Thread thread = new Thread(task, "axoniq-springcloud-query-scheduler");
                thread.setDaemon(true);
                return thread;
            });
            scheduler.setRemoveOnCancelPolicy(true);
            return scheduler;
        }

        /**
         * Bean creation method for the {@link RemoteCommandDispatcher} sending commands to other members.
         *
         * @param restClient        the client used to reach other members
         * @param executor          the executor the blocking HTTP exchanges run on
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
         * Bean creation method for the {@link IncomingCommandGateway} handling commands sent by other members.
         *
         * @param registry          the registry naming this member in the failures the gateway reports
         * @param converterProvider provides the {@link MessageConverter}, if one is available
         * @return the gateway handling commands sent by other members
         */
        @Bean
        @ConditionalOnMissingBean
        public IncomingCommandGateway axoniqSpringCloudIncomingCommandGateway(
                SpringCloudMemberRegistry registry,
                ObjectProvider<MessageConverter> converterProvider
        ) {
            return new IncomingCommandGateway(() -> registry.localMember().name(),
                                              converterProvider.getIfAvailable());
        }

        /**
         * Bean creation method for the controller receiving commands from other members.
         *
         * @param gateway the gateway handling commands sent by other members
         * @return the controller receiving commands from other members
         */
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnWebApplication(type = Type.SERVLET)
        public SpringCloudCommandController axoniqSpringCloudCommandController(IncomingCommandGateway gateway) {
            return new SpringCloudCommandController(gateway);
        }

        /**
         * Bean creation method for the {@link IncomingQueryGateway} answering queries sent by other members.
         *
         * @param registry          the registry naming this member in the failures the gateway reports
         * @param converterProvider provides the {@link MessageConverter}, if one is available
         * @return the gateway answering queries sent by other members
         */
        @Bean
        @ConditionalOnMissingBean
        public IncomingQueryGateway axoniqSpringCloudIncomingQueryGateway(
                SpringCloudMemberRegistry registry,
                ObjectProvider<MessageConverter> converterProvider
        ) {
            return new IncomingQueryGateway(() -> registry.localMember().name(),
                                            converterProvider.getIfAvailable());
        }

        /**
         * Bean creation method for the {@link RemoteQueryDispatcher} sending queries to other members.
         *
         * @param restClient the client sending the queries
         * @param executor   the executor each query's response stream is read on
         * @param scheduler  the scheduler each query's deadline runs on
         * @param converter  the converter the events of a query's response stream are read with
         * @param properties the connector's properties
         * @return the dispatcher sending queries to other members
         */
        @Bean
        @ConditionalOnMissingBean
        public RemoteQueryDispatcher axoniqSpringCloudRemoteQueryDispatcher(
                @Qualifier(REST_CLIENT_BEAN) RestClient restClient,
                @Qualifier(DISPATCH_EXECUTOR_BEAN) Executor executor,
                @Qualifier(QUERY_SCHEDULER_BEAN) ScheduledExecutorService scheduler,
                MessageConverter converter,
                SpringCloudProperties properties
        ) {
            return new HttpRemoteQueryDispatcher(restClient,
                                                 properties.getQueryEndpoint(),
                                                 executor,
                                                 converter,
                                                 properties.getQueryBufferSize(),
                                                 properties.getQueryResponseTimeout(),
                                                 properties.getSubscriptionInactivityTimeout(),
                                                 scheduler);
        }

        /**
         * Bean creation method for the controller answering queries from other members.
         *
         * @param gateway           the gateway answering queries sent by other members
         * @param scheduler         the scheduler deciding when each open subscription is due a keep-alive
         * @param keepAliveExecutor the executor those keep-alives are written on
         * @param properties        the connector's properties
         * @return the controller answering queries from other members
         */
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnWebApplication(type = Type.SERVLET)
        public SpringCloudQueryController axoniqSpringCloudQueryController(
                IncomingQueryGateway gateway,
                @Qualifier(QUERY_SCHEDULER_BEAN) ScheduledExecutorService scheduler,
                @Qualifier(KEEP_ALIVE_EXECUTOR_BEAN) Executor keepAliveExecutor,
                SpringCloudProperties properties
        ) {
            return new SpringCloudQueryController(gateway,
                                                  properties.getQueryTimeout(),
                                                  properties.getSubscriptionKeepAliveInterval(),
                                                  scheduler,
                                                  keepAliveExecutor);
        }

        /**
         * Bean creation method for the {@link Executor} the keep-alive of each subscription being answered is written
         * on.
         * <p>
         * Separate from the scheduler that decides when a keep-alive is due, because writing one is a blocking write
         * to a single subscriber and deciding it is due is not. A subscriber that has stopped reading without closing
         * blocks its write until the connection gives way; were that the scheduler's thread, it would hold up every
         * other subscription's keep-alive and every dispatched query's deadline with it.
         * <p>
         * A virtual-thread-per-task executor, for the same reason the dispatch executor is one: the work is a
         * blocking write and nothing else, and sizing a platform thread pool for it would cap the subscriptions this
         * member can answer at the pool's own size.
         *
         * @return the executor subscription keep-alives are written on
         */
        @Bean(destroyMethod = "shutdownNow", name = KEEP_ALIVE_EXECUTOR_BEAN)
        @ConditionalOnMissingBean(name = KEEP_ALIVE_EXECUTOR_BEAN)
        @ConditionalOnWebApplication(type = Type.SERVLET)
        public ExecutorService axoniqSpringCloudKeepAliveExecutor() {
            return Executors.newVirtualThreadPerTaskExecutor();
        }

        /**
         * Bean creation method for the controller serving this application's capabilities to other members.
         *
         * @param discoveryMode the mode holding this application's own capabilities
         * @return the controller serving this application's capabilities
         */
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnWebApplication(type = Type.SERVLET)
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
         * messages that are never distributed.
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

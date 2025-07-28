/*
 * Copyright (c) 2022-2025. AxonIQ B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.console.framework;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.console.framework.api.AxoniqConsoleDlqMode;
import io.axoniq.console.framework.api.DomainEventAccessMode;
import io.axoniq.console.framework.application.DomainEventStreamProvider;
import io.axoniq.console.framework.application.ApplicationMetricRegistry;
import io.axoniq.console.framework.application.ApplicationMetricReporter;
import io.axoniq.console.framework.application.ApplicationReportCreator;
import io.axoniq.console.framework.application.ApplicationThreadDumpProvider;
import io.axoniq.console.framework.application.RSocketDomainEntityDataResponder;
import io.axoniq.console.framework.application.RSocketThreadDumpResponder;
import io.axoniq.console.framework.client.AxoniqConsoleRSocketClient;
import io.axoniq.console.framework.client.ClientSettingsService;
import io.axoniq.console.framework.client.RSocketHandlerRegistrar;
import io.axoniq.console.framework.client.ServerProcessorReporter;
import io.axoniq.console.framework.client.SetupPayloadCreator;
import io.axoniq.console.framework.client.strategy.CborEncodingStrategy;
import io.axoniq.console.framework.client.strategy.RSocketPayloadEncodingStrategy;
import io.axoniq.console.framework.eventprocessor.DeadLetterManager;
import io.axoniq.console.framework.eventprocessor.EventProcessorManager;
import io.axoniq.console.framework.eventprocessor.ProcessorReportCreator;
import io.axoniq.console.framework.eventprocessor.RSocketDlqResponder;
import io.axoniq.console.framework.eventprocessor.RSocketProcessorResponder;
import io.axoniq.console.framework.eventprocessor.metrics.AxoniqConsoleProcessorInterceptor;
import io.axoniq.console.framework.eventprocessor.metrics.ProcessorMetricsRegistry;
import io.axoniq.console.framework.messaging.AxoniqConsoleDispatchInterceptor;
import io.axoniq.console.framework.messaging.AxoniqConsoleSpanFactory;
import io.axoniq.console.framework.messaging.AxoniqConsoleWrappedEventScheduler;
import io.axoniq.console.framework.messaging.HandlerMetricsRegistry;
import io.axoniq.console.framework.messaging.SpanMatcher;
import io.axoniq.console.framework.messaging.SpanMatcherPredicateMap;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.axonframework.common.BuilderUtils;
import org.axonframework.common.transaction.NoTransactionManager;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.config.Configuration;
import org.axonframework.config.Configurer;
import org.axonframework.config.ConfigurerModule;
import org.axonframework.eventhandling.scheduling.EventScheduler;
import org.axonframework.tracing.SpanFactory;
import org.jetbrains.annotations.NotNull;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static io.axoniq.console.framework.messaging.SpanMatcher.getSpanMatcherPredicateMap;

/**
 * Applies the configuration necessary for AxonIQ Console to the {@link Configurer} of Axon Framework. The module will
 * automatically start when Axon Framework does.
 */
public class AxoniqConsoleConfigurerModule implements ConfigurerModule {

    private final String environmentId;
    private final String accessToken;
    private final String applicationName;
    private final String host;
    private final Integer port;
    private final Boolean secure;
    private final Long initialDelay;
    private final AxoniqConsoleDlqMode dlqMode;
    private final List<String> dlqDiagnosticsWhitelist;
    private final DomainEventAccessMode domainEventAccessMode;
    private final ScheduledExecutorService reportingTaskExecutor;
    private final ExecutorService managementTaskExecutor;
    private final boolean configureSpanFactory;
    private final SpanMatcherPredicateMap spanMatcherPredicateMap;
    private final EventScheduler eventScheduler;
    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final String instanceName;
    private final ObjectMapper objectMapper;

    /**
     * Creates the {@link AxoniqConsoleConfigurerModule} with the given {@code builder}.
     *
     * @param builder The configured builder for the {@link AxoniqConsoleConfigurerModule}.
     */
    protected AxoniqConsoleConfigurerModule(Builder builder) {
        this.environmentId = builder.environmentId;
        this.accessToken = builder.accessToken;
        this.applicationName = builder.applicationName.replaceAll("([\\[\\]])", "-");
        this.instanceName = builder.hostname + "-" + builder.nodeId;
        this.host = builder.host;
        this.port = builder.port;
        this.secure = builder.secure;
        this.initialDelay = builder.initialDelay;
        this.dlqMode = builder.dlqMode;
        this.dlqDiagnosticsWhitelist = builder.dlqDiagnosticsWhitelist;
        this.domainEventAccessMode = builder.domainEventAccessMode;
        this.reportingTaskExecutor = builder.reportingTaskExecutor;
        this.managementTaskExecutor = builder.managementTaskExecutor;
        this.configureSpanFactory = !builder.disableSpanFactoryInConfiguration;
        this.spanMatcherPredicateMap = builder.spanMatcherPredicateMap;
        this.eventScheduler = builder.eventScheduler;
        this.objectMapper = builder.objectMapper;
    }

    /**
     * Creates the base builder with the required parameters. Defaults to the public production environment of AxonIQ
     * console.
     *
     * @param environmentId   The environment identifier of AxonIQ Console to connect.
     * @param accessToken     The access token needed to authenticate to the environment.
     * @param applicationName The display name of the application. Some special characters may be replaced with a
     *                        hyphen.
     * @return The builder with which you can further configure this module
     */
    public static Builder builder(String environmentId, String accessToken, String applicationName) {
        return new Builder(environmentId, accessToken, applicationName);
    }

    @Override
    public void configureModule(@NotNull Configurer configurer) {
        configurer
                .registerComponent(ClientSettingsService.class,
                                   c -> new ClientSettingsService()
                )
                .registerComponent(ProcessorMetricsRegistry.class,
                                   c -> new ProcessorMetricsRegistry()
                )
                .registerComponent(ApplicationMetricRegistry.class,
                                   c -> new ApplicationMetricRegistry(meterRegistry)
                )
                .registerComponent(ProcessorReportCreator.class,
                                   c -> new ProcessorReportCreator(
                                           c.eventProcessingConfiguration(),
                                           c.getComponent(ProcessorMetricsRegistry.class)
                                   )
                )
                .registerComponent(ApplicationReportCreator.class,
                                   c -> new ApplicationReportCreator(
                                           c.getComponent(ApplicationMetricRegistry.class)
                                   )
                )
                .registerComponent(SetupPayloadCreator.class,
                                   c -> new SetupPayloadCreator(
                                           c,
                                           dlqMode,
                                           domainEventAccessMode
                                   )
                )
                .registerComponent(EventProcessorManager.class,
                                   c -> new EventProcessorManager(
                                           c.eventProcessingConfiguration(),
                                           c.getComponent(TransactionManager.class, NoTransactionManager::instance)
                                   )
                )
                .registerComponent(RSocketPayloadEncodingStrategy.class,
                                   c -> new CborEncodingStrategy()
                )
                .registerComponent(RSocketHandlerRegistrar.class,
                                   c -> new RSocketHandlerRegistrar(c.getComponent(RSocketPayloadEncodingStrategy.class))
                )
                .registerComponent(RSocketProcessorResponder.class,
                                   c -> new RSocketProcessorResponder(
                                           c.getComponent(EventProcessorManager.class),
                                           c.getComponent(ProcessorReportCreator.class),
                                           c.getComponent(RSocketHandlerRegistrar.class)
                                   )
                )
                .registerComponent(AxoniqConsoleRSocketClient.class,
                                   c -> new AxoniqConsoleRSocketClient(
                                           environmentId,
                                           accessToken,
                                           applicationName,
                                           host,
                                           port,
                                           secure,
                                           initialDelay,
                                           c.getComponent(SetupPayloadCreator.class),
                                           c.getComponent(RSocketHandlerRegistrar.class),
                                           c.getComponent(RSocketPayloadEncodingStrategy.class),
                                           c.getComponent(ClientSettingsService.class),
                                           reportingTaskExecutor,
                                           instanceName
                                   )
                )
                .registerComponent(ServerProcessorReporter.class,
                                   c -> new ServerProcessorReporter(
                                           c.getComponent(AxoniqConsoleRSocketClient.class),
                                           c.getComponent(ProcessorReportCreator.class),
                                           c.getComponent(ClientSettingsService.class),
                                           reportingTaskExecutor)
                )
                .registerComponent(ApplicationMetricReporter.class,
                                   c -> new ApplicationMetricReporter(
                                           c.getComponent(AxoniqConsoleRSocketClient.class),
                                           c.getComponent(ApplicationReportCreator.class),
                                           c.getComponent(ClientSettingsService.class),
                                           reportingTaskExecutor)
                )
                .registerComponent(HandlerMetricsRegistry.class,
                                   c -> new HandlerMetricsRegistry(
                                           c.getComponent(AxoniqConsoleRSocketClient.class),
                                           c.getComponent(ClientSettingsService.class),
                                           reportingTaskExecutor,
                                           applicationName
                                   )
                )
                .registerComponent(DeadLetterManager.class,
                                   c -> new DeadLetterManager(
                                           c.eventProcessingConfiguration(),
                                           c.eventSerializer(),
                                           dlqMode,
                                           dlqDiagnosticsWhitelist,
                                           managementTaskExecutor
                                   ))
                .registerComponent(ApplicationThreadDumpProvider.class,
                                   c -> new ApplicationThreadDumpProvider()
                )
                .registerComponent(DomainEventStreamProvider.class,
                                   c -> new DomainEventStreamProvider(
                                           configurer.buildConfiguration(),
                                           objectMapper
                                   )
                )
                .registerComponent(RSocketDlqResponder.class,
                                   c -> new RSocketDlqResponder(
                                           c.getComponent(DeadLetterManager.class),
                                           c.getComponent(RSocketHandlerRegistrar.class)
                                   ))
                .registerComponent(RSocketThreadDumpResponder.class,
                                   c -> new RSocketThreadDumpResponder(
                                           c.getComponent(ApplicationThreadDumpProvider.class),
                                           c.getComponent(RSocketHandlerRegistrar.class)
                                   ))
                .registerComponent(RSocketDomainEntityDataResponder.class,
                                   c -> new RSocketDomainEntityDataResponder(
                                           c.getComponent(DomainEventStreamProvider.class),
                                           c.getComponent(RSocketHandlerRegistrar.class),
                                           domainEventAccessMode,
                                           c.eventSerializer()
                                   ))
                .eventProcessing()
                .registerDefaultHandlerInterceptor((
                                                           c, name) -> new AxoniqConsoleProcessorInterceptor(
                        c.getComponent(ProcessorMetricsRegistry.class),
                        name
                ));

        if (configureSpanFactory) {
            configurer.registerComponent(SpanFactory.class, c -> new AxoniqConsoleSpanFactory(spanMatcherPredicateMap));
        }

        if (Objects.nonNull(eventScheduler)) {
            configurer.registerComponent(
                    EventScheduler.class,
                    c -> new AxoniqConsoleWrappedEventScheduler(
                            eventScheduler,
                            applicationName));
        }

        configurer.onInitialize(c -> {
            c.getComponent(ServerProcessorReporter.class);
            c.getComponent(ApplicationMetricReporter.class);
            c.getComponent(RSocketProcessorResponder.class);
            c.getComponent(RSocketDlqResponder.class);
            c.getComponent(HandlerMetricsRegistry.class);
            c.getComponent(RSocketThreadDumpResponder.class);
            c.getComponent(RSocketDomainEntityDataResponder.class);
        });

        configurer.onStart(() -> {
            Configuration config = configurer.buildConfiguration();

            AxoniqConsoleDispatchInterceptor interceptor = new AxoniqConsoleDispatchInterceptor(
                    config.getComponent(HandlerMetricsRegistry.class),
                    applicationName
            );

            config.eventBus().registerDispatchInterceptor(interceptor);
            config.commandBus().registerDispatchInterceptor(interceptor);
            config.queryBus().registerDispatchInterceptor(interceptor);
            config.deadlineManager().registerDispatchInterceptor(interceptor);
        });

        configurer.onShutdown(() -> {
            managementTaskExecutor.shutdown();
            reportingTaskExecutor.shutdown();
        });

        new AxoniqConsoleAggregateConfigurerModule().configureModule(configurer);
        new AxoniqConsoleEnhancingConfigurerModule(spanMatcherPredicateMap).configureModule(configurer);
    }

    /**
     * Builder class to instantiate a {@link AxoniqConsoleConfigurerModule}.
     */
    public static class Builder {

        private final String environmentId;
        private final String accessToken;
        private final String applicationName;

        private String host = "framework.console.axoniq.io";
        private Boolean secure = true;
        private Integer port = 7000;
        private String hostname = ManagementFactory.getRuntimeMXBean().getName();
        private String nodeId = randomNodeId();
        private AxoniqConsoleDlqMode dlqMode = AxoniqConsoleDlqMode.NONE;
        private final List<String> dlqDiagnosticsWhitelist = new ArrayList<>();
        private DomainEventAccessMode domainEventAccessMode = DomainEventAccessMode.NONE;
        private Long initialDelay = 0L;
        private boolean disableSpanFactoryInConfiguration = false;
        private final SpanMatcherPredicateMap spanMatcherPredicateMap = getSpanMatcherPredicateMap();

        private ScheduledExecutorService reportingTaskExecutor;
        private Integer reportingThreadPoolSize = 2;

        private ExecutorService managementTaskExecutor;
        private Integer managementMaxThreadPoolSize = 5;
        private EventScheduler eventScheduler;

        private ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules()
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        /**
         * Constructor to instantiate a {@link Builder} based on the fields contained in the
         * {@link AxoniqConsoleConfigurerModule.Builder}. Requires the {@code environmentId}, {@code accessToken} and
         * {@code applicationName} to be set.
         *
         * @param environmentId   The environment identifier of AxonIQ Console to connect.
         * @param accessToken     The access token needed to authenticate to the environment.
         * @param applicationName The display name of the application. Some special characters may be replaced with a
         *                        hyphen.
         */
        public Builder(String environmentId, String accessToken, String applicationName) {
            BuilderUtils.assertNonEmpty(environmentId, "AxonIQ Console environmentId may not be null or empty");
            BuilderUtils.assertNonEmpty(accessToken, "AxonIQ Console accessToken may not be null or empty");
            BuilderUtils.assertNonEmpty(applicationName, "AxonIQ Console applicationName may not be null or empty");
            this.environmentId = environmentId;
            this.accessToken = accessToken;
            this.applicationName = applicationName;
        }

        /**
         * The hostname of the application. Defaults to the runtime name of the JVM.
         * Needs to be unique in combination with the {@code nodeId}.
         * @param hostname The hostname of the application
         * @return The builder for fluent interfacing
         */
        public Builder hostname(String hostname) {
            BuilderUtils.assertNonEmpty(hostname, "Hostname may not be null or empty");
            this.hostname = hostname;
            return this;
        }

        /**
         * The node id of the application. Defaults to a random four character string.
         * This is to prevent conflicts when multiple instances of the same application are running on the same host.
         * @param nodeId The node id of the application
         * @return The builder for fluent interfacing
         */
        public Builder nodeId(String nodeId) {
            BuilderUtils.assertNonEmpty(nodeId, "Node id may not be null or empty");
            this.nodeId = nodeId;
            return this;
        }

        /**
         * The host to connect to. Defaults to {@code framework.console.axoniq.io}.
         *
         * @param host The host to connect to
         * @return The builder for fluent interfacing
         */
        public Builder host(String host) {
            BuilderUtils.assertNonEmpty(host, "AxonIQ Console host may not be null or empty");
            this.host = host;
            return this;
        }

        /**
         * The port to connect to. Defaults to {@code 7000}.
         *
         * @param port The port to connect to
         * @return The builder for fluent interfacing
         */
        public Builder port(Integer port) {
            BuilderUtils.assertNonNull(host, "AxonIQ Console port may not be null");
            this.port = port;
            return this;
        }

        /**
         * The mode of the DLQ to operate in. Defaults to {@link AxoniqConsoleDlqMode#NONE}, which means that no DLQ
         * data is available.
         *
         * @param dlqMode The mode to set the DLQ to
         * @return The builder for fluent interfacing
         */
        public Builder dlqMode(AxoniqConsoleDlqMode dlqMode) {
            BuilderUtils.assertNonNull(dlqMode, "AxonIQ Console dlqMode may not be null");
            this.dlqMode = dlqMode;
            return this;
        }

        /**
         * Adding a key to the diagnostics whitelist. Will only be used in combination with setting the {@code dlqMode}
         * to {@link AxoniqConsoleDlqMode#LIMITED}. It will filter the diagnostics, and only show the ones included in
         * the whitelist.
         *
         * @param key The value to add to the whitelist
         * @return The builder for fluent interfacing
         */
        public Builder addDlqDiagnosticsWhitelistKey(String key) {
            BuilderUtils.assertNonEmpty(key, "Dlq diagnostics whitelist key may not be null");
            this.dlqDiagnosticsWhitelist.add(key);
            return this;
        }

        /**
         * The mode of domain event access. Defaults to {@link DomainEventAccessMode#NONE}, which means
         * that no domain event payload is visible and aggregate reconstruction is not supported.
         *
         * @param domainEventAccessMode The access mode to set for domain events
         * @return The builder for fluent interfacing
         */
        public Builder domainEventAccessMode(DomainEventAccessMode domainEventAccessMode) {
            BuilderUtils.assertNonNull(domainEventAccessMode, "Domain event access mode may not be null");
            this.domainEventAccessMode = domainEventAccessMode;
            return this;
        }

        /**
         * The initial delay before attempting to establish a connection. Defaults to {@code 0}.
         *
         * @param initialDelay The delay in milliseconds
         * @return The builder for fluent interfacing
         */
        public Builder initialDelay(Long initialDelay) {
            BuilderUtils.assertPositive(initialDelay, "AxonIQ Console initialDelay must be positive");
            this.initialDelay = initialDelay;
            return this;
        }

        /**
         * The thread pool's size that is used for reporting tasks, such as sending metrics to AxonIQ Console. Defaults
         * to {@code 2}.
         *
         * @param reportingThreadPoolSize The thread pool size
         * @return The builder for fluent interfacing
         */
        public Builder reportingThreadPoolSize(Integer reportingThreadPoolSize) {
            BuilderUtils.assertPositive(reportingThreadPoolSize,
                                        "AxonIQ Console reportingThreadPoolSize must be positive");
            this.reportingThreadPoolSize = reportingThreadPoolSize;
            return this;
        }

        /**
         * The {@link ScheduledExecutorService} that should be used for reporting metrics. Defaults to a
         * {@link Executors#newScheduledThreadPool(int)} with the {@code threadPoolSize} of this builder if not set.
         *
         * @param executorService The executor service.
         * @return The builder for fluent interfacing
         */
        public Builder reportingTaskExecutor(ScheduledExecutorService executorService) {
            BuilderUtils.assertNonNull(reportingTaskExecutor, "AxonIQ Console reportingTaskExecutor must be non-null");
            this.reportingTaskExecutor = executorService;
            return this;
        }

        /**
         * The maximum amount of threads that can be active in the management thread pool. Defaults to {@code 5}. These
         * threads are used for tasks such as processing DLQ messages after requested by the UI.
         *
         * @param managementMaxThreadPoolSize The maximum amount of threads
         * @return The builder for fluent interfacing
         */
        public Builder managementMaxThreadPoolSize(Integer managementMaxThreadPoolSize) {
            BuilderUtils.assertPositive(managementMaxThreadPoolSize,
                                        "AxonIQ Console managementMaxThreadPoolSize must be positive");
            this.managementMaxThreadPoolSize = managementMaxThreadPoolSize;
            return this;
        }

        /**
         * The {@link ExecutorService} that should be used for management tasks. This thread pool is used for tasks such
         * as processing DLQ messages after requested by the UI. Defaults to a
         * {@link java.util.concurrent.ThreadPoolExecutor} with a minimum of 0 threads, a maximum of
         * {@code managementMaxThreadPoolSize} threads and a keep-alive time of 60 seconds.
         *
         * @param executorService The executor service
         * @return The builder for fluent interfacing
         */
        public Builder managementTaskExecutor(ExecutorService executorService) {
            BuilderUtils.assertNonNull(managementTaskExecutor,
                                       "AxonIQ Console managementTaskExecutor must be non-null");
            this.managementTaskExecutor = executorService;
            return this;
        }

        /**
         * Disables setting the {@link SpanFactory} if set to {@code true}. Defaults to {@code false}. Useful in case
         * frameworks override this and can cause a split-brain situation.
         *
         * @return The builder for fluent interfacing
         */
        public Builder disableSpanFactoryInConfiguration() {
            this.disableSpanFactoryInConfiguration = true;
            return this;
        }

        /**
         * Overwrites a span predicate. It might be necessary to set these when the naming of the spans is customized.
         * See {@link SpanMatcher} for the defaults, based on the Axon Framework version.
         *
         * @param spanMatcher the type os span to change.
         * @param predicate   the function to determine is a predicate with a certain name, matches the type.
         * @return The builder for fluent interfacing
         */
        public Builder changeSpanPredicate(SpanMatcher spanMatcher, Predicate<String> predicate) {
            BuilderUtils.assertNonNull(spanMatcher, "Span matcher to update spanMatcherPredicateMap must be non-null");
            BuilderUtils.assertNonNull(predicate, "Predicate to update spanMatcherPredicateMap must be non-null");
            spanMatcherPredicateMap.put(spanMatcher, predicate);
            return this;
        }

        /**
         * Whether to use a secure connection using SSL/TLS or not. Defaults to {@code true}.
         *
         * @param secure Whether to use a secure connection or not
         * @return The builder for fluent interfacing
         */
        public Builder secure(boolean secure) {
            this.secure = secure;
            return this;
        }

        /**
         * Set the event scheduler, this will be wrapped to be able to include these messages in the message flow.
         *
         * @param eventScheduler the event scheduler to use
         * @return The builder for fluent interfacing
         */
        public Builder eventScheduler(EventScheduler eventScheduler) {
            BuilderUtils.assertNonNull(eventScheduler, "Event scheduler must be non-null");
            this.eventScheduler = eventScheduler;
            return this;
        }

        /**
         * Set the object mapper to be used for serialization and deserialization of domain events.
         * Defaults to a new {@link ObjectMapper} with all modules registered and field visibility set to any.
         *
         * @param objectMapper the object mapper to use
         * @return The builder for fluent interfacing
         */
        public Builder objectMapper(ObjectMapper objectMapper) {
            BuilderUtils.assertNonNull(objectMapper, "Object mapper must be non-null");
            this.objectMapper = objectMapper;
            return this;
        }

        /**
         * Builds the {@link AxoniqConsoleConfigurerModule} based on the fields set in this {@link Builder}.
         *
         * @return The module
         */
        public AxoniqConsoleConfigurerModule build() {
            if (reportingTaskExecutor == null) {
                reportingTaskExecutor = Executors.newScheduledThreadPool(reportingThreadPoolSize);
            }
            if (managementTaskExecutor == null) {
                managementTaskExecutor = new ThreadPoolExecutor(
                        0,
                        managementMaxThreadPoolSize,
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>()
                );
            }
            return new AxoniqConsoleConfigurerModule(this);
        }

        private String randomNodeId() {
            String uuid = UUID.randomUUID().toString();
            return uuid.substring(uuid.length() - 4);
        }
    }
}

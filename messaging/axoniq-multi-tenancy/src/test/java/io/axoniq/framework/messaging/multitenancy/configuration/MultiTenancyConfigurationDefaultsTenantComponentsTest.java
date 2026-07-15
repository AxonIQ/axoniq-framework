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
import io.axoniq.framework.messaging.multitenancy.api.RegisterTenantDescriptorHandlerInterceptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantProvider;
import io.axoniq.framework.messaging.multitenancy.configuration.MultiTenancyConfigurationUtils.MultiTenancyEnabled;
import io.axoniq.framework.messaging.multitenancy.util.StubTenantProvider;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.conversion.PassThroughConverter;
import org.axonframework.messaging.commandhandling.CommandBus;
import org.axonframework.messaging.commandhandling.CommandResultMessage;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.commandhandling.annotation.AnnotatedCommandHandlingComponent;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.AnnotationMessageTypeResolver;
import org.axonframework.messaging.core.annotation.ClasspathHandlerDefinition;
import org.axonframework.messaging.core.annotation.ClasspathParameterResolverFactory;
import org.axonframework.messaging.core.annotation.MultiParameterResolverFactory;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.axonframework.messaging.core.conversion.DelegatingMessageConverter;
import org.axonframework.messaging.core.interception.HandlerInterceptorRegistry;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.axoniq.framework.messaging.multitenancy.api.MultiTenancyApiUtils.TENANT_RESOURCE_KEY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the tenant-component wiring performed by {@link MultiTenancyConfigurationDefaults} against a real
 * {@link MessagingConfigurer}: providers are subscribed to the {@link TenantProvider}, and message handler parameters
 * resolve to the tenant-scoped component of the tenant carried by the message.
 */
class MultiTenancyConfigurationDefaultsTenantComponentsTest {

    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("tenant-a");
    private static final TenantDescriptor TENANT_B = TenantDescriptor.tenantWithId("tenant-b");

    private final StubTenantProvider tenantProvider = new StubTenantProvider();
    private final TenantComponentProvider<CourseRepository> componentProvider =
            TenantComponentProvider.withFactory(CourseRepository.class, CourseRepository::new);

    private AxonConfiguration configuration;

    @BeforeEach
    void buildAndStartConfiguration() {
        tenantProvider.addTenant(TENANT_A);
        configuration = MessagingConfigurer.create()
                                           .componentRegistry(MultiTenancyEnabled::enableMultiTenancyEnhancer)
                                           .componentRegistry(registry -> registry
                                                   // Scanning would pull in the Axon Server enhancer, which needs
                                                   // eventsourcing classes absent from this module's test classpath.
                                                   .disableEnhancerScanning()
                                                   .registerEnhancer(new MultiTenancyConfigurationDefaults())
                                                   .registerComponent(TenantProvider.class, config -> tenantProvider)
                                                   .registerComponent(TenantComponentProvider.class,
                                                                      config -> componentProvider))
                                           .build();
        configuration.start();
    }

    @AfterEach
    void shutdownConfiguration() {
        configuration.shutdown();
    }

    @Nested
    class TenantLifecycleWiring {

        @Test
        void subscribesTheProviderAtStartupAndReplaysKnownTenants() {
            // then the started configuration subscribed the provider, without any component lookup being needed
            assertThat(tenantProvider.subscribedComponents()).contains(componentProvider);
            assertThat(componentProvider.tenants()).containsExactly(TENANT_A);
        }

        @Test
        void propagatesTenantsAddedAtRuntimeToTheProvider() {
            // when
            tenantProvider.addTenant(TENANT_B);

            // then
            assertThat(componentProvider.tenants()).containsExactlyInAnyOrder(TENANT_A, TENANT_B);
        }

        @Test
        void removingATenantDestroysItsComponentInstance() {
            // given
            tenantProvider.addTenant(TENANT_B);
            CourseRepository repository = componentProvider.componentFor(TENANT_B);

            // when
            tenantProvider.removeTenant(TENANT_B);

            // then
            assertThat(componentProvider.tenants()).containsExactly(TENANT_A);
            assertThat(repository.closed).isTrue();
        }

        @Test
        void shuttingDownTheTenantProviderDestroysAllComponentInstances() {
            // given
            CourseRepository repository = componentProvider.componentFor(TENANT_A);

            // when the tenant provider shuts down, deregistering all subscribed components
            tenantProvider.shutdown();

            // then
            assertThat(repository.closed).isTrue();
            assertThat(componentProvider.tenants()).isEmpty();
        }

        @Test
        void shuttingDownTheConfigurationDestroysAllComponentInstances() {
            // given instances for a replayed tenant and for a tenant added at runtime
            tenantProvider.addTenant(TENANT_B);
            CourseRepository repositoryA = componentProvider.componentFor(TENANT_A);
            CourseRepository repositoryB = componentProvider.componentFor(TENANT_B);

            // when the configuration shuts down, cancelling the retained provider subscriptions
            configuration.shutdown();

            // then both instances are destroyed, without relying on the tenant provider's own shutdown
            assertThat(repositoryA.closed).isTrue();
            assertThat(repositoryB.closed).isTrue();
            assertThat(componentProvider.tenants()).isEmpty();
        }
    }

    @Nested
    class HandlerParameterResolution {

        @Test
        void resolvesTheTenantScopedComponentThroughTheConfiguredParameterResolverFactory() throws Exception {
            // given
            ParameterResolverFactory factory = configuration.getComponent(ParameterResolverFactory.class);
            Method handler = SampleHandlers.class.getDeclaredMethod("handle", CourseRepository.class);
            ParameterResolver<?> resolver = factory.createInstance(handler, handler.getParameters(), 0);
            assertThat(resolver).isNotNull();
            Message message = new GenericMessage(
                    "message-id",
                    new MessageType("TestEvent"),
                    "payload".getBytes(),
                    Map.of(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, TENANT_A.tenantId())
            );

            TenantDescriptor tenantDescriptor = new MetadataBasedTenantResolver().resolveTenant(message);
            ProcessingContext context = StubProcessingContext.forMessage(message)
                                                             .withResource(TENANT_RESOURCE_KEY, tenantDescriptor);

            // when
            Object resolved = resolver.resolveParameterValue(context).join();

            // then
            assertThat(resolved).isSameAs(componentProvider.componentFor(TENANT_A));
            assertThat(((CourseRepository) resolved).tenant).isEqualTo(TENANT_A);
        }
    }

    @Nested
    class HandlerInterceptorWiring {

        @Test
        void registersTheTenantDescriptorInterceptorForCommandAndQueryHandlersOnly() {
            // given
            HandlerInterceptorRegistry registry = configuration.getComponent(HandlerInterceptorRegistry.class);

            // when
            var commandInterceptors = registry.commandInterceptors(configuration,
                                                                  TenantAwareCommandHandler.class,
                                                                  "handle");
            var queryInterceptors = registry.queryInterceptors(configuration,
                                                              TenantAwareQueryHandler.class,
                                                              "handle");
            var eventInterceptors = registry.eventInterceptors(configuration,
                                                              SampleHandlers.class,
                                                              "handle");

            // then
            assertThat(commandInterceptors)
                    .anyMatch(RegisterTenantDescriptorHandlerInterceptor.class::isInstance);
            assertThat(queryInterceptors)
                    .anyMatch(RegisterTenantDescriptorHandlerInterceptor.class::isInstance);
            assertThat(eventInterceptors)
                    .noneMatch(RegisterTenantDescriptorHandlerInterceptor.class::isInstance);
        }

        @Test
        void commandBusHandlingResolvesTenantScopedComponentsWithoutManuallySeedingTheProcessingContext() {
            // given
            TenantAwareCommandHandler handler = new TenantAwareCommandHandler();
            CommandBus commandBus = configuration.getComponent(CommandBus.class);
            ParameterResolverFactory parameterResolverFactory = MultiParameterResolverFactory.ordered(
                    ClasspathParameterResolverFactory.forClass(TenantAwareCommandHandler.class),
                    configuration.getComponent(ParameterResolverFactory.class)
            );
            commandBus.subscribe(new AnnotatedCommandHandlingComponent<>(
                    handler,
                    parameterResolverFactory,
                    ClasspathHandlerDefinition.forClass(TenantAwareCommandHandler.class),
                    new AnnotationMessageTypeResolver(),
                    new DelegatingMessageConverter(PassThroughConverter.INSTANCE)
            ));
            GenericCommandMessage command = new GenericCommandMessage(
                    new MessageType("tenant-aware-command"),
                    "payload",
                    Map.of(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, TENANT_A.tenantId())
            );

            // when
            CommandResultMessage result = commandBus.dispatch(command, null)
                                                    .orTimeout(5, TimeUnit.SECONDS)
                                                    .join();

            // then
            assertThat(result.payloadAs(String.class)).isEqualTo(TENANT_A.tenantId());
            assertThat(handler.resolvedRepository).isSameAs(componentProvider.componentFor(TENANT_A));
            assertThat(handler.resolvedRepository.tenant).isEqualTo(TENANT_A);
        }
    }

    @SuppressWarnings("unused")
    private static final class SampleHandlers {

        void handle(CourseRepository repository) {
            // Reflection target only. The parameter type drives the matching under test.
        }
    }

    private static final class TenantAwareCommandHandler {

        private CourseRepository resolvedRepository;

        @CommandHandler(commandName = "tenant-aware-command")
        String handle(String command, CourseRepository repository) {
            this.resolvedRepository = repository;
            return repository.tenant.tenantId();
        }
    }

    @SuppressWarnings("unused")
    private static final class TenantAwareQueryHandler {

        @org.axonframework.messaging.queryhandling.annotation.QueryHandler(queryName = "tenant-aware-query")
        String handle(String query, CourseRepository repository) {
            return repository.tenant.tenantId();
        }
    }

    private static final class CourseRepository implements AutoCloseable {

        private final TenantDescriptor tenant;
        private boolean closed;

        private CourseRepository(TenantDescriptor tenant) {
            this.tenant = tenant;
        }

        @Override
        public void close() {
            this.closed = true;
        }
    }
}

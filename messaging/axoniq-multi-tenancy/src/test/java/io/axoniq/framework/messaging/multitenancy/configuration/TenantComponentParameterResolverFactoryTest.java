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
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import io.axoniq.framework.messaging.multitenancy.api.TenantNotResolvedException;
import io.axoniq.framework.messaging.multitenancy.api.TenantResolver;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.messaging.commandhandling.GenericCommandMessage;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.Message;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.*;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static io.axoniq.framework.messaging.multitenancy.api.MultiTenancyApiUtils.TENANT_RESOURCE_KEY;
import static io.axoniq.framework.messaging.multitenancy.api.MultiTenancyApiUtils.setTenantDescriptor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class TenantComponentParameterResolverFactoryTest {

    private static final TenantDescriptor TENANT_A = TenantDescriptor.tenantWithId("tenant-a");

    private final Configuration configuration = mock(Configuration.class);
    private final TenantComponentParameterResolverFactory testSubject =
            new TenantComponentParameterResolverFactory(configuration);

    @Test
    @SuppressWarnings("DataFlowIssue")
    void rejectsNullConfiguration() {
        // when / then
        assertThatThrownBy(() -> new TenantComponentParameterResolverFactory(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Nested
    class ProviderMatching {

        @Test
        void returnsNullWhenNoProviderMatchesTheParameterType() throws Exception {
            // given
            givenProviders(courseRepositoryProvider());

            // when
            ParameterResolver<?> resolver = resolverFor("handlesUnrelated", String.class);

            // then
            assertThat(resolver).isNull();
        }

        @Test
        void matchesProviderByExactParameterType() throws Exception {
            // given a provider holding the exact JdbcCourseRepository type the handler declares
            givenProviders(jdbcCourseRepositoryProvider());

            // when
            ParameterResolver<?> resolver = resolverFor("handlesJdbcRepository", JdbcCourseRepository.class);

            // then
            assertThat(resolver).isNotNull();
        }

        @Test
        void matchesProviderByAssignableSupertype() throws Exception {
            // given a provider holding the concrete JdbcCourseRepository while the handler declares the interface
            givenProviders(jdbcCourseRepositoryProvider());

            // when
            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);

            // then
            assertThat(resolver).isNotNull();
        }

        @Test
        void rejectsDuplicateProvidersForTheSameExactComponentType() {
            // given two providers registered under different names for the same component type
            givenProviders(courseRepositoryProvider(), courseRepositoryProvider());

            // when / then
            assertThatThrownBy(() -> resolverFor("handlesCourseRepository", CourseRepository.class))
                    .isInstanceOf(AxonConfigurationException.class)
                    .hasMessageContaining(CourseRepository.class.getName());
        }

        @Test
        void rejectsAmbiguousAssignableMatches() {
            // given two providers whose component types both implement the declared CourseRepository interface
            givenProviders(jdbcCourseRepositoryProvider(), inMemoryCourseRepositoryProvider());

            // when / then
            assertThatThrownBy(() -> resolverFor("handlesCourseRepository", CourseRepository.class))
                    .isInstanceOf(AxonConfigurationException.class)
                    .hasMessageContaining(CourseRepository.class.getName())
                    .hasMessageContaining(JdbcCourseRepository.class.getName())
                    .hasMessageContaining(InMemoryCourseRepository.class.getName());
        }

        @Test
        void exactMatchWinsOverAmbiguousAssignableMatches() throws Exception {
            // given an exact match next to two assignable candidates
            givenProviders(jdbcCourseRepositoryProvider(),
                           inMemoryCourseRepositoryProvider(),
                           courseRepositoryProvider());

            // when
            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);

            // then
            assertThat(resolver).isNotNull();
        }
    }

    @Nested
    class ParameterResolution {

        @Test
        void resolvesTheTenantScopedComponentForTheTenantOnTheMessage() throws Exception {
            // given
            TenantComponentProvider<CourseRepository> provider = courseRepositoryProvider();
            provider.registerTenant(TENANT_A);
            givenProviders(provider);
            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);
            ProcessingContext context = StubProcessingContext.forMessage(messageForTenant(TENANT_A.tenantId()))
                                                             .withResource(TENANT_RESOURCE_KEY, TENANT_A);

            // when
            Object resolved = resolver.resolveParameterValue(context).join();

            // then
            assertThat(resolved).isSameAs(provider.componentFor(TENANT_A));
        }

        @Test
        void resolvesThroughAnAssignableSupertypeMatch() throws Exception {
            // given a provider for the concrete type while the handler declares the interface
            TenantComponentProvider<JdbcCourseRepository> provider = jdbcCourseRepositoryProvider();
            provider.registerTenant(TENANT_A);
            givenProviders(provider);
            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);
            ProcessingContext context = StubProcessingContext.forMessage(messageForTenant(TENANT_A.tenantId()))
                                                             .withResource(TENANT_RESOURCE_KEY, TENANT_A);

            // when
            Object resolved = resolver.resolveParameterValue(context).join();

            // then
            assertThat(resolved).isSameAs(provider.componentFor(TENANT_A));
        }

        @Test
        void resolvesFromTheMatchingProviderWhenMultipleAreRegistered() throws Exception {
            // given
            TenantComponentProvider<CourseRepository> courseProvider = courseRepositoryProvider();
            courseProvider.registerTenant(TENANT_A);
            TenantComponentProvider<AuditService> auditProvider = auditServiceProvider();
            givenProviders(courseProvider, auditProvider);
            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);
            ProcessingContext context = StubProcessingContext.forMessage(messageForTenant(TENANT_A.tenantId()))
                                                             .withResource(TENANT_RESOURCE_KEY, TENANT_A);

            // when
            Object resolved = resolver.resolveParameterValue(context).join();

            // then
            assertThat(resolved).isInstanceOf(CourseRepository.class)
                                .isSameAs(courseProvider.componentFor(TENANT_A));
        }

        @Test
        void usesTheTenantResolverRegisteredInTheTenantResolverRegistry() throws Exception {
            // given a registry with a resolver reading a custom metadata key
            TenantComponentProvider<CourseRepository> provider = courseRepositoryProvider();
            provider.registerTenant(TENANT_A);
            givenProviders(provider);

            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);
            Message message = new GenericMessage("message-id",
                                                 new MessageType("TestCommand"),
                                                 "payload".getBytes(),
                                                 Map.of("customTenantKey", TENANT_A.tenantId()));

            // when
            Object resolved = resolver.resolveParameterValue(
                    StubProcessingContext.forMessage(message)
                                         .withResource(TENANT_RESOURCE_KEY, TENANT_A)
            ).join();

            // then the tenant is resolved through the custom key, not the default tenantId key
            assertThat(resolved).isSameAs(provider.componentFor(TENANT_A));
        }

        @Test
        void honorsMessageTypeSpecificResolversFromTheRegistry() throws Exception {
            // given a command-specific resolver reading a custom key, next to the default for other messages
            TenantComponentProvider<CourseRepository> provider = courseRepositoryProvider();
            provider.registerTenant(TENANT_A);
            givenProviders(provider);

            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);
            Message commandMessage = new GenericCommandMessage(
                    new GenericMessage("message-id",
                                       new MessageType("TestCommand"),
                                       "payload".getBytes(),
                                       Map.of("commandTenantKey", TENANT_A.tenantId()))
            );

            // when the command resolves through the command-specific key
            Object resolved = resolver.resolveParameterValue(
                    StubProcessingContext.forMessage(commandMessage)
                                         .withResource(TENANT_RESOURCE_KEY, TENANT_A)
            ).join();

            // then
            assertThat(resolved).isSameAs(provider.componentFor(TENANT_A));

            // and a plain message still resolves through the default tenantId key
            Object resolvedDefault = resolver
                    .resolveParameterValue(
                            StubProcessingContext.forMessage(messageForTenant(TENANT_A.tenantId()))
                                                 .withResource(TENANT_RESOURCE_KEY, TENANT_A)
                    )
                    .join();
            assertThat(resolvedDefault).isSameAs(provider.componentFor(TENANT_A));
        }

        @Test
        void honorsAResolverRegisteredAfterAnEarlierHandlerInspection() throws Exception {
            // given a registry that only receives a custom resolver after a first handler was inspected
            TenantComponentProvider<CourseRepository> provider = courseRepositoryProvider();
            provider.registerTenant(TENANT_A);
            givenProviders(provider);

            when(configuration.getComponent(TenantResolver.class)).thenReturn(new MetadataBasedTenantResolver(
                    "lateTenantKey"));
            resolverFor("handlesCourseRepository", CourseRepository.class);

            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);
            Message message = new GenericMessage("message-id",
                                                 new MessageType("TestCommand"),
                                                 "payload".getBytes(),
                                                 Map.of("lateTenantKey", TENANT_A.tenantId()));

            ProcessingContext context = StubProcessingContext.forMessage(message);
            TenantDescriptor tenantDescriptor = new MetadataBasedTenantResolver("lateTenantKey").resolveTenant(message);
            setTenantDescriptor(context, tenantDescriptor);

            // when
            Object resolved = resolver.resolveParameterValue(
                    StubProcessingContext.forMessage(message)
                                         .withResource(TENANT_RESOURCE_KEY, TENANT_A)
            ).join();

            // then the late resolver is picked up by the later inspection
            assertThat(resolved).isSameAs(provider.componentFor(TENANT_A));
        }

        @Test
        void failsWhenTheMessageCarriesNoTenant() throws Exception {
            // given
            givenProviders(courseRepositoryProvider());
            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);
            ProcessingContext context = StubProcessingContext.forMessage(messageWithoutTenant());

            // when / then
            assertThatThrownBy(() -> resolver.resolveParameterValue(context).join())
                    .hasCauseInstanceOf(TenantNotResolvedException.class);
        }

        @Test
        void failsWhenTheTenantOnTheMessageIsNotRegistered() throws Exception {
            // given a provider without any registered tenants
            givenProviders(courseRepositoryProvider());
            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);
            ProcessingContext context = StubProcessingContext.forMessage(messageForTenant("unknown-tenant"));

            // when / then
            assertThatThrownBy(() -> resolver.resolveParameterValue(context).join())
                    .hasCauseInstanceOf(TenantNotResolvedException.class);
        }

        @Test
        void failsWhenNoMessageIsPresentInTheContext() throws Exception {
            // given
            givenProviders(courseRepositoryProvider());
            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);

            // when / then
            assertThatThrownBy(() -> resolver.resolveParameterValue(new StubProcessingContext()).join())
                    .hasCauseInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class Matches {

        @Test
        void matchesWhenAMessageIsPresentInTheContext() throws Exception {
            // given
            givenProviders(courseRepositoryProvider());
            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);

            // when
            boolean matches = resolver.matches(StubProcessingContext.forMessage(messageForTenant(TENANT_A.tenantId())));

            // then
            assertThat(matches).isTrue();
        }

        @Test
        void doesNotMatchWhenNoMessageIsPresentInTheContext() throws Exception {
            // given
            givenProviders(courseRepositoryProvider());
            ParameterResolver<?> resolver = resolverFor("handlesCourseRepository", CourseRepository.class);

            // when
            boolean matches = resolver.matches(new StubProcessingContext());

            // then
            assertThat(matches).isFalse();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void givenProviders(TenantComponentProvider<?>... providers) {
        Map<String, TenantComponentProvider> byName = new HashMap<>();
        for (int i = 0; i < providers.length; i++) {
            byName.put("provider-" + i, providers[i]);
        }
        when(configuration.getComponents(TenantComponentProvider.class)).thenReturn(byName);
    }

    private ParameterResolver<?> resolverFor(String methodName, Class<?> parameterType) throws Exception {
        Method method = SampleHandlers.class.getDeclaredMethod(methodName, parameterType);
        return testSubject.createInstance(method, method.getParameters(), 0);
    }

    private static TenantComponentProvider<CourseRepository> courseRepositoryProvider() {
        return TenantComponentProvider.withFactory(CourseRepository.class, JdbcCourseRepository::new);
    }

    private static TenantComponentProvider<JdbcCourseRepository> jdbcCourseRepositoryProvider() {
        return TenantComponentProvider.withFactory(JdbcCourseRepository.class, JdbcCourseRepository::new);
    }

    private static TenantComponentProvider<InMemoryCourseRepository> inMemoryCourseRepositoryProvider() {
        return TenantComponentProvider.withFactory(InMemoryCourseRepository.class, InMemoryCourseRepository::new);
    }

    private static TenantComponentProvider<AuditService> auditServiceProvider() {
        return TenantComponentProvider.withFactory(AuditService.class, DefaultAuditService::new);
    }

    private static Message messageForTenant(String tenantId) {
        return new GenericMessage("message-id",
                                  new MessageType("TestCommand"),
                                  "payload".getBytes(),
                                  Map.of(MetadataBasedTenantResolver.DEFAULT_TENANT_METADATA_KEY, tenantId));
    }

    private static Message messageWithoutTenant() {
        return new GenericMessage("message-id", new MessageType("TestCommand"), "payload".getBytes(), Map.of());
    }

    @SuppressWarnings("unused")
    private static final class SampleHandlers {

        void handlesCourseRepository(CourseRepository repository) {
            // Reflection target only. The parameter type drives the matching under test.
        }

        void handlesJdbcRepository(JdbcCourseRepository repository) {
            // Reflection target only. The parameter type drives the matching under test.
        }

        void handlesUnrelated(String value) {
            // Reflection target only. The parameter type drives the matching under test.
        }
    }

    private interface CourseRepository {

    }

    private record JdbcCourseRepository(TenantDescriptor tenant) implements CourseRepository {

    }

    private record InMemoryCourseRepository(TenantDescriptor tenant) implements CourseRepository {

    }

    private interface AuditService {

    }

    private record DefaultAuditService(TenantDescriptor tenant) implements AuditService {

    }
}

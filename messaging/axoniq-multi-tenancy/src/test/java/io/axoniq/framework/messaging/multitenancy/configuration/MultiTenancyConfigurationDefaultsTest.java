/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
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

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentRegistry;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.common.configuration.DefaultComponentRegistry;
import org.axonframework.common.configuration.StubLifecycleRegistry;
import org.axonframework.messaging.core.GenericMessage;
import org.axonframework.messaging.core.MessageType;
import org.axonframework.messaging.core.annotation.ParameterResolver;
import org.axonframework.messaging.core.annotation.ParameterResolverFactory;
import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.axonframework.messaging.core.unitofwork.StubProcessingContext;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MultiTenancyConfigurationDefaultsTest {

    @Nested
    class ParameterResolverRegistration {

        @Test
        void resolvesTenantScopedComponentsFromMessageMetadata() throws Exception {
            DefaultComponentRegistry registry = new DefaultComponentRegistry();
            registry.disableEnhancerScanning();
            registry.registerComponent(TenantComponentRegistry.class, cfg ->
                    new DefaultTenantComponentRegistry<>(
                            StringBuilder.class,
                            tenant -> new StringBuilder("repo-" + tenant.tenantId())
                    )
            );
            registry.registerEnhancer(new MultiTenancyConfigurationDefaults());

            Configuration configuration = registry.build(new StubLifecycleRegistry());
            ParameterResolverFactory parameterResolverFactory = configuration.getComponent(ParameterResolverFactory.class);

            Method method = SampleHandler.class.getDeclaredMethod("handle", String.class, StringBuilder.class);
            ParameterResolver<?> resolver = parameterResolverFactory.createInstance(method, method.getParameters(), 1);

            ProcessingContext context = StubProcessingContext.forMessage(
                    new GenericMessage(
                            new MessageType("TestCommand"),
                            "payload",
                            Map.of(MetadataBasedTenantResolver.DEFAULT_TENANT_KEY, "foo-a")
                    )
            );

            Object value = resolver.resolveParameterValue(context).orTimeout(5, java.util.concurrent.TimeUnit.SECONDS).join();

            assertThat(value).isInstanceOf(StringBuilder.class);
            assertThat(value.toString()).isEqualTo("repo-foo-a");
        }
    }

    private static class SampleHandler {

        @SuppressWarnings("unused")
        public void handle(String payload, StringBuilder repository) {
            // no-op
        }
    }
}

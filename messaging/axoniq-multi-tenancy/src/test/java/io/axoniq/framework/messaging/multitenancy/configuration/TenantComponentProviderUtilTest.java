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
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package io.axoniq.framework.messaging.multitenancy.configuration;

import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import org.axonframework.common.configuration.AmbiguousComponentMatchException;
import org.axonframework.common.configuration.Configuration;
import org.axonframework.conversion.ChainingContentTypeConverter;
import org.axonframework.conversion.Converter;
import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class TenantComponentProviderUtilTest {

    @Nested
    class Find {

        @Test
        void findsAProviderInTheRootOfAConfigurationHierarchy() {
            // given
            TenantComponentProvider<Converter> converterProvider = converterProvider();
            Configuration child = mock(Configuration.class);
            Configuration parent = mock(Configuration.class);
            Configuration root = mock(Configuration.class);
            when(child.getParent()).thenReturn(parent);
            when(parent.getParent()).thenReturn(root);
            when(root.getParent()).thenReturn(null);
            when(root.getComponents(TenantComponentProvider.class)).thenReturn(Map.of("converter", converterProvider));

            // when
            var result = TenantComponentProviderUtil.find(child, Converter.class);

            // then
            assertThat(result).containsSame(converterProvider);
        }

        @Test
        void rejectsMoreThanTwoProvidersForTheSameComponentType() {
            // given
            Configuration configuration = mock(Configuration.class);
            when(configuration.getParent()).thenReturn(null);
            when(configuration.getComponents(TenantComponentProvider.class)).thenReturn(Map.of(
                    "first", converterProvider(),
                    "second", converterProvider(),
                    "third", converterProvider()
            ));

            // when / then
            assertThatThrownBy(() -> TenantComponentProviderUtil.find(configuration, Converter.class))
                    .isInstanceOf(AmbiguousComponentMatchException.class)
                    .hasMessageContaining(TenantComponentProvider.class.getName());
        }
    }

    private static TenantComponentProvider<Converter> converterProvider() {
        return TenantComponentProvider.withFactory(Converter.class, tenant -> new ChainingContentTypeConverter());
    }
}

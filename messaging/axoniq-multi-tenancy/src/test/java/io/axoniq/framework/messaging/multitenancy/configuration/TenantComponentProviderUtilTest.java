/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
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

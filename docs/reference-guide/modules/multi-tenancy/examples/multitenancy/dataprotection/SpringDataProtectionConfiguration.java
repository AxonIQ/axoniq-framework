/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package multitenancy.dataprotection;

import io.axoniq.framework.dataprotection.api.FieldEncryptingConverter;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringDataProtectionConfiguration {

    // tag::register-tenant-converters[]
    @Bean
    public TenantComponentProvider<Converter> tenantDataProtectionConverters(
            DeclarativeDataProtectionConfiguration.TenantCryptoEngineFactory cryptoEngines
    ) {
        return TenantComponentProvider.withFactory(
                Converter.class,                                                    // <1>
                tenant -> new FieldEncryptingConverter(
                        cryptoEngines.createFor(tenant),                            // <2>
                        new JacksonConverter()
                )
        );
    }
    // end::register-tenant-converters[]
}

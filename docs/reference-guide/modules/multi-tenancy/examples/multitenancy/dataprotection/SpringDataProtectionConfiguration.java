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

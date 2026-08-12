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
import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.messaging.multitenancy.api.TenantComponentProvider;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.jackson.JacksonConverter;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;

public class DeclarativeDataProtectionConfiguration {

    private final TenantCryptoEngineFactory cryptoEngines;

    public DeclarativeDataProtectionConfiguration(TenantCryptoEngineFactory cryptoEngines) {
        this.cryptoEngines = cryptoEngines;
    }

    // tag::register-tenant-converters[]
    public void registerTenantConverters(MessagingConfigurer configurer) {
        configurer.componentRegistry(registry -> registry.registerComponent(
                TenantComponentProvider.class,                                      // <1>
                configuration -> TenantComponentProvider.withFactory(
                        Converter.class,                                            // <2>
                        tenant -> new FieldEncryptingConverter(
                                cryptoEngines.createFor(tenant),                    // <3>
                                new JacksonConverter()
                        )
                )
        ));
    }
    // end::register-tenant-converters[]

    @FunctionalInterface
    public interface TenantCryptoEngineFactory {

        CryptoEngine createFor(TenantDescriptor tenant);
    }
}

/*
 * Copyright (c) 2010-2025. AxonIQ B.V.
 *
 * Licensed under the AXONIQ SOFTWARE SUBSCRIPTION AGREEMENT TERMS,
 * Version September 2025 (the "License");
 * The software is available under Non-Production Free License.
 * Production use requires a paid license. See the License for the
 * specific language governing permissions and limitations under
 * the License.
 *
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */

package io.axoniq.dataprotection.sample.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.axoniq.framework.dataprotection.api.FieldEncrypter;
import io.axoniq.framework.dataprotection.api.FieldEncryptingConverter;
import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.dataprotection.cryptoengine.jpa.JpaCryptoEngine;
import io.axoniq.license.entitlement.EntitlementConfiguration;
import io.axoniq.license.entitlement.EntitlementManager;
import io.axoniq.license.entitlement.source.axonserver.AxonServerLicenseSource;
import jakarta.persistence.EntityManagerFactory;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.conversion.ChainingContentTypeConverter;
import org.axonframework.conversion.Converter;
import org.axonframework.conversion.json.JacksonConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;

/**
 * Configuration for field-level encryption in Axon Framework 5.x.
 * <p>
 * This configuration provides the base {@link Converter} bean which is a {@link FieldEncryptingConverter}.
 * Axon's autoconfiguration will automatically wrap this converter in {@code MessageConverter} and
 * {@code EventConverter} implementations, ensuring that:
 * - Events are stored encrypted in the event store
 * - Events are automatically decrypted when loaded from the event store
 * - Event handlers receive decrypted events with plain text @PersonalData fields
 * - Projection databases can store plain text values for query optimization
 * <p>
 * Additionally, this configuration ensures that TrackingEventProcessor is used instead of
 * PooledStreamingEventProcessor, as the pooled version doesn't support token reset in AF5.
 *
 * @author Stefan Mirkovic
 */
@org.springframework.context.annotation.Configuration
@AutoConfigureBefore(name = "org.axonframework.extension.springboot.autoconfig.ConverterAutoConfiguration")
@Order(0)
public class DataProtectionConfiguration {


    /**
     * Disables the {@link AxonServerLicenseSource} to prevent license fetching via Axon Server's gRPC
     * {@code LicenseService} endpoint.
     * <p>
     * This is required for backward compatibility when connecting to <strong>Axon Server 2025.x or earlier</strong>,
     * which does not expose the {@code LicenseService} endpoint. Without this configuration, the
     * {@code AxonServerLicenseSource} (priority 100) would be selected as the active license source, fail to
     * fetch a license, and trigger a grace period that eventually blocks all licensed features.
     * <p>
     * When disabled, the next highest-priority configured source will be used instead (e.g.,
     * {@code PlatformLicenseSource} at priority 70, {@code EnvLicenseSource} at priority 50,
     * or {@code FileLicenseSource} at priority 10).
     * <p>
     * <strong>Note:</strong> This bean is not needed when connecting to Axon Server 2026.x or later.
     *
     * @return a {@link ConfigurationEnhancer} that registers an {@link EntitlementConfiguration} with
     *         {@code AxonServerLicenseSource} disabled
     * @see EntitlementConfiguration#disableLicenseSource(Class)
     */
    @Bean
    public ConfigurationEnhancer disableAxonServerLicenseSource() {
        return registry -> registry.registerComponent(
                EntitlementConfiguration.class,
                c -> {
                    EntitlementConfiguration config = new EntitlementConfiguration();
                    config.disableLicenseSource(AxonServerLicenseSource.class);
                    return config;
                }
        );
    }


    /**
     * Provides a JPA-based CryptoEngine that stores encryption keys in H2 database.
     * <p>
     * Keys are persisted in the axoniq_gdpr_keys table and survive application restarts.
     * This is the production-ready implementation for encryption key management.
     *
     * @param entityManagerFactory JPA entity manager factory for database access
     * @param entitlementManager the entitlement manager for license validation
     * @return a JpaCryptoEngine instance for persistent key storage
     */
    @Bean
    public CryptoEngine cryptoEngine(EntityManagerFactory entityManagerFactory, EntitlementManager entitlementManager) {
        return new JpaCryptoEngine(entityManagerFactory, entitlementManager);
    }

    /**
     * Provides the default ObjectMapper if not already configured.
     */
    @Bean("defaultAxonObjectMapper")
    @ConditionalOnMissingBean(name = "defaultAxonObjectMapper")
    public ObjectMapper defaultAxonObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Creates the base converter bean that Axon Framework will use for all conversions.
     * <p>
     * This bean is a {@link FieldEncryptingConverter} which wraps a {@link JacksonConverter}
     * and handles automatic encryption/decryption of @PersonalData annotated fields.
     * <p>
     * Axon's {@code ConverterAutoConfiguration} will automatically use this converter to
     * construct {@code MessageConverter} and {@code EventConverter} beans.
     */
    @Bean(name = "converter")
    @Primary
    @ConditionalOnMissingBean(Converter.class)
    public Converter converter(CryptoEngine cryptoEngine,
                               @Qualifier("defaultAxonObjectMapper") ObjectMapper objectMapper) {
        ChainingContentTypeConverter contentTypeConverter = new ChainingContentTypeConverter(
                getClass().getClassLoader()
        );
        Converter delegateConverter = new JacksonConverter(objectMapper, contentTypeConverter);
        return new FieldEncryptingConverter(cryptoEngine, delegateConverter);
    }

    /**
     * Provides a FieldEncrypter bean for manual field-level encryption/decryption.
     * <p>
     * This bean is used in query handlers to decrypt @PersonalData fields before
     * returning query results to REST API clients. Required because Spring Boot's
     * default Jackson ObjectMapper doesn't automatically decrypt fields.
     * <p>
     * The FieldEncrypter uses the same CryptoEngine and Converter as the event store,
     * ensuring consistent encryption/decryption across the application.
     *
     * @param cryptoEngine the crypto engine for managing encryption keys
     * @return a FieldEncrypter instance for manual encryption/decryption operations
     */
    @Bean
    public FieldEncrypter fieldEncrypter(CryptoEngine cryptoEngine) {
        Converter converter = new JacksonConverter();
        return new FieldEncrypter(cryptoEngine, converter);
    }
}

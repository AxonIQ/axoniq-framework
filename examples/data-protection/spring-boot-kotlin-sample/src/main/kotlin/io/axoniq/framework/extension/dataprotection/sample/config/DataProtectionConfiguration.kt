/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
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
 *    https://www.axoniq.io/legal/terms-of-service
 *
 *
 */

package io.axoniq.framework.extension.dataprotection.sample.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.axoniq.framework.extension.dataprotection.api.FieldEncrypter
import io.axoniq.framework.extension.dataprotection.api.FieldEncryptingConverter
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine
import io.axoniq.framework.extension.dataprotection.cryptoengine.jpa.JpaCryptoEngine
import io.axoniq.license.entitlement.EntitlementManager
import jakarta.persistence.EntityManagerFactory
import org.axonframework.conversion.ChainingContentTypeConverter
import org.axonframework.conversion.Converter
import org.axonframework.conversion.jackson2.Jackson2Converter
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigureBefore
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.core.annotation.Order

/**
 * Configuration for field-level encryption in Axon Framework 5.x.
 *
 * This configuration provides the base Converter bean which is a FieldEncryptingConverter.
 * Axon's autoconfiguration will automatically wrap this converter in MessageConverter and
 * EventConverter implementations, ensuring that:
 * - Events are stored encrypted in the event store
 * - Events are automatically decrypted when loaded from the event store
 * - Event handlers receive decrypted events with plain text @PersonalData fields
 * - Projection databases can store plain text values for query optimization
 *
 * Additionally, this configuration ensures that TrackingEventProcessor is used instead of
 * PooledStreamingEventProcessor, as the pooled version doesn't support token reset in AF5.
 *
 */
@AutoConfiguration
@AutoConfigureBefore(name = ["org.axonframework.extension.springboot.autoconfig.ConverterAutoConfiguration"])
@Order(0)
class DataProtectionConfiguration {

    /**
     * Provides a JPA-based CryptoEngine that stores encryption keys in H2 database.
     *
     * Keys are persisted in the axoniq_gdpr_keys table and survive application restarts.
     * This is the production-ready implementation for encryption key management.
     *
     * @param entityManagerFactory JPA entity manager factory for database access
     * @param entitlementManager the entitlement manager for license validation
     * @return a JpaCryptoEngine instance for persistent key storage
     */
    @Bean
    fun cryptoEngine(entityManagerFactory: EntityManagerFactory, entitlementManager: EntitlementManager): CryptoEngine {
        return JpaCryptoEngine(entityManagerFactory, entitlementManager)
    }

    /**
     * Provides the default ObjectMapper if not already configured.
     */
    @Bean("defaultAxonObjectMapper")
    @ConditionalOnMissingBean(name = ["defaultAxonObjectMapper"])
    fun defaultAxonObjectMapper(): ObjectMapper {
        return ObjectMapper()
            .registerModule(JavaTimeModule())
            .registerKotlinModule()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
    }

    /**
     * Creates the base converter bean that Axon Framework will use for all conversions.
     *
     * This bean is a FieldEncryptingConverter which wraps a Jackson2Converter
     * and handles automatic encryption/decryption of @PersonalData annotated fields.
     *
     * Axon's ConverterAutoConfiguration will automatically use this converter to
     * construct MessageConverter and EventConverter beans.
     */
    @Bean(name = ["converter"])
    @Primary
    fun converter(
        cryptoEngine: CryptoEngine,
        @Qualifier("defaultAxonObjectMapper") objectMapper: ObjectMapper
    ): Converter {
        val contentTypeConverter = ChainingContentTypeConverter(
            this::class.java.classLoader
        )
        val delegateConverter = Jackson2Converter(objectMapper, contentTypeConverter)
        return FieldEncryptingConverter(cryptoEngine, delegateConverter)
    }

    /**
     * Provides a FieldEncrypter bean for manual field-level encryption/decryption.
     *
     * This bean is used in query handlers to decrypt @PersonalData fields before
     * returning query results to REST API clients. Required because Spring Boot's
     * default Jackson ObjectMapper doesn't automatically decrypt fields.
     *
     * The FieldEncrypter uses the same CryptoEngine and Converter as the event store,
     * ensuring consistent encryption/decryption across the application.
     *
     * @param cryptoEngine the crypto engine for managing encryption keys
     * @return a FieldEncrypter instance for manual encryption/decryption operations
     */
    @Bean
    fun fieldEncrypter(cryptoEngine: CryptoEngine): FieldEncrypter {
        val converter = Jackson2Converter()
        return FieldEncrypter(cryptoEngine, converter)
    }
}

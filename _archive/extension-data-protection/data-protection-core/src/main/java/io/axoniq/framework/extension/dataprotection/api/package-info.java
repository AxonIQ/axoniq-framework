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

/**
 * Annotations, exceptions and encryption service objects of the Axon Data Protection Module.
 *
 * <h2>Axon Framework 5.x</h2>
 * <p>
 * This module version is designed for Axon Framework 5.x, which uses the
 * {@link org.axonframework.conversion.Converter} API. The Serializer API from Axon Framework 4.x
 * is no longer available.
 * </p>
 *
 * <h3>Key Components</h3>
 * <ul>
 *   <li>{@link io.axoniq.framework.extension.dataprotection.api.FieldEncrypter} - Core encryption engine accepting
 *       {@link org.axonframework.conversion.Converter}</li>
 *   <li>{@link io.axoniq.framework.extension.dataprotection.api.FieldEncryptingConverter} - Converter wrapper for
 *       integrating field-level encryption with Axon Framework 5.x</li>
 * </ul>
 *
 * <h3>Configuration Example</h3>
 * <pre>{@code
 * @Configuration
 * @AutoConfigureBefore(name = "org.axonframework.extension.springboot.autoconfig.ConverterAutoConfiguration")
 * public class CryptoEngineConfig {
 *     @Bean
 *     public CryptoEngine cryptoEngine() {
 *         return new InMemoryCryptoEngine();
 *     }
 *
 *     @Bean("converter")
 *     @Primary
 *     public Converter converter(CryptoEngine cryptoEngine,
 *                                @Qualifier("defaultAxonObjectMapper") ObjectMapper objectMapper) {
 *         ChainingContentTypeConverter contentTypeConverter =
 *             new ChainingContentTypeConverter(getClass().getClassLoader());
 *         Converter delegateConverter = new JacksonConverter(objectMapper, contentTypeConverter);
 *         return new FieldEncryptingConverter(cryptoEngine, delegateConverter);
 *     }
 * }
 * }</pre>
 *
 * @author Frans van Buul
 */
@NullMarked
package io.axoniq.framework.extension.dataprotection.api;

import org.jspecify.annotations.NullMarked;
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

package io.axoniq.framework.extension.dataprotection.sample

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.boot.runApplication
import org.springframework.context.annotation.ComponentScan

/**
 * Sample Spring Boot application with Kotlin demonstrating Axon Data Protection extension.
 *
 * This application shows how to use field-level encryption annotations (@DataSubjectId, @PersonalData)
 * for GDPR compliance in event-sourced applications built with Axon Framework using Kotlin.
 *
 * Key features demonstrated:
 * - Automatic encryption/decryption during event serialization
 * - JPA-based crypto engine for key storage
 * - Field-level encryption with Kotlin data classes
 * - Cryptographic erasure (right to be forgotten)
 * - Integration of Kotlin with Spring Boot and Axon Framework
 *
 */
@SpringBootApplication
@EntityScan(
    basePackages = [
        "io.axoniq.framework.extension.dataprotection.sample",
        "io.axoniq.framework.extension.dataprotection.cryptoengine.jpa",
        "org.axonframework.eventsourcing.eventstore.jpa",
        "org.axonframework.messaging.eventhandling.processing.streaming.token.store.jpa"
    ]
)
@ComponentScan(
    basePackages = [
        "io.axoniq.framework.extension.dataprotection.sample"
    ]
)
class DataProtectionKotlinSampleApplication

fun main(args: Array<String>) {
    runApplication<DataProtectionKotlinSampleApplication>(*args)
}

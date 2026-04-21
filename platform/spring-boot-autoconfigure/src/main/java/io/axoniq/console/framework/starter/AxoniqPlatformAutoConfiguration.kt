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

package io.axoniq.console.framework.starter

import io.axoniq.platform.framework.AxoniqPlatformConfiguration
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@AutoConfiguration
@Configuration
@EnableConfigurationProperties(AxoniqPlatformSpringProperties::class)
class AxoniqPlatformAutoConfiguration {
    private val logger = LoggerFactory.getLogger(this::class.java)

    @Bean
    @ConditionalOnProperty("axoniq.platform.credentials", matchIfMissing = false)
    fun axoniqConsoleProperties(
            properties: AxoniqPlatformSpringProperties,
            applicationContext: ApplicationContext
    ): AxoniqPlatformConfiguration {
        val credentials = properties.credentials
                ?: throw IllegalArgumentException("No credentials were provided for the connection to Axoniq Platform. Please provide them as instructed through the 'axoniq.console.credentials' property.")
        if (!credentials.contains(":")) {
            throw IllegalArgumentException("The credentials for the connection to Axoniq Platform don't have the right format. Please provide them as instructed through the 'axoniq.console.credentials' property.")
        }
        val applicationName = getApplicationName(properties, applicationContext)
                ?: throw IllegalArgumentException("Was unable to determine your application's name. Please provide it through the 'axoniq.console.application-name' property.")
        val (environmentId, accessToken) = credentials.split(":")
        logger.info(
                "Setting up client for Axoniq Platform environment {}. This application will be registered as {}",
                environmentId,
                applicationName
        )
        return AxoniqPlatformConfiguration(environmentId, accessToken, applicationName)
                .port(properties.port)
                .host(properties.host)
                .secure(properties.isSecure)
                .initialDelay(properties.initialDelay)
    }

    private fun getApplicationName(
            properties: AxoniqPlatformSpringProperties,
            applicationContext: ApplicationContext
    ): String? {
        return (properties.applicationName?.trim()?.ifEmpty { null })
                ?: (applicationContext.applicationName.trim().ifEmpty { null })
                ?: (applicationContext.id?.removeSuffix("-1"))
    }


    @Bean
    fun axoniqPlatformBeanPostProcessor(): BeanPostProcessor {
        return AxoniqPlatformBeanPostProcessor()
    }

    class AxoniqPlatformBeanPostProcessor: BeanPostProcessor {
        override fun postProcessBeforeInitialization(bean: Any, beanName: String): Any? {
            return super.postProcessBeforeInitialization(bean, beanName)
        }
    }
}

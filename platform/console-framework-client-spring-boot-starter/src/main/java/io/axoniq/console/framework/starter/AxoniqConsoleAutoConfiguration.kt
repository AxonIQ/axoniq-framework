/*
 * Copyright (c) 2022-2023. AxonIQ B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.axoniq.console.framework.starter

import io.axoniq.console.framework.AxoniqConsoleProperties
import io.axoniq.console.framework.messaging.AxoniqConsoleSpanFactory
import org.axonframework.config.ConfigurerModule
import org.axonframework.tracing.MultiSpanFactory
import org.axonframework.tracing.NoOpSpanFactory
import org.axonframework.tracing.SpanFactory
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
@EnableConfigurationProperties(AxoniqConsoleSpringProperties::class)
class AxoniqConsoleAutoConfiguration {
    private val logger = LoggerFactory.getLogger(this::class.java)

    @Bean
    @ConditionalOnProperty("axoniq.console.credentials", matchIfMissing = false)
    fun axoniqConsoleConfigurerModule(
        properties: AxoniqConsoleSpringProperties,
        applicationContext: ApplicationContext
    ): ConfigurerModule {
        val credentials = properties.credentials
        if (credentials == null) {
            logger.warn("No credentials were provided for the connection to AxonIQ Console. Please provide them as instructed through the 'axoniq.console.credentials' property.")
            return ConfigurerModule {  }
        }
        val applicationName = properties.applicationName ?: applicationContext.applicationName
        val (environmentId, accessToken) = credentials.split(":")
        logger.info(
            "Setting up client for AxonIQ Console environment {}. This application will be registered as {}",
            environmentId,
            applicationName
        )
        return io.axoniq.console.framework.AxoniqConsoleConfigurerModule(
            properties = AxoniqConsoleProperties(
                host = properties.host,
                port = properties.port,
                initialDelay = properties.initialDelay,
                secure = properties.isSecure,
                environmentId = environmentId,
                accessToken = accessToken,
                applicationName = applicationName,
                dlqEnabled = properties.isDlqEnabled
            ),
            configureSpanFactory = false
        )
    }

    @Bean
    @ConditionalOnProperty("axoniq.console.credentials", matchIfMissing = false)
    fun axoniqConsoleSpanFactoryPostProcessor(): BeanPostProcessor = object : BeanPostProcessor {
        override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
            if (bean !is SpanFactory || bean is AxoniqConsoleSpanFactory) {
                return bean
            }
            val spanFactory = AxoniqConsoleSpanFactory()
            if (bean is NoOpSpanFactory) {
                return spanFactory
            }
            return MultiSpanFactory(
                listOf(
                    spanFactory,
                    bean
                )
            )
        }
    }
}

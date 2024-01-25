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

import io.axoniq.console.framework.AxoniqConsoleConfigurerModule
import io.axoniq.console.framework.messaging.AxoniqConsoleSpanFactory
import io.axoniq.console.framework.messaging.SpanMatcher.Companion.getSpanMatcherPredicateMap
import io.axoniq.console.framework.messaging.SpanMatcherPredicateMap
import org.axonframework.config.ConfigurerModule
import org.axonframework.tracing.MultiSpanFactory
import org.axonframework.tracing.NoOpSpanFactory
import org.axonframework.tracing.SpanFactory
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
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
            return ConfigurerModule { }
        }
        if (!credentials.contains(":")) {
            logger.warn("The credentials for the connection to AxonIQ Console don't have the right format. Please provide them as instructed through the 'axoniq.console.credentials' property.")
            return ConfigurerModule { }
        }
        val applicationName = (properties.applicationName?.trim()?.ifEmpty { null })
            ?: (applicationContext.applicationName.trim().ifEmpty { null })
            ?: (applicationContext.id?.removeSuffix("-1"))
        if (applicationName == null) {
            logger.warn("Was unable to determine your application's name. Please provide it through the 'axoniq.console.application-name' property.")
            return ConfigurerModule { }
        }
        val (environmentId, accessToken) = credentials.split(":")
        logger.info(
            "Setting up client for AxonIQ Console environment {}. This application will be registered as {}",
            environmentId,
            applicationName
        )
        return AxoniqConsoleConfigurerModule
            .builder(environmentId, accessToken, applicationName)
            .port(properties.port)
            .host(properties.host)
            .dlqMode(properties.dlqMode)
            .secure(properties.isSecure)
            .initialDelay(properties.initialDelay)
            .disableSpanFactoryInConfiguration()
            .managementMaxThreadPoolSize(properties.maxConcurrentManagementTasks)
            .build()
    }

    @Bean
    @ConditionalOnMissingBean(SpanMatcherPredicateMap::class)
    fun spanMatcherPredicateMap(): SpanMatcherPredicateMap {
        return getSpanMatcherPredicateMap()
    }

    @Bean
    @ConditionalOnProperty("axoniq.console.credentials", matchIfMissing = false)
    fun axoniqConsoleSpanFactoryPostProcessor(
            spanMatcherPredicateMap: SpanMatcherPredicateMap
    ): BeanPostProcessor = object : BeanPostProcessor {
        override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
            if (bean !is SpanFactory || bean is AxoniqConsoleSpanFactory) {
                return bean
            }
            val spanFactory = AxoniqConsoleSpanFactory(spanMatcherPredicateMap)
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

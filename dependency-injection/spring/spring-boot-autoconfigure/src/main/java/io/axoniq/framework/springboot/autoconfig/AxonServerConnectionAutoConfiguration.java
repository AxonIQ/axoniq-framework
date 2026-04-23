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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.testcontainer.AxonServerConnectionDetails;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;

import java.lang.reflect.Method;

/**
 * Spring Boot autoconfiguration that maps {@link AxonServerConnectionDetails} to the Axon Server
 * configuration bean's server address.
 * <p>
 * Activates when an {@link AxonServerConnectionDetails} bean is present — typically produced by
 * {@link io.axoniq.framework.testcontainer.AxonServerContainerConnectionDetailsFactory} when
 * {@link io.axoniq.framework.testcontainer.AxonServerContainer} is annotated with
 * {@link org.springframework.boot.testcontainers.service.connection.ServiceConnection}.
 * <p>
 * The gRPC address from the container is applied to {@code AxonServerConfiguration.setServers()} via
 * reflection to avoid a compile-time dependency on the connector module. This overrides the value
 * previously bound from the {@code axon.axonserver.servers} property.
 *
 * @author Mateusz Nowak
 * @since 5.1.0
 * @see AxonServerConnectionDetails
 */
@AutoConfiguration
@ConditionalOnClass(AxonServerConnectionDetails.class)
@ConditionalOnBean(AxonServerConnectionDetails.class)
public class AxonServerConnectionAutoConfiguration {

    /**
     * Creates a {@link BeanPostProcessor} that applies the routing servers address from
     * {@link AxonServerConnectionDetails} to any {@code AxonServerConfiguration} bean present in the
     * application context.
     * <p>
     * Reflection is used intentionally to avoid a compile-time dependency on the connector module,
     * keeping this autoconfiguration usable regardless of which Axon Server connector version is present.
     *
     * @param connectionDetails the connection details produced from the running container
     * @return a {@link BeanPostProcessor} that configures the AxonServer address
     */
    @Bean
    public BeanPostProcessor axonServerConnectionDetailsBeanPostProcessor(
            AxonServerConnectionDetails connectionDetails) {
        String servers = connectionDetails.routingServers();
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
                if (bean.getClass().getName().endsWith("AxonServerConfiguration")) {
                    try {
                        Method setServers = bean.getClass().getMethod("setServers", String.class);
                        setServers.invoke(bean, servers);
                    } catch (ReflectiveOperationException ignored) {
                        // bean does not have setServers(String) — not an AxonServerConfiguration we know
                    }
                }
                return bean;
            }
        };
    }
}

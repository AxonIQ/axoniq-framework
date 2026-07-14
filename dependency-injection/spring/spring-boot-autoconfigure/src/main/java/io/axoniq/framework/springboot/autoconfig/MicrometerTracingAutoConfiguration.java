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

package io.axoniq.framework.springboot.autoconfig;

import io.axoniq.framework.springboot.ThreadLocalContextPropagationProperties;
import io.axoniq.framework.tracing.micrometer.MicrometerSpanFactory;
import io.axoniq.framework.tracing.micrometer.MicrometerTracingConfigurationEnhancer;
import io.axoniq.framework.tracing.micrometer.threadlocal.MicrometerThreadLocalContextPropagationConfigurationEnhancer;
import io.micrometer.tracing.Tracer;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration that toggles the Micrometer tracing binding when the {@code axoniq-tracing-micrometer} module is on
 * the classpath. The binding itself self-wires via ServiceLoader: {@link MicrometerTracingConfigurationEnhancer}
 * registers the {@link org.axonframework.messaging.tracing.SpanFactory} from the {@link Tracer}/{@code Propagator}
 * beans (bridged to Axon components by the Spring configurer), and
 * {@link MicrometerThreadLocalContextPropagationConfigurationEnhancer} installs the thread-local bridge. This
 * autoconfiguration therefore contributes no wiring beans — only property-driven <em>toggles</em>:
 * <ul>
 *     <li>{@link #micrometerTracingDisablingEnhancer()} disables both enhancers when {@code axon.tracing.enabled=false},
 *     so no {@code SpanFactory} is registered and nothing is decorated;</li>
 *     <li>{@link #threadLocalContextPropagationTogglingEnhancer(ThreadLocalContextPropagationProperties)} disables only
 *     the thread-local bridge when {@code axon.tracing.thread-local-context-propagation.enabled=false}, leaving core
 *     tracing (spans + cross-service propagation) on.</li>
 * </ul>
 * The {@link Tracer}, {@code Propagator} and {@code ObservationRegistry} beans come from Spring Boot's own tracing
 * auto-configuration (Boot 3.5 / 4.0 {@code spring-boot-starter-actuator} + a bridge such as
 * {@code micrometer-tracing-bridge-otel}).
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
@AutoConfiguration(afterName = "org.springframework.boot.actuate.autoconfigure.tracing.MicrometerTracingAutoConfiguration")
@ConditionalOnClass({Tracer.class, MicrometerSpanFactory.class})
@EnableConfigurationProperties(ThreadLocalContextPropagationProperties.class)
public class MicrometerTracingAutoConfiguration {

    /**
     * Disables the Micrometer tracing enhancers when {@code axon.tracing.enabled=false}, so that even with the module
     * (and a {@link Tracer} bean) on the classpath no {@link org.axonframework.messaging.tracing.SpanFactory} is
     * registered and every component stays undecorated.
     *
     * @return a {@link ConfigurationEnhancer} that disables both Micrometer tracing enhancers
     */
    @Bean
    @ConditionalOnProperty(prefix = "axon.tracing", name = "enabled", havingValue = "false")
    public ConfigurationEnhancer micrometerTracingDisablingEnhancer() {
        return registry -> {
            registry.disableEnhancer(MicrometerTracingConfigurationEnhancer.class);
            registry.disableEnhancer(MicrometerThreadLocalContextPropagationConfigurationEnhancer.class);
        };
    }

    /**
     * Disables the {@link MicrometerThreadLocalContextPropagationConfigurationEnhancer} — and with it the
     * {@code UnitOfWorkFactory} decorator and thread-local span accessor that make the active Axon span current on the
     * framework's worker threads — when {@code axon.tracing.thread-local-context-propagation.enabled} is {@code false}.
     * Core tracing (spans and cross-service propagation) is unaffected. When the property is {@code true} (the default)
     * this enhancer does nothing and the ServiceLoader-discovered enhancer installs the bridge as usual.
     *
     * @param properties the bound {@link ThreadLocalContextPropagationProperties}
     * @return a {@link ConfigurationEnhancer} that disables the thread-local bridge when it is switched off
     */
    @Bean
    public ConfigurationEnhancer threadLocalContextPropagationTogglingEnhancer(
            ThreadLocalContextPropagationProperties properties) {
        return registry -> {
            if (!properties.isEnabled()) {
                registry.disableEnhancer(MicrometerThreadLocalContextPropagationConfigurationEnhancer.class);
            }
        };
    }
}

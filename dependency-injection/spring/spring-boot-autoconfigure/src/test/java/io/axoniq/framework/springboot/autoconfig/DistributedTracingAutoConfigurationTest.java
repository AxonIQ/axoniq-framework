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

import io.axoniq.framework.messaging.distributed.tracing.DistributedTracingSettings;
import io.axoniq.framework.springboot.DistributedTracingProperties;
import org.axonframework.common.configuration.AxonConfiguration;
import org.axonframework.common.configuration.ConfigurationEnhancer;
import org.axonframework.messaging.core.configuration.MessagingConfigurer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests validating the wiring of {@link DistributedTracingAutoConfiguration} through Spring Boot's
 * {@link ApplicationContextRunner}.
 * <p>
 * The Spring layer is a thin properties-to-settings translation: assertions therefore run at the framework-component
 * level — the enhancer bean participates in a {@link MessagingConfigurer} build and the resulting
 * {@link AxonConfiguration} is inspected for the {@link DistributedTracingSettings} component read by the
 * distributed-connector tracing enhancer.
 *
 * @author Mateusz Nowak
 * @since 5.3.0
 */
class DistributedTracingAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(DistributedTracingAutoConfiguration.class));

    private static AxonConfiguration configurationFrom(ApplicationContext context) {
        return MessagingConfigurer.create()
                                  .componentRegistry(registry -> context.getBeansOfType(ConfigurationEnhancer.class)
                                                                        .values()
                                                                        .forEach(e -> e.enhance(registry)))
                                  .build();
    }

    @Test
    void connectorTogglesReachTheDistributedSettingsComponent() {
        // given / when the connector toggles are set, the settings component must carry them — the component the
        // distributed tracing enhancer reads when decorating the CommandBusConnector / QueryBusConnector
        contextRunner.withPropertyValues(
                             "axon.tracing.command-bus-connector.enabled=false",
                             "axon.tracing.query-bus-connector.enabled=false")
                     .run(context -> {
                         AxonConfiguration configuration = configurationFrom(context);

                         // then
                         DistributedTracingSettings settings =
                                 configuration.getComponent(DistributedTracingSettings.class);
                         assertThat(settings.commandBusConnectorEnabled()).isFalse();
                         assertThat(settings.queryBusConnectorEnabled()).isFalse();
                     });
    }

    @Test
    void connectorsAreEnabledByDefault() {
        // given / when
        contextRunner.run(context -> {
            assertThat(context).hasSingleBean(DistributedTracingProperties.class);
            AxonConfiguration configuration = configurationFrom(context);

            // then
            DistributedTracingSettings settings = configuration.getComponent(DistributedTracingSettings.class);
            assertThat(settings.commandBusConnectorEnabled()).isTrue();
            assertThat(settings.queryBusConnectorEnabled()).isTrue();
        });
    }

    @Test
    void tracingDisabledContributesNoEnhancerAtAll() {
        // given / when / then — the autoconfiguration backs off entirely
        contextRunner.withPropertyValues("axon.tracing.enabled=false")
                     .run(context -> assertThat(context).doesNotHaveBean("distributedTracingConfigurationEnhancer"));
    }

    @Test
    void userRegisteredSettingsComponentWinsOverThePropertyTranslation() {
        // given a user-registered DistributedTracingSettings component and a conflicting property
        DistributedTracingSettings userSettings = new DistributedTracingSettings(false, false);
        contextRunner.withPropertyValues("axon.tracing.command-bus-connector.enabled=true")
                     .run(context -> {
                         AxonConfiguration configuration = MessagingConfigurer.create()
                                 .componentRegistry(registry -> {
                                     registry.registerComponent(DistributedTracingSettings.class, c -> userSettings);
                                     context.getBeansOfType(ConfigurationEnhancer.class).values()
                                            .forEach(e -> e.enhance(registry));
                                 })
                                 .build();

                         // when / then — registerIfNotPresent leaves the user registration in place
                         assertThat(configuration.getComponent(DistributedTracingSettings.class))
                                 .isSameAs(userSettings);
                     });
    }
}

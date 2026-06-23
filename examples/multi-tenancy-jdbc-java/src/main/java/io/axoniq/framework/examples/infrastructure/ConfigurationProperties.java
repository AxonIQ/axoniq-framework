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

package io.axoniq.framework.examples.infrastructure;

import io.axoniq.framework.axonserver.connector.api.AxonServerConfiguration;
import io.axoniq.platform.framework.AxoniqPlatformConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.configuration.ComponentBuilder;

public record ConfigurationProperties(
        String applicationName,
        Platform platform
) {

    public record Platform(
            String applicationName,
            String environmentId,
            String accessToken,
            String host
    ) {

        Platform(Configuration configuration) {
            this(
                    configuration.getString("application.name"),
                    configuration.getString("platform.environmentId"),
                    configuration.getString("platform.accessToken"),
                    configuration.getString("platform.host")
            );
        }

        public ComponentBuilder<AxoniqPlatformConfiguration> axoniqPlatformConfiguration() {
            return c -> new AxoniqPlatformConfiguration(
                    environmentId,
                    accessToken,
                    applicationName
            ).host(host);
        }
    }

    ConfigurationProperties(Configuration configuration) {
        this(configuration.getString("application.name"), new Platform(configuration));
    }

    public ComponentBuilder<AxonServerConfiguration> axonServerConfiguration() {
        return c -> AxonServerConfiguration.builder()
                                           .componentName(applicationName)
                                           .build();
    }

    public static ConfigurationProperties load() {
        try {
            var config = new Configurations()
                    .properties(ConfigurationProperties.class.getResource("/application.properties"));
            return new ConfigurationProperties(config);
        } catch (ConfigurationException e) {
            throw new AxonConfigurationException(e.getMessage());
        }
    }
}

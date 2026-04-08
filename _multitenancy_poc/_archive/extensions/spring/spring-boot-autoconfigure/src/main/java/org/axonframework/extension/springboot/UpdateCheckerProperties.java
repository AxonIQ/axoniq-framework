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

package org.axonframework.extension.springboot;

import org.axonframework.update.configuration.UsagePropertyProvider;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.function.Supplier;

/**
 * An {@link UsagePropertyProvider} implementation that reacts to Spring Boot properties through the
 * {@link ConfigurationProperties} annotation.
 * <p>
 * This component allows users to disable the {@link org.axonframework.update.UpdateChecker} through application
 * properties.
 *
 * @author Allard Buijze
 * @since 4.12.2
 */
@ConfigurationProperties(prefix = "axon.update-check")
public class UpdateCheckerProperties implements Supplier<UsagePropertyProvider> {

    /**
     * Indicates whether the update check should be disabled.
     * <p>
     * Defaults to detecting this setting via system properties or environment variables. Unless disabled in any one of
     * these locations, the update check will be enabled.
     */
    private Boolean disabled;

    /**
     * The url to use to check for updates. Generally doesn't need to be changed, unless there is a local proxy, or for
     * test purposes. Defaults to the url defined via system properties or environment variables.
     */
    private String url;

    /**
     * Set's the boolean dictating whether the {@link org.axonframework.update.UpdateChecker} is disabled.
     * <p>
     * Defaults to detecting this setting via system properties or environment variables. Unless disabled in any one of
     * these locations, the update check will be enabled.
     *
     * @param disabled A boolean dictating whether the {@link org.axonframework.update.UpdateChecker} is disabled.
     */
    public void setDisabled(Boolean disabled) {
        this.disabled = disabled;
    }

    public Boolean getDisabled() {
        return disabled;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int priority() {
        // higher than Environment Variables, but lower than Command Line Arguments
        return Integer.MAX_VALUE - Short.MAX_VALUE;
    }

    @Override
    public UsagePropertyProvider get() {
        return new UsagePropertyProvider() {
            @Override
            public Boolean getDisabled() {
                return UpdateCheckerProperties.this.getDisabled();
            }

            @Override
            public String getUrl() {
                return UpdateCheckerProperties.this.getUrl();
            }

            @Override
            public int priority() {
                return UpdateCheckerProperties.this.priority();
            }
        };
    }
}


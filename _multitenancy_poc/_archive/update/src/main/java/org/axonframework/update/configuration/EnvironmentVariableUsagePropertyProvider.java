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

package org.axonframework.update.configuration;

import org.axonframework.common.annotation.Internal;

import java.util.Objects;

/**
 * A {@link UsagePropertyProvider} implementation that reads the usage properties from the
 * environment variables. The priority is half of max integer value, meaning it will be overridden by
 * the command-line properties provider.
 *
 * @author Mitchell Herrijgers
 * @since 5.0.0
 */
@Internal
public class EnvironmentVariableUsagePropertyProvider implements UsagePropertyProvider {

    /**
     * The environment variable key to check if the update checker is disabled.
     */
    public static final String DISABLED_KEY = "AXONIQ_UPDATE_CHECKER_DISABLED";
    /**
     * The environment variable key to retrieve the URL for the usage collection endpoint.
     */
    public static final String URL_KEY = "AXONIQ_UPDATE_CHECKER_URL";

    private final EnvironmentVariableSupplier envSupplier;

    /**
     * Creates a new {@code EnvironmentVariableUsagePropertyProvider} that reads properties from the system environment.
     * This constructor uses {@link System#getenv()} as the default supplier for environment variables.
     */
    public EnvironmentVariableUsagePropertyProvider() {
        this(System::getenv);
    }

    /**
     * Creates a new {@code EnvironmentVariableUsagePropertyProvider} that reads properties from the provided
     * {@link EnvironmentVariableSupplier}. This allows for custom implementations to provide environment variable
     * values, which can be useful for testing or when the default {@link System#getenv()} is not suitable.
     *
     * @param envSupplier The supplier to use for retrieving environment variables.
     */
    public EnvironmentVariableUsagePropertyProvider(EnvironmentVariableSupplier envSupplier) {
        this.envSupplier = Objects.requireNonNull(envSupplier, "The envSupplier must not be null.");
    }

    @Override
    public Boolean getDisabled() {
        String property = envSupplier.get(DISABLED_KEY);
        if (property != null) {
            return Boolean.parseBoolean(property);
        }
        return null;
    }

    @Override
    public String getUrl() {
        return envSupplier.get(URL_KEY);
    }

    @Override
    public int priority() {
        return Integer.MAX_VALUE / 2;
    }

    /**
     * A functional interface to supply environment variables. This allows for custom implementations to provide
     * environment variable values, which can be useful for testing or when the default {@link System#getenv()} is not
     * suitable.
     */
    @FunctionalInterface
    public interface EnvironmentVariableSupplier {
        /**
         * Retrieves the value of the specified environment variable.
         *
         * @param key The name of the environment variable to retrieve.
         * @return The value of the environment variable, or {@code null} if it is not set.
         */
        String get(String key);
    }
}

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

package io.axoniq.framework.springboot;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring Boot configuration properties for AxoniqFramework distributed tracing.
 * <p>
 * Binds to the {@code axon.tracing.*} property namespace. The {@link #isEnabled() master switch} toggles tracing
 * autoconfiguration as a whole, while nested settings allow tuning individual concerns such as which messaging
 * components are decorated:
 * <pre>{@code
 * axon:
 *   tracing:
 *     enabled: true
 *     command-bus:
 *       enabled: true
 * }</pre>
 *
 * @author Mateusz Nowak
 * @since 5.2.0
 */
@ConfigurationProperties(prefix = "axon.tracing")
public class TracingProperties {

    /**
     * Master switch enabling AxoniqFramework tracing autoconfiguration. Defaults to {@code true}.
     */
    private boolean enabled = true;

    /**
     * Tracing settings for the {@code CommandBus}.
     */
    private final CommandBus commandBus = new CommandBus();

    /**
     * Returns whether tracing autoconfiguration is enabled.
     *
     * @return {@code true} when tracing is enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether tracing autoconfiguration is enabled.
     *
     * @param enabled {@code true} to enable tracing, {@code false} to disable it
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Returns the tracing settings for the {@code CommandBus}.
     *
     * @return the {@code CommandBus} tracing settings, never {@code null}
     */
    public CommandBus getCommandBus() {
        return commandBus;
    }

    /**
     * Tracing settings for the {@code CommandBus}.
     */
    public static class CommandBus {

        /**
         * Whether the {@code CommandBus} is decorated with tracing. Defaults to {@code true}.
         */
        private boolean enabled = true;

        /**
         * Returns whether the {@code CommandBus} is decorated with tracing.
         *
         * @return {@code true} when {@code CommandBus} tracing is enabled, {@code false} otherwise
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * Sets whether the {@code CommandBus} is decorated with tracing.
         *
         * @param enabled {@code true} to enable {@code CommandBus} tracing, {@code false} to disable it
         */
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}

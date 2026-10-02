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

package io.axoniq.framework.springcloud.query;

import org.junit.jupiter.api.*;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test class validating the {@link SpringCloudQueryControllerConfiguration}.
 *
 * @author Mateusz Nowak
 */
class SpringCloudQueryControllerConfigurationTest {

    @Nested
    class Defaults {

        @Test
        void defaultConfigurationUsesTheDefaultQueryTimeoutAndKeepAliveInterval() {
            // when
            SpringCloudQueryControllerConfiguration configuration = SpringCloudQueryControllerConfiguration.DEFAULT;

            // then
            assertThat(configuration.queryTimeout())
                    .isEqualTo(SpringCloudQueryControllerConfiguration.DEFAULT_QUERY_TIMEOUT);
            assertThat(configuration.keepAliveInterval())
                    .isEqualTo(SpringCloudQueryControllerConfiguration.DEFAULT_KEEP_ALIVE_INTERVAL);
        }
    }

    @Nested
    class FluentAdjustment {

        @Test
        void queryTimeoutReturnsACopyWithOnlyTheQueryTimeoutChanged() {
            // given
            SpringCloudQueryControllerConfiguration original = SpringCloudQueryControllerConfiguration.DEFAULT;

            // when
            SpringCloudQueryControllerConfiguration adjusted = original.queryTimeout(Duration.ofMinutes(1));

            // then
            assertThat(adjusted.queryTimeout()).isEqualTo(Duration.ofMinutes(1));
            assertThat(adjusted.keepAliveInterval()).isEqualTo(original.keepAliveInterval());
            assertThat(original.queryTimeout())
                    .isEqualTo(SpringCloudQueryControllerConfiguration.DEFAULT_QUERY_TIMEOUT);
        }

        @Test
        void keepAliveIntervalReturnsACopyWithOnlyTheKeepAliveIntervalChanged() {
            // given
            SpringCloudQueryControllerConfiguration original = SpringCloudQueryControllerConfiguration.DEFAULT;

            // when
            SpringCloudQueryControllerConfiguration adjusted = original.keepAliveInterval(Duration.ofSeconds(5));

            // then
            assertThat(adjusted.keepAliveInterval()).isEqualTo(Duration.ofSeconds(5));
            assertThat(adjusted.queryTimeout()).isEqualTo(original.queryTimeout());
            assertThat(original.keepAliveInterval())
                    .isEqualTo(SpringCloudQueryControllerConfiguration.DEFAULT_KEEP_ALIVE_INTERVAL);
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsAMissingQueryTimeout() {
            assertThatThrownBy(() -> SpringCloudQueryControllerConfiguration.DEFAULT.queryTimeout(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("queryTimeout");
        }

        @Test
        void rejectsAMissingKeepAliveInterval() {
            assertThatThrownBy(() -> SpringCloudQueryControllerConfiguration.DEFAULT.keepAliveInterval(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("keepAliveInterval");
        }

        @Test
        void rejectsAKeepAliveIntervalThatWouldNeverBeat() {
            assertThatThrownBy(() -> SpringCloudQueryControllerConfiguration.DEFAULT.keepAliveInterval(Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must be positive");
        }

        @Test
        void rejectsANegativeKeepAliveInterval() {
            assertThatThrownBy(() -> SpringCloudQueryControllerConfiguration.DEFAULT.keepAliveInterval(
                    Duration.ofSeconds(-1)
            ))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must be positive");
        }
    }
}

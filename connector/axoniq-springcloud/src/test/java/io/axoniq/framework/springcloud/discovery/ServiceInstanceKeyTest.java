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

package io.axoniq.framework.springcloud.discovery;

import io.axoniq.framework.springcloud.util.TestServiceInstance;
import org.junit.jupiter.api.*;
import org.springframework.cloud.client.ServiceInstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that a {@link ServiceInstanceKey} identifies a service instance by value, which is the whole reason it
 * exists: several Spring Cloud Discovery implementations return instances that do not override
 * {@link Object#equals(Object)}.
 *
 * @author Allard Buijze
 */
class ServiceInstanceKeyTest {

    @Nested
    class Identity {

        @Test
        void treatsTwoObjectsForTheSameAddressAsOneInstance() {
            // given — two distinct objects describing the same running service, as two discovery lookups produce
            ServiceInstance first = TestServiceInstance.instance("university", "localhost", 8080);
            ServiceInstance second = TestServiceInstance.instance("university", "localhost", 8080);

            // when
            ServiceInstanceKey firstKey = ServiceInstanceKey.of(first);
            ServiceInstanceKey secondKey = ServiceInstanceKey.of(second);

            // then
            assertThat(first).isNotSameAs(second);
            assertThat(firstKey).isEqualTo(secondKey).hasSameHashCodeAs(secondKey);
        }

        @Test
        void distinguishesInstancesByPort() {
            // given
            ServiceInstance onePort = TestServiceInstance.instance("university", "localhost", 8080);
            ServiceInstance anotherPort = TestServiceInstance.instance("university", "localhost", 8081);

            // when / then
            assertThat(ServiceInstanceKey.of(onePort)).isNotEqualTo(ServiceInstanceKey.of(anotherPort));
        }

        @Test
        void distinguishesInstancesByHost() {
            // given
            ServiceInstance oneHost = TestServiceInstance.instance("university", "node-a", 8080);
            ServiceInstance anotherHost = TestServiceInstance.instance("university", "node-b", 8080);

            // when / then
            assertThat(ServiceInstanceKey.of(oneHost)).isNotEqualTo(ServiceInstanceKey.of(anotherHost));
        }

        @Test
        void distinguishesInstancesByServiceId() {
            // given
            ServiceInstance oneService = TestServiceInstance.instance("university", "localhost", 8080);
            ServiceInstance anotherService = TestServiceInstance.instance("library", "localhost", 8080);

            // when / then
            assertThat(ServiceInstanceKey.of(oneService)).isNotEqualTo(ServiceInstanceKey.of(anotherService));
        }
    }

    @Nested
    class Describing {

        @Test
        void readsAsTheAddressItIdentifies() {
            // given
            ServiceInstance instance = TestServiceInstance.instance("university", "localhost", 8080);

            // when / then
            assertThat(ServiceInstanceKey.of(instance)).hasToString("university@localhost:8080");
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsANullInstance() {
            // when / then
            assertThatThrownBy(() -> ServiceInstanceKey.of(null))
                    .isInstanceOf(NullPointerException.class);
        }
    }
}

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

package io.axoniq.framework.messaging.multitenancy.api;

import org.junit.jupiter.api.*;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TenantDescriptorTest {

    private static final String TENANT_ID_ONE = "me";
    private static final String TENANT_ID_TWO = "you";

    private TenantDescriptor testSubjectOne;
    private TenantDescriptor testSubjectTwo;
    private TenantDescriptor testSubjectThree;
    private TenantDescriptor testSubjectFour;
    private TenantDescriptor testSubjectFive;

    @BeforeEach
    void setUp() {
        Map<String, String> testPropertiesOne = Map.of(
                "key", "value",
                "key1", "value2"
        );
        Map<String, String> testPropertiesTwo = Map.of(
                "value", "key",
                "value2", "key1"
        );

        testSubjectOne = TenantDescriptor.tenantWithId(TENANT_ID_ONE);
        testSubjectTwo = TenantDescriptor.tenantWithId(TENANT_ID_TWO);

        testSubjectThree = new TenantDescriptor(TENANT_ID_ONE, testPropertiesOne);
        testSubjectFour = new TenantDescriptor(TENANT_ID_TWO, testPropertiesTwo);
        testSubjectFive = new TenantDescriptor(TENANT_ID_ONE, testPropertiesTwo);
    }

    @Nested
    class Equals {

        @Test
        void equalsOnlyValidatesTenantId() {
            // then - validate test subject one, only matching on tenant id
            assertThat(testSubjectOne).isNotEqualTo(testSubjectTwo)
                                      .isEqualTo(testSubjectThree)
                                      .isNotEqualTo(testSubjectFour)
                                      .isEqualTo(testSubjectFive);

            // then - validate test subject two, only matching on tenant id
            assertThat(testSubjectTwo).isNotEqualTo(testSubjectThree)
                                      .isEqualTo(testSubjectFour)
                                      .isNotEqualTo(testSubjectFive);

            // then - validate test subject three, only matching on tenant id
            assertThat(testSubjectThree).isNotEqualTo(testSubjectFour)
                                        .isEqualTo(testSubjectFive);

            // then - validate test subject four, only matching on tenant id
            assertThat(testSubjectFour).isNotEqualTo(testSubjectFive);
        }
    }

    @Nested
    class HashCode {

        @Test
        void hashOnlyHashesTenantId() {
            // then - validate test subject one, only matching on tenant id
            assertThat(testSubjectOne.hashCode()).isNotEqualTo(testSubjectTwo.hashCode());
            assertThat(testSubjectOne).hasSameHashCodeAs(testSubjectThree);
            assertThat(testSubjectOne.hashCode()).isNotEqualTo(testSubjectFour.hashCode());
            assertThat(testSubjectOne).hasSameHashCodeAs(testSubjectFive);

            // then - validate test subject two, only matching on tenant id
            assertThat(testSubjectTwo.hashCode()).isNotEqualTo(testSubjectThree.hashCode());
            assertThat(testSubjectTwo).hasSameHashCodeAs(testSubjectFour);
            assertThat(testSubjectTwo.hashCode()).isNotEqualTo(testSubjectFive.hashCode());

            // then - validate test subject three, only matching on tenant id
            assertThat(testSubjectThree.hashCode()).isNotEqualTo(testSubjectFour.hashCode());
            assertThat(testSubjectThree).hasSameHashCodeAs(testSubjectFive);

            // then - validate test subject four, only matching on tenant id
            assertThat(testSubjectFour.hashCode()).isNotEqualTo(testSubjectFive.hashCode());
        }
    }

    @Test
    void createWithTenantId() {
        TenantDescriptor tenantDescriptor = TenantDescriptor.tenantWithId("tenant-a");

        assertThat(tenantDescriptor.tenantId()).isEqualTo("tenant-a");
        assertThat(tenantDescriptor.properties()).isEmpty();
    }
}
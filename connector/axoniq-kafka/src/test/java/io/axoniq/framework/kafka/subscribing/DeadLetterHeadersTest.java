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

package io.axoniq.framework.kafka.subscribing;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The names of the dead-letter headers are a contract with the consumers of dead-letter topics, so these tests pin
 * them to their literal values.
 *
 * @author Allard Buijze
 */
class DeadLetterHeadersTest {

    @Test
    void headerNamesMatchTheDocumentedNames() {
        // when / then
        assertThat(DeadLetterHeaders.EXCEPTION_CLASS).isEqualTo("axon-dlt-exception-class");
        assertThat(DeadLetterHeaders.EXCEPTION_MESSAGE).isEqualTo("axon-dlt-exception-message");
        assertThat(DeadLetterHeaders.ORIGINAL_TOPIC).isEqualTo("axon-dlt-original-topic");
        assertThat(DeadLetterHeaders.ORIGINAL_PARTITION).isEqualTo("axon-dlt-original-partition");
        assertThat(DeadLetterHeaders.ORIGINAL_OFFSET).isEqualTo("axon-dlt-original-offset");
    }
}

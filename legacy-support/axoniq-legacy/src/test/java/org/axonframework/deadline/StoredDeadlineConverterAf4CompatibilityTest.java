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

package org.axonframework.deadline;

import org.axonframework.deadline.AxonFramework4.Flavor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@link StoredDeadlineConverter} stores a {@code String} or {@code byte[]} payload
 * exactly as Axon Framework 4.13.2's serializers stored it, for each serializer an Axon Framework 4 application could
 * have used.
 *
 * @author Jakob Hatzl
 */
class StoredDeadlineConverterAf4CompatibilityTest {

    private static AxonFramework4 axonFramework4;

    @BeforeAll
    static void openAxonFramework4() throws IOException {
        axonFramework4 = new AxonFramework4();
    }

    @AfterAll
    static void closeAxonFramework4() throws IOException {
        axonFramework4.close();
    }

    @ParameterizedTest
    @MethodSource("org.axonframework.deadline.AxonFramework4#flavorsAndRawPayloads")
    void aStringOrBytesPayloadIsStoredAsAxonFramework4StoredIt(Flavor flavor, Object payload) {
        // given
        Object axonFramework4Stored =
                axonFramework4.serialize(flavor.axonFramework4Serializer(axonFramework4), payload, String.class);
        StoredDeadlineConverter testSubject = new StoredDeadlineConverter(flavor.axonFramework5Converter());

        // when
        String stored = testSubject.payloadToStored(payload, String.class);

        // then
        assertThat(stored).isEqualTo(axonFramework4Stored);
    }
}

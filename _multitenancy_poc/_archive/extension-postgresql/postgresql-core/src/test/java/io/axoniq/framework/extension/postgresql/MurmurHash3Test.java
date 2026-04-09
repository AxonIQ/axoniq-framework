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

package io.axoniq.framework.extension.postgresql;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@code MurmurHash3}.
 *
 * @author John Hendrikx
 */
public class MurmurHash3Test {

    @ParameterizedTest(name = "\"{0}\" -> {1}")
    @CsvSource({
        " '', 0 ",
        " 'a', 1009084850 ",
        " 'abc', 3017643002 ",
        " 'message digest', 1670332777 ",
        " 'abcdefghijklmnopqrstuvwxyz', 2739798893 ",
        " 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789', 2725966747 ",
        " '12345678901234567890123456789012345678901234567890123456789012345678901234567890', 4175450759 "
    })
    void verifyKnownHashes(String input, long expected) {
        byte[] data = input.getBytes(StandardCharsets.UTF_8);
        int hash = MurmurHash3.hash32(data);

        assertEquals((int)expected, hash, "Hash mismatch for input: \"" + input + "\"");
    }
}

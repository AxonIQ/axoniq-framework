/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.postgresql;

import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for {@code MurmurHash3}.
 *
 * @author John Hendrikx
 */
class MurmurHash3Test {

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

        assertThat(hash).as("Hash mismatch for input: %s", input).isEqualTo((int)expected);
    }
}

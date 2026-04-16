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

package io.axoniq.framework.postgresql;

/**
 * MurmurHash3 x86 32-bit implementation in Java
 * <p>
 * This is a Java implementation of the MurmurHash3 algorithm originally
 * created by Austin Appleby (https://github.com/aappleby/smhasher).
 *
 * @author John Hendrikx
 * @since 1.0.0
 */
final class MurmurHash3 {

    /**
     * Calculate the hash using the given bytes.
     *
     * @return the hash
     */
    static int hash32(byte[] bytes) {
        int h1 = 0;
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;
        int len = bytes.length;
        int roundedEnd = (len & 0xfffffffc);  // round down to 4-byte block

        /*
         * Hash per group of 4 bytes:
         */

        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = ((bytes[i] & 0xff)) |
                     ((bytes[i + 1] & 0xff) << 8) |
                     ((bytes[i + 2] & 0xff) << 16) |
                     ((bytes[i + 3] & 0xff) << 24);

            k1 *= c1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        /*
         * Add remaining bytes (tail):
         */

        int k1 = 0;
        int tailStart = roundedEnd;

        switch (len & 0x03) {  // fall-throughs intended
            case 3:
                k1 ^= (bytes[tailStart + 2] & 0xff) << 16;
            case 2:
                k1 ^= (bytes[tailStart + 1] & 0xff) << 8;
            case 1:
                k1 ^= (bytes[tailStart] & 0xff);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        }

        /*
         * Finalize hash:
         */

        h1 ^= len;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);

        return h1;
    }

    private MurmurHash3() {
        // do not instantiate
    }
}

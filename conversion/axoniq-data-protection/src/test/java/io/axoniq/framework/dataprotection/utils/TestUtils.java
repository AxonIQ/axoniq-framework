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
package io.axoniq.framework.dataprotection.utils;

import io.axoniq.license.entitlement.EnforcingEntitlementManager;
import io.axoniq.license.entitlement.EntitlementManager;
import org.axonframework.conversion.Converter;
import org.mockito.*;
import org.axonframework.conversion.jackson2.Jackson2Converter;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Given the nature of our protobuf format, encrypted fields will always start with these 3 bytes:
 * 8  ->   indicating that the next value is field 1 of type varint
 * 1  ->   indicating the value 1, the version
 * 21 ->   indicating that the next value is field 2 and is fixed64
 *
 * OR, when its a storage field:
 * 8  ->   indicating that the next value is field 1 of type varint
 * 1  ->   indicating the value 1, the version
 * 22 ->   indicating that the next value is field 2 and is fixed64
 *
 *
 * In Base64, this translates to the 4 characters "CAEV" or "CAES"
 *
 * We can exploit this in our unit tests to have an easy way to detect encryption (albeit not with 100,0%
 * certainty.) This utils class provides methods doing just that.
 */
public abstract class TestUtils {

    private TestUtils() {
        throw new Error("non-instantiable utils class");
    }

    public static final String ENCRYPTED_STRING_START = "CAEV";
    public static final byte[] ENCRYPTED_BYTEARRAYSTART = new byte[] {8, 1, 21};
    public static final String ENCRYPTED_SERIALIZED_STRING_START = "CAES";
    public static final byte[] ENCRYPTED_SERIALIZED_BYTEARRAYSTART = new byte[] {8, 1, 22};

    public static boolean isEncrypted(String value) {
        return value.startsWith(ENCRYPTED_STRING_START);
    }

    public static boolean isEncrypted(byte[] value) {
        if(value.length < ENCRYPTED_BYTEARRAYSTART.length) return false;
        for(int i = 0; i < ENCRYPTED_BYTEARRAYSTART.length; i++) {
            if(value[i] != ENCRYPTED_BYTEARRAYSTART[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean isSerializedEncrypted(String value) {
        return value.startsWith(ENCRYPTED_SERIALIZED_STRING_START);
    }

    public static boolean isSerializedEncrypted(byte[] value) {
        if(value.length < ENCRYPTED_SERIALIZED_BYTEARRAYSTART.length) return false;
        for(int i = 0; i < ENCRYPTED_SERIALIZED_BYTEARRAYSTART.length; i++) {
            if(value[i] != ENCRYPTED_SERIALIZED_BYTEARRAYSTART[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean isClear(byte[] value) {
        return value != null && value.length != 0 && !isEncrypted(value);
    }

    public static boolean isClear(String value) {
        return value != null && value.length() != 0 && !isEncrypted(value);
    }

    public static <T> ArrayList<T> list(T... x) {
        return new ArrayList<T>(Arrays.asList(x));
    }

    /**
     * Returns default test Converter using Jackson 3 (tools.jackson).
     *
     * @return JacksonConverter instance
     */
    public static Converter defaultTestConverter() {
        return new Jackson2Converter();
    }

    /**
     * Creates a mocked EntitlementManager for testing purposes.
     * <p>
     * This method creates a Mockito mock of {@link EnforcingEntitlementManager}.
     * Since {@link EntitlementManager} is a sealed interface, we mock the concrete
     * implementation class instead.
     *
     * @return a mocked EntitlementManager suitable for testing
     */
    public static EntitlementManager mockEntitlementManager() {
        return Mockito.mock(EnforcingEntitlementManager.class);
    }
}

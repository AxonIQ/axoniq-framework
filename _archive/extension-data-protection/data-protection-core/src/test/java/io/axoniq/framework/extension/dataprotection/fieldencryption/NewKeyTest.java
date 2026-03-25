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
package io.axoniq.framework.extension.dataprotection.fieldencryption;

import io.axoniq.framework.extension.dataprotection.api.DataSubjectId;
import io.axoniq.framework.extension.dataprotection.api.FieldEncrypter;
import io.axoniq.framework.extension.dataprotection.api.PersonalData;
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.extension.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.extension.dataprotection.utils.TestUtils;
import lombok.Data;
import org.junit.jupiter.api.*;

import static io.axoniq.framework.extension.dataprotection.utils.TestUtils.defaultTestConverter;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests of the scenario that a key has changed.
 */
class NewKeyTest {

    private CryptoEngine cryptoEngine;
    private FieldEncrypter fieldEncrypter;

    @BeforeEach
    public void setUp() throws Exception {
        cryptoEngine = new InMemoryCryptoEngine();
        cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());
        fieldEncrypter = new FieldEncrypter(cryptoEngine, defaultTestConverter());
    }

    @Test
    public void smokeTest() {
        Person p = new Person("x", "Test");
        fieldEncrypter.encrypt(p);
        fieldEncrypter.decrypt(p);
        assertEquals("Test", p.getName());
        fieldEncrypter.encrypt(p);
        cryptoEngine.deleteKey("x");
        cryptoEngine.getOrCreateKey("x");
        fieldEncrypter.decrypt(p);
        assertEquals("", p.getName());
    }

    @Data public static class Person {

        @DataSubjectId
        private final String id;
        @PersonalData
        private final String name;
    }
}

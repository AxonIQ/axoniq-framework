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

import io.axoniq.framework.extension.dataprotection.api.ConfigurationException;
import io.axoniq.framework.extension.dataprotection.api.DataSubjectId;
import io.axoniq.framework.extension.dataprotection.api.FieldEncrypter;
import io.axoniq.framework.extension.dataprotection.api.PersonalData;
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.extension.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.extension.dataprotection.utils.TestUtils;
import lombok.Data;
import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.axoniq.framework.extension.dataprotection.utils.TestUtils.defaultTestConverter;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests of the scenario that a keyid is preloaded through the FieldEncrypter methods rather than provided through
 * a @DataSubjectId
 */
class PreloadKeyTest {

    private CryptoEngine cryptoEngine;
    private FieldEncrypter fieldEncrypter;

    @BeforeEach
    public void setUp() throws Exception {
        cryptoEngine = new InMemoryCryptoEngine();
        cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());
        fieldEncrypter = new FieldEncrypter(cryptoEngine, defaultTestConverter());
    }

    @Test
    public void expectedExceptionTestEncrypt() {
        assertThrows(ConfigurationException.class, () -> {
            fieldEncrypter.encrypt(new Person("Test"));
        });
    }

    @Test
    public void expectedExceptionTestDecrypt() {
        assertThrows(ConfigurationException.class, () -> {
            fieldEncrypter.decrypt(new Person("Test"));
        });
    }

    @Test
    public void singleExternalKeyTest() {
        Person person = new Person("Test");
        String keyId = "bla";
        fieldEncrypter.encrypt(person, keyId);
        assertTrue(TestUtils.isEncrypted(person.getName()));
        fieldEncrypter.decrypt(person, keyId);
        assertEquals("Test", person.getName());
    }

    @Test
    public void multiExternalKeyTest() {
        Person2 person = new Person2(UUID.randomUUID(),"Test", "kipsaté", "cat");
        String foodKey = "12345";
        String petKey = "67890";
        Map<String, String> keyIds = new HashMap<>();
        keyIds.put("food", foodKey);
        keyIds.put("pet", petKey);

        fieldEncrypter.encrypt(person, keyIds);
        assertTrue(TestUtils.isEncrypted(person.getName()));
        assertTrue(TestUtils.isEncrypted(person.getFavouriteFood()));
        assertTrue(TestUtils.isEncrypted(person.getFavouritePet()));

        cryptoEngine.deleteKey(foodKey);

        fieldEncrypter.decrypt(person, keyIds);
        assertEquals("Test", person.getName());
        assertEquals("", person.getFavouriteFood());
        assertEquals("cat", person.getFavouritePet());
    }

    @Data public static class Person {
        @PersonalData
        private final String name;
    }

    @Data public static class Person2 {
        @DataSubjectId
        private final UUID id;

        @PersonalData
        private final String name;

        @PersonalData(group = "food")
        private final String favouriteFood;

        @PersonalData(group = "pet")
        private final String favouritePet;
    }
}

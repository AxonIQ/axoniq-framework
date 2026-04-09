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
package io.axoniq.framework.dataprotection.conversion;

import io.axoniq.framework.dataprotection.api.DataSubjectId;
import io.axoniq.framework.dataprotection.api.FieldEncryptingConverter;
import io.axoniq.framework.dataprotection.api.PersonalData;
import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.dataprotection.utils.TestUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.axonframework.conversion.Converter;
import org.fluttercode.datafactory.impl.DataFactory;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ThreadLocalRandom;

import static io.axoniq.framework.dataprotection.utils.TestUtils.ENCRYPTED_STRING_START;
import static io.axoniq.framework.dataprotection.utils.TestUtils.defaultTestConverter;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests basic conversion (serialization/deserialization) with field encryption using AF5 Converter API.
 * This is the AF5 equivalent of BasicSerializationTest which used the AF4 Serializer API.
 */
public class BasicConversionTest {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private FieldEncryptingConverter converter;
    private DataFactory dataFactory;

    @BeforeEach
    public void setUp() throws Exception {
        final CryptoEngine cryptoEngine = new InMemoryCryptoEngine();
        cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());
        Converter delegateConverter = defaultTestConverter();
        converter = new FieldEncryptingConverter(cryptoEngine, delegateConverter);
        dataFactory = new DataFactory();
        dataFactory.randomize(ThreadLocalRandom.current().nextInt());
    }

    @Test
    public void conversionMustDoEncryption() {
        Person person = new Person(ThreadLocalRandom.current().nextLong(), dataFactory.getLastName());
        logger.info("Person before conversion: {}", person);
        String originalName = person.name;

        // Convert to byte[] (serialize)
        byte[] converted = converter.convert(person, byte[].class);
        assertNotNull(converted);
        String convertedString = new String(converted);
        logger.info("Converted form: {}", convertedString);

        assertFalse(convertedString.contains(originalName),
                   "Converted output should not contain original name");
        assertTrue(convertedString.contains(ENCRYPTED_STRING_START),
                  "Converted output should contain encrypted data marker");
    }

    @Test
    public void conversionMustBeReversible() {
        Person person = new Person(ThreadLocalRandom.current().nextLong(), dataFactory.getLastName());
        logger.info("Original person: {}", person);

        // Convert to byte[] (serialize)
        byte[] converted = converter.convert(person, byte[].class);

        // Convert back (deserialize)
        Person newPerson = converter.convertBack(converted, Person.class);
        logger.info("Restored person: {}", newPerson);

        assertEquals(person, newPerson,
                    "Person should be equal after conversion roundtrip");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person {

        @DataSubjectId
        private long id;

        @PersonalData
        private String name;
    }
}

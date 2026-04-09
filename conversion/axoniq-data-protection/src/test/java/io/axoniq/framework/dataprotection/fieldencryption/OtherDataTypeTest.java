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
package io.axoniq.framework.dataprotection.fieldencryption;

import io.axoniq.framework.dataprotection.api.DataSubjectId;
import io.axoniq.framework.dataprotection.api.FieldEncrypter;
import io.axoniq.framework.dataprotection.api.SerializedPersonalData;
import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.dataprotection.utils.TestUtils;
import lombok.Data;
import org.fluttercode.datafactory.impl.DataFactory;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.LocalDate;
import java.util.concurrent.ThreadLocalRandom;

import static io.axoniq.framework.dataprotection.utils.TestUtils.defaultTestConverter;
import static org.junit.jupiter.api.Assertions.*;

class OtherDataTypeTest {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private CryptoEngine cryptoEngine;
    private FieldEncrypter fieldEncrypter;
    private DataFactory dataFactory;

    @BeforeEach
    public void setUp() throws Exception {
        cryptoEngine = new InMemoryCryptoEngine();
        cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());
        fieldEncrypter = new FieldEncrypter(cryptoEngine, defaultTestConverter());
        dataFactory = new DataFactory();
        dataFactory.randomize(ThreadLocalRandom.current().nextInt());
    }

    @Test
    public void otherDataTypesMustGetEncryptedInSeparateField() {
        LocalDate date = LocalDate.now();
        PersonEvent evt = new PersonEvent(1, date, 3);
        logger.info("before encryption: {}", evt);
        assertNull(evt.dateOfBirthEncrypted);
        assertNull(evt.nEncrypted);
        fieldEncrypter.encrypt(evt);
        logger.info("after encryption: {}", evt);
        assertNull(evt.dateOfBirth);
        assertNotNull(evt.dateOfBirthEncrypted);
        assertEquals(0, evt.n);
        assertNotNull(evt.nEncrypted);
    }

    @Test
    public void otherDataTypeMustBeReplaceable() {
        LocalDate date = LocalDate.now();
        int n = 3;
        PersonEvent evt = new PersonEvent(1, date, n);
        fieldEncrypter.encrypt(evt);
        fieldEncrypter.decrypt(evt);
        assertEquals(date, evt.dateOfBirth);
        assertNull(evt.dateOfBirthEncrypted);
        assertEquals((long)n, (long)evt.n);
        assertNull(evt.nEncrypted);
    }


    @Test
    public void nullMustBeHandledCorrectly() {
        int n = 3;
        PersonEvent evt = new PersonEvent(1, null, n);
        fieldEncrypter.encrypt(evt);
        assertNull(evt.dateOfBirth);
        assertNull(evt.dateOfBirthEncrypted);
        fieldEncrypter.decrypt(evt);
        assertNull(evt.dateOfBirth);
        assertNull(evt.dateOfBirthEncrypted);
    }

    @Data
    public static class PersonEvent {
        @DataSubjectId
        private final int id;

        @SerializedPersonalData
        private final LocalDate dateOfBirth;
        private byte[] dateOfBirthEncrypted;

        @SerializedPersonalData
        private final int n;
        private String nEncrypted;

    }

}

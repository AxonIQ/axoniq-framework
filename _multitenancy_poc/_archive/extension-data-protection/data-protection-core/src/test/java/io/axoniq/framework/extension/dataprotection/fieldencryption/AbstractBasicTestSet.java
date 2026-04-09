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

import io.axoniq.framework.extension.dataprotection.api.FieldEncrypter;
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.extension.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.extension.dataprotection.utils.TestUtils;
import org.fluttercode.datafactory.impl.DataFactory;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static io.axoniq.framework.extension.dataprotection.utils.TestUtils.defaultTestConverter;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Contains a few elementary tests of the FieldEncrypter, encrypting byte[] and String fields in a single class.
 * This is implemented an abstract test class, allowing us to run these tests against vanilla Java, Lombok
 * and Kotlin classes.
 */
public abstract class AbstractBasicTestSet<T> {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private CryptoEngine cryptoEngine;
    private FieldEncrypter fieldEncrypter;
    private DataFactory dataFactory;

    public abstract T createEvent(UUID id, String name, byte[] picture, String city);
    public abstract UUID getId(T event);
    public abstract String getName(T event);
    public abstract byte[] getPicture(T event);
    public abstract String getCity(T event);

    @BeforeEach
    public void setUp() throws Exception {
        cryptoEngine = new InMemoryCryptoEngine();
        cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());
        fieldEncrypter = new FieldEncrypter(cryptoEngine, defaultTestConverter());
        dataFactory = new DataFactory();
        dataFactory.randomize(ThreadLocalRandom.current().nextInt());
    }

    @Test
    public void personalDataFieldsMustGetEncrypted() {
        UUID idOriginal = UUID.randomUUID();
        String nameOriginal = dataFactory.getLastName();
        byte[] pictureOriginal = dataFactory.getRandomChars(20).getBytes(StandardCharsets.UTF_8);
        String cityOriginal = dataFactory.getCity();

        T personRegisteredEvent = createEvent(idOriginal, nameOriginal, pictureOriginal.clone(), cityOriginal);
        logger.info("Before encryption: {}", personRegisteredEvent);
        personRegisteredEvent = fieldEncrypter.encryptAndReturn(personRegisteredEvent);
        logger.info("After encryption: {}", personRegisteredEvent);

        assertEquals(idOriginal, getId(personRegisteredEvent));
        assertNotEquals(nameOriginal, getName(personRegisteredEvent));
        assertNotEquals(pictureOriginal, getPicture(personRegisteredEvent));
        assertEquals(cityOriginal, getCity(personRegisteredEvent));

        assertTrue(TestUtils.isEncrypted(getName(personRegisteredEvent)));
        assertTrue(TestUtils.isEncrypted(getPicture(personRegisteredEvent)));
        assertFalse(TestUtils.isEncrypted(getCity(personRegisteredEvent)));
    }

    @Test
    public void decryptionMustRestoreTheOriginal() {
        UUID idOriginal = UUID.randomUUID();
        String nameOriginal = dataFactory.getLastName();
        byte[] pictureOriginal = dataFactory.getRandomChars(20).getBytes(StandardCharsets.UTF_8);
        String cityOriginal = dataFactory.getCity();

        T personRegisteredEvent1 = createEvent(idOriginal, nameOriginal, pictureOriginal.clone(), cityOriginal);
        T personRegisteredEvent2 = createEvent(idOriginal, nameOriginal, pictureOriginal.clone(), cityOriginal);
        assertEquals(personRegisteredEvent1, personRegisteredEvent2);

        personRegisteredEvent2 = fieldEncrypter.encryptAndReturn(personRegisteredEvent2);
        assertNotEquals(personRegisteredEvent1, personRegisteredEvent2);

        personRegisteredEvent2 = fieldEncrypter.decryptAndReturn(personRegisteredEvent2);
        assertEquals(personRegisteredEvent1, personRegisteredEvent2);
    }

    @Test
    public void keyDeletionMustPreventDecryption() {
        UUID idOriginal = UUID.randomUUID();
        String nameOriginal = dataFactory.getLastName();
        byte[] pictureOriginal = dataFactory.getRandomChars(20).getBytes(StandardCharsets.UTF_8);
        String cityOriginal = dataFactory.getCity();

        T personRegisteredEvent = createEvent(idOriginal, nameOriginal, pictureOriginal.clone(), cityOriginal);
        logger.info("Before encryption: {}", personRegisteredEvent);
        personRegisteredEvent = fieldEncrypter.encryptAndReturn(personRegisteredEvent);
        logger.info("After encryption: {}", personRegisteredEvent);
        cryptoEngine.deleteKey(idOriginal.toString());
        personRegisteredEvent = fieldEncrypter.decryptAndReturn(personRegisteredEvent);
        logger.info("After decryption with deleted key: {}", personRegisteredEvent);

        assertEquals(idOriginal, getId(personRegisteredEvent));
        assertEquals("", getName(personRegisteredEvent));
        assertNull(getPicture(personRegisteredEvent));
        assertEquals(cityOriginal, getCity(personRegisteredEvent));
    }

    @Test
    public void replaceMustDeleteValue() {
        UUID idOriginal = UUID.randomUUID();
        String nameOriginal = dataFactory.getLastName();
        byte[] pictureOriginal = dataFactory.getRandomChars(20).getBytes(StandardCharsets.UTF_8);
        String cityOriginal = dataFactory.getCity();

        T personRegisteredEvent = createEvent(idOriginal, nameOriginal, pictureOriginal.clone(), cityOriginal);
        logger.info("Before replacement: {}", personRegisteredEvent);
        personRegisteredEvent = fieldEncrypter.replaceAndReturn(personRegisteredEvent);
        logger.info("After replacement: {}", personRegisteredEvent);

        assertEquals(idOriginal, getId(personRegisteredEvent));
        assertEquals("", getName(personRegisteredEvent));
        assertNull(getPicture(personRegisteredEvent));
        assertEquals(cityOriginal, getCity(personRegisteredEvent));
    }


    @Test
    public void encryptionMustHaveRandomness() {
        UUID idOriginal = UUID.randomUUID();
        String nameOriginal = dataFactory.getLastName();
        byte[] pictureOriginal = dataFactory.getRandomChars(20).getBytes(StandardCharsets.UTF_8);
        String cityOriginal = dataFactory.getCity();

        T personRegisteredEvent1 = createEvent(idOriginal, nameOriginal, pictureOriginal.clone(), cityOriginal);
        T personRegisteredEvent2 = createEvent(idOriginal, nameOriginal, pictureOriginal.clone(), cityOriginal);
        assertEquals(personRegisteredEvent1, personRegisteredEvent2);

        personRegisteredEvent1 = fieldEncrypter.encryptAndReturn(personRegisteredEvent1);
        personRegisteredEvent2 = fieldEncrypter.encryptAndReturn(personRegisteredEvent2);
        assertNotEquals(personRegisteredEvent1, personRegisteredEvent2);
    }
    
    
}

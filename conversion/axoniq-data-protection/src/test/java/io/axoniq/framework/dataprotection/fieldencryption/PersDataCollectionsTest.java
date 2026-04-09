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
import io.axoniq.framework.dataprotection.api.PersonalData;
import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.dataprotection.utils.TestUtils;
import lombok.Data;
import org.fluttercode.datafactory.impl.DataFactory;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.axoniq.framework.dataprotection.utils.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests of encryption of collection elements containing personal data (without wrapper objects).
 */
class PersDataCollectionsTest {

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
    public void personalDataCollFieldsMustGetEncrypted() {
        A a = new A(3, new ArrayList<>(Arrays.asList(dataFactory.getLastName(), dataFactory.getLastName(), dataFactory.getLastName())));
        logger.info("Before encryption: {}", a);
        fieldEncrypter.encrypt(a);
        logger.info("After encryption: {}", a);
        a.names.forEach(name -> assertTrue(isEncrypted(name)));
        fieldEncrypter.decrypt(a);
        a.names.forEach(name -> assertTrue(isClear(name)));
    }

    @Data public static class A {
        @DataSubjectId private final int id;
        @PersonalData private final List<String> names;
    }


}

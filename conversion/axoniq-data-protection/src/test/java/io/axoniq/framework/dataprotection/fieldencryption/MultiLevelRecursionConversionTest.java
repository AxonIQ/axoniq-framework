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
import io.axoniq.framework.dataprotection.api.DeepPersonalData;
import io.axoniq.framework.dataprotection.api.FieldEncrypter;
import io.axoniq.framework.dataprotection.api.PersonalData;
import io.axoniq.framework.dataprotection.api.SerializedPersonalData;
import io.axoniq.framework.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.dataprotection.utils.TestUtils;
import lombok.Data;
import org.axonframework.conversion.Converter;
import org.fluttercode.datafactory.impl.DataFactory;
import org.junit.jupiter.api.*;

import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static io.axoniq.framework.dataprotection.utils.TestUtils.defaultTestConverter;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests encryption in objects within collections at multiple levels using AF5 Converter API.
 * This is the AF5 equivalent of MultiLevelRecursionTest which used the AF4 Serializer API.
 */
class MultiLevelRecursionConversionTest {

    private FieldEncrypter fieldEncrypter;
    private Converter converter;
    private DataFactory dataFactory;

    @BeforeEach
    public void setUp() throws Exception {
        CryptoEngine cryptoEngine = new InMemoryCryptoEngine();
        cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());
        converter = defaultTestConverter();
        fieldEncrypter = new FieldEncrypter(cryptoEngine, converter);
        dataFactory = new DataFactory();
        dataFactory.randomize(ThreadLocalRandom.current().nextInt());
    }

    @Test
    public void deepInspectionCompletenessTest() {
        A a = randomA();

        // Convert to String before encryption
        String converted1 = converter.convert(a, String.class);
        System.out.println(converted1);
        assertTrue(converted1.contains("@"), "Should contain email before encryption");
        assertTrue(converted1.contains("AAAAAAAAAAAAAA=="),
                         "Should contain fixed byte array before encryption");
        assertTrue(converted1.contains("2017"), "Should contain date before encryption");

        // Encrypt
        fieldEncrypter.encrypt(a);

        // Convert to String after encryption
        String converted2 = converter.convert(a, String.class);
        assertFalse(converted2.contains("@"), "Should NOT contain email after encryption");
        assertFalse(converted2.contains("AAAAAAAAAAAAAA=="),
                          "Should NOT contain fixed byte array after encryption");
        assertFalse(converted2.contains("2017"), "Should NOT contain date after encryption");
        System.out.println(converted2);

        // Decrypt
        fieldEncrypter.decrypt(a);

        // Convert to String after decryption
        String converted3 = converter.convert(a, String.class);
        assertEquals(converted1, converted3, "Should match original after decrypt");
    }

    @Data public static class A {
        @DataSubjectId private final long id;
        @PersonalData private final List<? extends Set<String[]>> b;
        @PersonalData private final List<byte[]> c;
        @DeepPersonalData private final List<? extends Set<B[]>> d;
        @DeepPersonalData private final HashSet<List<B>>[] f;
    }

    @Data public static class B {
        @PersonalData private final String x;
        @PersonalData private final List<String[]> y;
        @SerializedPersonalData private final LocalDate z;
        private byte[] zEncrypted;
    }

    private A randomA() {
        return new A(1L, randomListOfSetOfStringArray(), fixedListOfByteArray(),
                    randomListOfSetOfBArray(), randomArrayOfSetOfBlist());
    }

    private List<Set<String[]>> randomListOfSetOfStringArray() {
        List<Set<String[]>> list = new LinkedList<>();
        for(int i = 0; i < 2; i++) {
            list.add(randomSetOfStringArray());
        }
        return list;
    }

    private List<byte[]> fixedListOfByteArray() {
        List<byte[]> list = new ArrayList<>();
        for(int i = 0; i < 2; i++) {
            byte[] bytearray = new byte[10];
            list.add(bytearray);
        }
        return list;
    }

    private Set<String[]> randomSetOfStringArray() {
        Set<String[]> set = new HashSet<>();
        for(int i = 0; i < 2; i++) {
            set.add(randomStringArray());
        }
        return set;
    }

    private List<Set<B[]>> randomListOfSetOfBArray() {
        List<Set<B[]>> list = new ArrayList<>();
        for(int i = 0; i < 2; i++) {
            list.add(randomSetOfBArray());
        }
        return list;
    }

    private Set<B[]> randomSetOfBArray() {
        Set<B[]> set = new HashSet<>();
        for(int i = 0; i < 2; i++) {
            set.add(randomBArray());
        }
        return set;
    }

    private String[] randomStringArray() {
        String[] sArray = new String[3];
        for(int i = 0; i < sArray.length; i++) {
            sArray[i] = dataFactory.getEmailAddress();
        }
        return sArray;
    }

    private HashSet<List<B>>[] randomArrayOfSetOfBlist() {
        HashSet<List<B>>[] array = (HashSet<List<B>>[])Array.newInstance(HashSet.class, 2);
        for(int i = 0; i < array.length; i++) {
            array[i] = randomSetOfBlist();
        }
        return array;
    }

    private HashSet<List<B>> randomSetOfBlist() {
        HashSet<List<B>> set = new HashSet<>();
        for(int i = 0; i < 2; i++) {
            set.add(randomBList());
        }
        return set;
    }

    private List<B> randomBList() {
        return Arrays.asList(randomBArray());
    }

    private B[] randomBArray() {
        B[] bArray = new B[3];
        for(int i = 0; i < bArray.length; i++)
            bArray[i] = randomB();
        return bArray;
    }

    private B randomB() {
        String[] strings = new String[1];
        strings[0] = dataFactory.getEmailAddress();
        List<String[]> list = new ArrayList<>();
        list.add(strings);
        return new B(dataFactory.getEmailAddress(), list, LocalDate.of(2017, Month.NOVEMBER, 6));
    }
}

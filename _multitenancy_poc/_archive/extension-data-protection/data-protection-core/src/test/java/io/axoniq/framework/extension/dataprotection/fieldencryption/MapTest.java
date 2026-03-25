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
import io.axoniq.framework.extension.dataprotection.api.DeepPersonalData;
import io.axoniq.framework.extension.dataprotection.api.FieldEncrypter;
import io.axoniq.framework.extension.dataprotection.api.PersonalData;
import io.axoniq.framework.extension.dataprotection.api.Scope;
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.extension.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.extension.dataprotection.utils.TestUtils;
import lombok.Data;
import lombok.Value;
import org.fluttercode.datafactory.impl.DataFactory;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static io.axoniq.framework.extension.dataprotection.utils.TestUtils.defaultTestConverter;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests of encryption of maps.
 */
public class MapTest {

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
    public void mapSupportSmokeTest() {
        HashMap<String, String> emails = new HashMap<>();
        emails.put("private", dataFactory.getEmailAddress());
        emails.put("work", dataFactory.getEmailAddress());
        Person person = new Person(1, emails);
        logger.info("Person before encryption: {}", person);
        assertTrue(person.emailAddresses.containsKey("private"));
        assertTrue(person.emailAddresses.containsKey("work"));
        assertTrue(person.emailAddresses.get("private").contains("@"));
        assertTrue(person.emailAddresses.get("work").contains("@"));
        fieldEncrypter.encrypt(person);
        assertTrue(person.emailAddresses.containsKey("private"));
        assertTrue(person.emailAddresses.containsKey("work"));
        assertFalse(person.emailAddresses.get("private").contains("@"));
        assertFalse(person.emailAddresses.get("work").contains("@"));
        logger.info("Person after encryption: {}", person);
        fieldEncrypter.decrypt(person);
        assertTrue(person.emailAddresses.containsKey("private"));
        assertTrue(person.emailAddresses.containsKey("work"));
        assertTrue(person.emailAddresses.get("private").contains("@"));
        assertTrue(person.emailAddresses.get("work").contains("@"));
        logger.info("Person after decryption: {}", person);
    }

    @Data public static class Person {
        @DataSubjectId
        private final int id;
        @PersonalData(scope = Scope.VALUE)
        private final Map<String, String> emailAddresses;
    }

    @Test
    public void personalDataInKeyTest() {
        HashMap<String, String> x = new HashMap<>();
        x.put("test", "test");
        A a = new A(1, x);
        fieldEncrypter.encrypt(a);
        assertFalse(a.x.containsKey("test"));
        assertTrue(a.x.containsValue("test"));
        fieldEncrypter.decrypt(a);
        assertEquals("test", a.x.get("test"));
    }

    @Data public static class A {
        @DataSubjectId
        private final int id;
        @PersonalData(scope = Scope.KEY)
        private final Map<String, String> x;
    }

    @Test
    public void personalDataInBothTest() {
        HashMap<String, String> x = new HashMap<>();
        x.put("test", "test");
        B b = new B(1, x);
        fieldEncrypter.encrypt(b);
        assertFalse(b.x.containsKey("test"));
        assertFalse(b.x.containsValue("test"));
        fieldEncrypter.decrypt(b);
        assertEquals("test", b.x.get("test"));
    }

    @Data public static class B {
        @DataSubjectId
        private final int id;
        @PersonalData(scope = Scope.BOTH)
        private final Map<String, String> x;
    }

    @Test
    public void deepPersonalDataInKeyTest() {
        HashMap<PDO, PDO> x = new HashMap<>();
        x.put(new PDO("test"), new PDO("test"));
        C c = new C(1, x);
        fieldEncrypter.encrypt(c);
        assertFalse(c.x.containsKey(new PDO("test")));
        assertTrue(c.x.containsValue(new PDO("test")));
        fieldEncrypter.decrypt(c);
        assertEquals(new PDO("test"), c.x.get(new PDO("test")));
    }

    @Value public static class PDO {
        @PersonalData String x;
    }

    @Data public static class C {
        @DataSubjectId
        private final int id;
        @DeepPersonalData(scope = Scope.KEY)
        private final Map<PDO, PDO> x;
    }

    @Test
    public void deepPersonalDataInValueTest() {
        HashMap<PDO, PDO> x = new HashMap<>();
        x.put(new PDO("test"), new PDO("test"));
        D d = new D(1, x);
        fieldEncrypter.encrypt(d);
        assertTrue(d.x.containsKey(new PDO("test")));
        assertFalse(d.x.containsValue(new PDO("test")));
        fieldEncrypter.decrypt(d);
        assertEquals(new PDO("test"), d.x.get(new PDO("test")));
    }

    @Data public static class D {
        @DataSubjectId
        private final int id;
        @DeepPersonalData(scope = Scope.VALUE)
        private final Map<PDO, PDO> x;
    }

    @Test
    public void deepPersonalDataInBothTest() {
        HashMap<PDO, PDO> x = new HashMap<>();
        x.put(new PDO("test"), new PDO("test"));
        E e = new E(1, x);
        fieldEncrypter.encrypt(e);
        assertFalse(e.x.containsKey(new PDO("test")));
        assertFalse(e.x.containsValue(new PDO("test")));
        fieldEncrypter.decrypt(e);
        assertEquals(new PDO("test"), e.x.get(new PDO("test")));
    }

    @Data public static class E {
        @DataSubjectId
        private final int id;
        @DeepPersonalData(scope = Scope.BOTH)
        private final Map<PDO, PDO> x;
    }

    @Test
    public void mixImmediateAndDeepPersonalDataTest() {
        HashMap<String, PDO> x = new HashMap<>();
        x.put("test", new PDO("test"));
        F f = new F(1, x);
        fieldEncrypter.encrypt(f);
        assertFalse(f.x.containsKey("test"));
        assertFalse(f.x.containsValue(new PDO("test")));
        fieldEncrypter.decrypt(f);
        assertEquals(new PDO("test"), f.x.get("test"));
    }

    @Data public static class F {
        @DataSubjectId
        private final int id;
        @PersonalData(scope = Scope.KEY)
        @DeepPersonalData(scope = Scope.VALUE)
        private final Map<String, PDO> x;
    }

    @Test
    public void subjInKeyWithDeepData() {
        Map<String, PDO> x = new HashMap<>();
        x.put("key1", new PDO("test"));
        x.put("key2", new PDO("test"));
        G g = new G(1, x);
        fieldEncrypter.encrypt(g);
        cryptoEngine.deleteKey("key1");
        fieldEncrypter.decrypt(g);
        assertEquals("", g.x.get("key1").x);
        assertEquals("test", g.x.get("key2").x);
    }

    @Data public static class G {
        @DataSubjectId
        private final int id;
        @DataSubjectId(scope = Scope.KEY)
        @DeepPersonalData(scope = Scope.VALUE)
        private final Map<String, PDO> x;
    }

    @Test
    public void subjInKeyWithImmediateData() {
        Map<String, String> x = new HashMap<>();
        x.put("key1", "test");
        x.put("key2", "test");
        H h = new H(1, x);
        fieldEncrypter.encrypt(h);
        cryptoEngine.deleteKey("key2");
        fieldEncrypter.decrypt(h);
        assertEquals("test", h.x.get("key1"));
        assertEquals("", h.x.get("key2"));
    }

    @Data public static class H {
        @DataSubjectId
        private final int id;
        @DataSubjectId(scope = Scope.KEY)
        @PersonalData(scope = Scope.VALUE)
        private final Map<String, String> x;
    }

    @Test
    public void ummodifiableMapTest() {
        HashMap<String, String> x = new HashMap<>();
        x.put("test", "test");
        B b = new B(1, Collections.synchronizedMap(Collections.unmodifiableMap(x)));
        fieldEncrypter.encrypt(b);
        assertFalse(b.x.containsKey("test"));
        assertFalse(b.x.containsValue("test"));
        fieldEncrypter.decrypt(b);
        assertEquals("test", b.x.get("test"));
    }

    @Test
    public void emptyMapTest() {
        B b = new B(1, Collections.emptyMap());
        fieldEncrypter.encrypt(b);
        assertTrue(b.x.isEmpty());
        fieldEncrypter.decrypt(b);
        assertTrue(b.x.isEmpty());
    }

    @Test
    public void singletonMapTest() {
        B b = new B(1, Collections.singletonMap("test", "test"));
        fieldEncrypter.encrypt(b);
        assertFalse(b.x.containsKey("test"));
        assertFalse(b.x.containsValue("test"));
        fieldEncrypter.decrypt(b);
        assertEquals("test", b.x.get("test"));
    }
}

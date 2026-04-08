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
import io.axoniq.framework.extension.dataprotection.api.DeepPersonalData;
import io.axoniq.framework.extension.dataprotection.api.FieldEncrypter;
import io.axoniq.framework.extension.dataprotection.api.PersonalData;
import io.axoniq.framework.extension.dataprotection.api.PersonalDataType;
import io.axoniq.framework.extension.dataprotection.api.Scope;
import io.axoniq.framework.extension.dataprotection.api.SerializedPersonalData;
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.extension.dataprotection.cryptoengine.InMemoryCryptoEngine;
import io.axoniq.framework.extension.dataprotection.utils.TestUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.axoniq.framework.extension.dataprotection.utils.TestUtils.defaultTestConverter;
import static org.junit.jupiter.api.Assertions.*;

class TypeAnalysisTests {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private FieldEncrypter fieldEncrypter;

    @BeforeEach
    public void setUp() throws Exception {
        CryptoEngine cryptoEngine = new InMemoryCryptoEngine();
        cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());
        fieldEncrypter = new FieldEncrypter(cryptoEngine, defaultTestConverter());
    }

    @Test
    public void noConfigurationTest() {
        assertFalse(fieldEncrypter.willProcess(new X()));
    }

    @Test
    public void correctConfigurationTests() throws Exception {
        List<Class<?>> classes = new ArrayList<>();
        classes.addAll(Arrays.asList(this.getClass().getClasses()));
        Collections.reverse(classes);
        for(Class<?> clazz : classes) {
            if(clazz.getSimpleName().startsWith("OK")) {
                assertTrue(fieldEncrypter.willProcess(clazz.newInstance()));
                logger.info("Correctly processed {}", clazz.getSimpleName());
            }
        }
    }

    @Test
    public void badConfigurationTests() throws Exception {
        List<Class<?>> classes = new ArrayList<>();
        classes.addAll(Arrays.asList(this.getClass().getClasses()));
        Collections.reverse(classes);
        for(Class<?> clazz : classes) {
            if (clazz.getSimpleName().startsWith("BAD")) {
                try {
                    assertTrue(fieldEncrypter.willProcess(clazz.newInstance()));
                    fail("Didn't receive correct exception for class " + clazz.getSimpleName());
                } catch (ConfigurationException ex) {
                    logger.info("Correct exception for class {}: {}", clazz.getSimpleName(), ex.getMessage());
                }
            }
        }
    }

    // Test class without any Data Protection annotations - should not be processed
    public static class X {
        String x;
    }

    // Valid annotation configurations - these should be accepted by FieldEncrypter
    public static class OK1 {
        @PersonalData String x;
        @PersonalData byte[] y;
    }

    public static class OK2 {
        @PersonalData List<String> x;
    }

    public static class OK3 {
        @PersonalData String[] x;
    }

    @PersonalDataType
    public static class A {
    }

    public static class OK4 {
        @DeepPersonalData List<A> x;
    }

    public static class OK5 {
        @PersonalData(scope = Scope.KEY)
        @DeepPersonalData(scope = Scope.VALUE)
        Map<String, ? extends OK4> x;
    }

    public interface B<X,Y,Z> extends Map<Y,Z> {
    }

    public static class OK6 {
        @DataSubjectId(scope = Scope.KEY)
        @DeepPersonalData(scope = Scope.VALUE)
        B<Object, String, A> b;
    }

    public static class OK7 {
        @PersonalData
        @PersonalData(group = "x")
        String x;
    }

    public static class OK8 {
        @PersonalData(scope = Scope.KEY)
        @PersonalData(scope = Scope.BOTH, group = "x")
        Map<String, String> x;
    }

    // Invalid annotation configurations - these should throw ConfigurationException
    public static class BAD1 {
        @PersonalData LocalDate x;
    }

    public static class BAD2 {
        @DeepPersonalData LocalDate x;
    }

    public static class BAD3 {
        @DataSubjectId String x;
        @DataSubjectId String y;
    }

    public static class BAD4 {
        @PersonalData @DeepPersonalData String x;
    }

    public static class BAD5 {
        @SerializedPersonalData @DeepPersonalData String x;
    }

    public static class BAD6 {
        @PersonalData @DeepPersonalData @PersonalData Map x;
    }

    public static class BAD7 {
        @PersonalData(scope = Scope.VALUE) String x;
    }

    public static class BAD8 {
        @PersonalData Map x;
    }

    public static class BAD9 {
        @SerializedPersonalData LocalDate x;
    }

    public static class BAD10 {
        @SerializedPersonalData LocalDate x;
        LocalDate xEncrypted;
    }

    public static class BAD11 {
        @DataSubjectId(scope = Scope.VALUE) Map x;
    }

    public static class BAD12 {
        @DataSubjectId(scope = Scope.KEY) @PersonalData(scope = Scope.BOTH) Map<String, String> x;
    }

    public static class BAD13 {
        @SerializedPersonalData(storageField = "z") LocalDate x;
        @SerializedPersonalData(storageField = "z") LocalDate y;
        byte[] z;
    }

    public static class BAD14 {
        @PersonalData
        @PersonalData
        String x;
    }

    public static class BAD15 {
        @PersonalData(scope = Scope.KEY)
        @PersonalData(scope = Scope.BOTH)
        Map<String, String> x;
    }

    public static class BAD16 {
        @PersonalData(scope = Scope.VALUE)
        @PersonalData(scope = Scope.VALUE)
        Map<String, String> x;
    }
}

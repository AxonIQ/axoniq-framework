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

package org.axonframework.extension.spring.conversion.avro;

import org.apache.avro.Schema;
import org.axonframework.extension.spring.conversion.avro.test1.ComplexObject;
import org.junit.jupiter.api.*;
import org.springframework.core.io.DefaultResourceLoader;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Finds schemas declared in {@link org.apache.avro.specific.SpecificRecordBase} classes in test packages 1 and 2.
 *
 * @author Simon Zambrovski
 * @author Jan Galinski
 * @since 4.11.0
 */
class SpecificRecordBaseClasspathAvroSchemaLoaderTest {

    private SpecificRecordBaseClasspathAvroSchemaLoader testSubject;

    @Test
    void findsSchemas() {
        DefaultResourceLoader loader = new DefaultResourceLoader();
        testSubject = new SpecificRecordBaseClasspathAvroSchemaLoader(loader);
        List<String> packages = new ArrayList<>();
        packages.add("org.axonframework.extension.spring.conversion.avro.test1");
        packages.add("org.axonframework.extension.spring.conversion.avro.test2");
        packages.add("org.axonframework.extension.spring.conversion.avro.doesntexist");
        List<Schema> schemas = testSubject.load(packages);
        assertEquals(2, schemas.size());

        List<Schema> expected = new ArrayList<>();
        expected.add(ComplexObject.getClassSchema());
        expected.add(org.axonframework.extension.spring.conversion.avro.test2.ComplexObject.getClassSchema());
        assertIterableEquals(schemas, expected);
    }
}

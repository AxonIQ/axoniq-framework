/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package org.axonframework.deadline;

import com.thoughtworks.xstream.XStream;
import org.axonframework.conversion.xstream.XStreamConverter;
import org.axonframework.deadline.AxonFramework4.CompatScope;
import org.axonframework.deadline.AxonFramework4.Flavor;
import org.axonframework.messaging.ScopeDescriptor;
import org.axonframework.modelling.saga.SagaScopeDescriptor;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

import java.io.IOException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class validating that the {@link StoredDeadlineConverter} stores a {@code String} or {@code byte[]} payload
 * exactly as Axon Framework 4.13.2's serializers stored it, for each serializer an Axon Framework 4 application could
 * have used, and reads the type names Axon Framework 4's {@code XStreamSerializer} stored under XStream aliases.
 *
 * @author Jakob Hatzl
 */
class StoredDeadlineConverterAf4CompatibilityTest {

    private static AxonFramework4 axonFramework4;

    @BeforeAll
    static void openAxonFramework4() throws IOException {
        axonFramework4 = new AxonFramework4();
    }

    @AfterAll
    static void closeAxonFramework4() throws IOException {
        axonFramework4.close();
    }

    @ParameterizedTest
    @MethodSource("org.axonframework.deadline.AxonFramework4#flavorsAndRawPayloads")
    void aStringOrBytesPayloadIsStoredAsAxonFramework4StoredIt(Flavor flavor, Object payload) {
        // given
        Object axonFramework4Stored =
                axonFramework4.serialize(flavor.axonFramework4Serializer(axonFramework4), payload, String.class);
        StoredDeadlineConverter testSubject = new StoredDeadlineConverter(flavor.axonFramework5Converter());

        // when
        String stored = testSubject.payloadToStored(payload, String.class);

        // then
        assertThat(stored).isEqualTo(axonFramework4Stored);
    }

    /**
     * Axon Framework 4's {@code XStreamSerializer} stored the type name XStream's mapper gives a class, which is the
     * built-in alias {@code uuid} for a {@code UUID}.
     */
    @Test
    @SuppressWarnings("removal")
    void aUuidPayloadAxonFramework4StoredUnderXStreamsAliasIsReadAsAUuid() {
        // given
        UUID payload = UUID.fromString("062833ae-eb22-38f6-9412-4f1837e7e900");
        Object serializer = axonFramework4.xStreamSerializer(new XStream());
        String typeName = storedTypeName(serializer, UUID.class);
        Object stored = axonFramework4.serialize(serializer, payload, String.class);
        StoredDeadlineConverter testSubject = new StoredDeadlineConverter(new XStreamConverter(new XStream()));

        // when
        Object result = testSubject.payload(typeName, null, stored);

        // then
        assertThat(typeName).isEqualTo("uuid");
        assertThat(result).isEqualTo(payload);
    }

    @Test
    @SuppressWarnings("removal")
    void aScopeAxonFramework4StoredUnderAnAliasOfTheApplicationIsReadThroughTheSameAlias() {
        // given
        Object af4Scope = CompatScope.SAGA.axonFramework4(axonFramework4);
        XStream af4XStream = new XStream();
        af4XStream.alias("sagaScope", af4Scope.getClass());
        Object serializer = axonFramework4.xStreamSerializer(af4XStream);
        String typeName = storedTypeName(serializer, af4Scope.getClass());
        Object stored = axonFramework4.serialize(serializer, af4Scope, String.class);
        XStream af5XStream = new XStream();
        af5XStream.alias("sagaScope", SagaScopeDescriptor.class);
        StoredDeadlineConverter testSubject = new StoredDeadlineConverter(new XStreamConverter(af5XStream));

        // when
        ScopeDescriptor result = testSubject.scope(typeName, stored);

        // then
        assertThat(typeName).isEqualTo("sagaScope");
        assertThat(result).isEqualTo(CompatScope.SAGA.axonFramework5());
    }

    /**
     * Returns the type name the given Axon Framework 4 serializer stores next to an object of the given type.
     */
    private static String storedTypeName(Object serializer, Class<?> type) {
        return (String) axonFramework4.call(axonFramework4.call(serializer, "typeForClass", type), "getName");
    }
}

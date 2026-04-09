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
package io.axoniq.framework.extension.dataprotection.api;

import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import lombok.Getter;
import lombok.Setter;
import org.axonframework.common.infra.ComponentDescriptor;
import org.axonframework.conversion.Converter;
import org.jspecify.annotations.Nullable;

import java.lang.reflect.Type;

/**
 * Implementation of field-level encryption that wraps a delegate {@link Converter}, performing encryption before
 * converting and decryption after converting, using a {@link FieldEncrypter}.
 * <p>
 * This is the primary implementation for Axon Framework 5.x applications using the Converter API. The delegate
 * {@link Converter} must support <code>byte[]</code> conversions.
 * <p>
 * This class implements {@link Converter} and can be used as a drop-in replacement for standard converters in Axon
 * Framework 5.x applications.
 *
 * @author Frans van Buul
 */
public class FieldEncryptingConverter implements Converter {

    private final FieldEncrypter objectFieldEncrypter;
    private final Converter delegateConverter;

    /**
     * Property that determines whether or not the converter should create a clone first before invoking the
     * {@link FieldEncrypter} on the object. By default, this is <code>false</code>. Setting this to <code>true</code>
     * ensures that {@link FieldEncrypter} doesn't modify the original object, but introduces a performance penalty.
     *
     * @param skipCloning whether to skip cloning before encryption
     * @return the current value of skipCloning
     */
    @Getter @Setter
    private boolean skipCloning = false;

    // ===== Factory methods =====

    /**
     * Creates a {@link FieldEncryptingConverter} using a {@link CryptoEngine} and delegate {@link Converter}. A
     * {@link FieldEncrypter} will be constructed using the {@link CryptoEngine}, using the delegate converter also as
     * the converter for the {@link FieldEncrypter}. The {@link FieldEncrypter} will use the default
     * {@link ReplacementValueProvider}.
     * <p>
     * This is the recommended factory method for Axon Framework 5.x applications.
     *
     * @param cryptoEngine      the {@link CryptoEngine} to be used
     * @param delegateConverter the {@link Converter} to wrap
     * @return a new FieldEncryptingConverter instance
     */
    public static FieldEncryptingConverter create(CryptoEngine cryptoEngine, Converter delegateConverter) {
        return new FieldEncryptingConverter(cryptoEngine, delegateConverter);
    }

    /**
     * Creates a {@link FieldEncryptingConverter} using a {@link CryptoEngine}, {@link ReplacementValueProvider}, and
     * delegate {@link Converter}.
     *
     * @param cryptoEngine             the {@link CryptoEngine} to be used
     * @param replacementValueProvider the {@link ReplacementValueProvider} to use
     * @param delegateConverter        the {@link Converter} to wrap
     * @return a new FieldEncryptingConverter instance
     */
    public static FieldEncryptingConverter create(CryptoEngine cryptoEngine,
                                                  ReplacementValueProvider replacementValueProvider,
                                                  Converter delegateConverter) {
        return new FieldEncryptingConverter(cryptoEngine, replacementValueProvider, delegateConverter);
    }

    // ===== Constructors =====

    /**
     * Constructs a {@link FieldEncryptingConverter} for a given {@link FieldEncrypter} and {@link Converter}. This
     * version of the constructor gives maximum control over the {@link FieldEncrypter}.
     *
     * @param fieldEncrypter    the {@link FieldEncrypter} to be used
     * @param delegateConverter the {@link Converter} to wrap
     */
    public FieldEncryptingConverter(FieldEncrypter fieldEncrypter, Converter delegateConverter) {
        this.objectFieldEncrypter = fieldEncrypter;
        this.delegateConverter = delegateConverter;
    }

    /**
     * Constructs a {@link FieldEncryptingConverter} using a given {@link CryptoEngine} and {@link Converter}. A
     * {@link FieldEncrypter} will be constructed using the {@link CryptoEngine} and using the delegate converter also
     * as the converter for the {@link FieldEncrypter}. The {@link FieldEncrypter} will use the default
     * {@link ReplacementValueProvider}.
     *
     * @param cryptoEngine      the {@link CryptoEngine} to be used
     * @param delegateConverter the {@link Converter} to wrap
     */
    public FieldEncryptingConverter(CryptoEngine cryptoEngine, Converter delegateConverter) {
        this(new FieldEncrypter(cryptoEngine, delegateConverter), delegateConverter);
    }

    /**
     * Constructs a {@link FieldEncryptingConverter} using a given {@link CryptoEngine},
     * {@link ReplacementValueProvider} and {@link Converter}.
     *
     * @param cryptoEngine             the {@link CryptoEngine} to be used
     * @param replacementValueProvider the {@link ReplacementValueProvider} to use
     * @param delegateConverter        the {@link Converter} to wrap
     */
    public FieldEncryptingConverter(CryptoEngine cryptoEngine,
                                    ReplacementValueProvider replacementValueProvider,
                                    Converter delegateConverter) {
        this(new FieldEncrypter(cryptoEngine, delegateConverter, replacementValueProvider), delegateConverter);
    }

    /**
     * Constructs a {@link FieldEncryptingConverter} using a given {@link CryptoEngine} and two {@link Converter}s. A
     * {@link FieldEncrypter} will be constructed using the {@link CryptoEngine} and personalDataConverter as the
     * converter for the {@link FieldEncrypter}.
     *
     * @param cryptoEngine          the {@link CryptoEngine} to be used
     * @param personalDataConverter the {@link Converter} to be used internally for personal data conversion
     * @param delegateConverter     the {@link Converter} to wrap
     */
    public FieldEncryptingConverter(CryptoEngine cryptoEngine,
                                    Converter personalDataConverter,
                                    Converter delegateConverter) {
        this(new FieldEncrypter(cryptoEngine, personalDataConverter), delegateConverter);
    }

    // ===== Core Methods =====

    /**
     * Clones the object. Will be invoked prior to encryption if <code>skipCloning</code> is <code>false</code>.
     *
     * <p>IMPORTANT: This method must handle cloning carefully to avoid issues with Axon Framework's
     * immutable objects like Metadata, which cannot be deserialized by Jackson (they throw
     * UnsupportedOperationException when Jackson tries to call put() during deserialization).</p>
     *
     * <p>The default implementation checks if the object is a Java record. Records are immutable,
     * so cloning is unnecessary - the FieldEncrypter will create a new instance with encrypted fields. For non-record
     * objects (mutable POJOs), this method attempts serialization-based cloning.</p>
     *
     * @param object the object to clone
     * @return the cloned object, or the same object if cloning is not needed (e.g., for records)
     */
    @Nullable
    protected Object clone(Object object) {
        // For Java records, skip cloning as they are immutable and FieldEncrypter creates new instances
        if (object.getClass().isRecord()) {
            return object;
        }

        // For mutable POJOs, attempt traditional clone via serialization roundtrip
        try {
            byte[] bytes = delegateConverter.convert(object, byte[].class);
            return delegateConverter.convert(bytes, object.getClass());
        } catch (Exception e) {
            // If cloning fails (e.g., due to Metadata), fall back to returning the original object.
            // This may cause mutation of the original for mutable POJOs, but prevents runtime failures.
            return object;
        }
    }

    /**
     * Converts the given {@code object} to the expected target type.
     * <p>
     * This method intelligently handles both serialization and deserialization:
     * <ul>
     *   <li><strong>Deserialization (byte[] → Object):</strong> Converts first, then decrypts @PersonalData fields</li>
     *   <li><strong>Serialization (Object → byte[]):</strong> Encrypts @PersonalData fields first, then converts</li>
     * </ul>
     * <p>
     * This dual behavior ensures that:
     * - Events stored in the event store have encrypted @PersonalData fields
     * - Events loaded from the event store are automatically decrypted
     * - Event handlers receive decrypted events with plain text @PersonalData fields
     *
     * @param object     The object to convert
     * @param targetType The expected target type
     * @param <T>        The expected target type
     * @return the converted object
     */
    @Override
    @Nullable
    public <T> T convert(@Nullable Object object, Type targetType) {
        // DESERIALIZATION: byte[] -> Object (decrypt after conversion)
        // This happens when loading events from the event store
        if (object instanceof byte[]) {
            T result = delegateConverter.convert(object, targetType);
            if (result != null) {
                // Use decryptAndReturn for immutable objects (records)
                result = objectFieldEncrypter.decryptAndReturn(result);
            }
            return result;
        }

        // SERIALIZATION: Object -> byte[] (encrypt before conversion)
        // This happens when storing events to the event store
        if (!skipCloning && objectFieldEncrypter.willProcess(object)) {
            object = clone(object);
        }
        // Use encryptAndReturn for immutable objects (records)
        object = objectFieldEncrypter.encryptAndReturn(object);
        return delegateConverter.convert(object, targetType);
    }

    /**
     * Convenience method for converting to a Class type.
     *
     * @param object                 The object to convert
     * @param expectedRepresentation The expected target class
     * @param <T>                    The expected target type
     * @return the converted object
     */
    public <T> T convert(Object object, Class<T> expectedRepresentation) {
        return convert(object, (Type) expectedRepresentation);
    }

    /**
     * Converts from a serialized byte array back to an object, performing decryption after conversion.
     *
     * @param bytes      The serialized bytes
     * @param targetType The target type to deserialize to
     * @param <T>        The target type
     * @return the deserialized and decrypted object
     */
    public <T> T convertBack(byte[] bytes, Class<T> targetType) {
        T result = delegateConverter.convert(bytes, targetType);
        if (result != null) {
            objectFieldEncrypter.decrypt(result);
        }
        return result;
    }

    /**
     * Returns the underlying delegate converter.
     *
     * @return the delegate converter
     */
    public Converter getDelegateConverter() {
        return delegateConverter;
    }

    /**
     * Returns the field encrypter used by this converter.
     *
     * @return the field encrypter
     */
    public FieldEncrypter getFieldEncrypter() {
        return objectFieldEncrypter;
    }

    /**
     * Describes this component to the given descriptor.
     *
     * @param descriptor the descriptor to describe this component to
     */
    @Override
    public void describeTo(ComponentDescriptor descriptor) {
        // Delegate to the underlying converter if it's describable
        if (delegateConverter != null) {
            delegateConverter.describeTo(descriptor);
        }
    }
}

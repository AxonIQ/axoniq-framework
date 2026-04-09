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
package io.axoniq.framework.dataprotection.internal.encryption.spd;

import com.google.common.base.Defaults;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.framework.dataprotection.api.ReplacementValueProvider;
import io.axoniq.framework.dataprotection.internal.encryption.core.ByteArrayEncrypter;
import io.axoniq.framework.dataprotection.internal.encryption.core.Encrypter;
import io.axoniq.framework.dataprotection.internal.encryption.core.Operation;
import io.axoniq.framework.extension.dataprotection.internal.messages.SerializedEncryptedFieldData;
import io.axoniq.framework.dataprotection.internal.model.PDField;
import io.axoniq.framework.dataprotection.internal.utils.ExceptionFactory;
import org.axonframework.conversion.Converter;

import java.util.Base64;
import java.util.Optional;
import javax.crypto.SecretKey;

/**
 * Converter-based implementation for encrypting and decrypting field values.
 * <p>
 * Uses the {@link Converter} API for object serialization in Axon Framework 5.x.
 *
 * @author Frans van Buul
 */
public class ConvertingEncrypter implements Encrypter<ValuePair> {

    private final ByteArrayEncrypter byteArrayEncrypter;
    private final Converter converter;
    private final ReplacementValueProvider replacementValueProvider;

    /**
     * Constructs a {@code ConvertingEncrypter} with the given dependencies.
     *
     * @param byteArrayEncrypter       the encrypter for byte array encryption/decryption
     * @param converter                the converter for object serialization
     * @param replacementValueProvider the provider for replacement values
     */
    public ConvertingEncrypter(ByteArrayEncrypter byteArrayEncrypter,
                              Converter converter,
                              ReplacementValueProvider replacementValueProvider) {
        this.byteArrayEncrypter = byteArrayEncrypter;
        this.converter = converter;
        this.replacementValueProvider = replacementValueProvider;
    }

    @Override
    public ValuePair process(PDField pdField, ValuePair pair, Optional<SecretKey> key, Operation operation) {
        boolean encrypted = isEncrypted(pair);
        switch(operation) {
            case ENCRYPT:
                if(!encrypted) encrypt(pdField, pair, key.get());
                break;
            case DECRYPT:
                if(encrypted) {
                    if (key.isPresent() && keyMatches(pair, key.get())) {
                        decrypt(pdField, pair, key.get());
                    } else {
                        setAlternativeValue(pdField, pair);
                    }
                }
                break;
            case REPLACE:
                if(encrypted) {
                    setAlternativeValue(pdField, pair);
                } else {
                    replace(pdField, pair);
                }
                break;
            default:
                throw ExceptionFactory.missingOperation();
        }
        return pair;
    }


    private boolean isEncrypted(ValuePair pair) {
        return pair.getStorage() != null;
    }

    private void encrypt(PDField pdField, ValuePair pair, SecretKey key) {
        if(pair.getValue() == null) {
            if (pair.getStorageType().equals(byte[].class)
                    || pair.getStorageType().equals(String.class)) {
                pair.setStorage(null);
            } else {
                throw ExceptionFactory.wrongTypeForStorage(pair.getStorageType());
            }
        } else {
            // NEW: Using Converter instead of Serializer
            byte[] serializedData = converter.convert(pair.getValue(), byte[].class);
            String typeName = pair.getValue().getClass().getName();

            byte[] partialValue = replacementValueProvider.partialValueForStorage(
                    pdField.getClazz(),
                    pdField.getField(),
                    pdField.getField().getGenericType(),
                    pdField.getGroup(),
                    pdField.getReplacement(),
                    pair.getValue());

            byte[] encryptedData = byteArrayEncrypter.encrypt(serializedData, key, partialValue);

            SerializedEncryptedFieldData.Builder builder =
                    SerializedEncryptedFieldData.newBuilder()
                                                .setVersion(1)
                                                .setEncryptedData(ByteString.copyFrom(encryptedData))
                                                .setTypeName(typeName);
            // Note: Type revision is not available with Converter API

            byte[] storageBytes = builder.build().toByteArray();
            pair.setValue(Defaults.defaultValue(pdField.getField().getType()));

            if (pair.getStorageType().equals(byte[].class)) {
                pair.setStorage(storageBytes);
            } else if (pair.getStorageType().equals(String.class)) {
                pair.setStorage(Base64.getEncoder().encodeToString(storageBytes));
            } else {
                throw ExceptionFactory.wrongTypeForStorage(pair.getStorageType());
            }
        }
    }

    private void decrypt(PDField pdField, ValuePair pair, SecretKey key) {
        try {
            byte[] storageBytes;
            if(pair.getStorageType().equals(byte[].class)) {
                storageBytes = (byte[])pair.getStorage();
            } else if(pair.getStorageType().equals(String.class)) {
                storageBytes = Base64.getDecoder().decode((String)pair.getStorage());
            } else {
                throw ExceptionFactory.wrongTypeForStorage(pair.getStorageType());
            }

            SerializedEncryptedFieldData message = SerializedEncryptedFieldData.parseFrom(storageBytes);
            byte[] clearData = byteArrayEncrypter.decrypt(message.getEncryptedData().toByteArray(), key);

            // NEW: Using Converter instead of Serializer
            Class<?> targetType;
            try {
                targetType = Class.forName(message.getTypeName());
            } catch (ClassNotFoundException e) {
                throw ExceptionFactory.unknownSerializedType(message.getTypeName());
            }

            Object deserializedObject = converter.convert(clearData, targetType);

            if(deserializedObject == null) {
                throw ExceptionFactory.unknownSerializedType(message.getTypeName());
            }

            pair.setValue(deserializedObject);
            pair.setStorage(null);
        } catch (InvalidProtocolBufferException ex) {
            throw ExceptionFactory.forInvalidProtobuf(ex);
        }
    }

    private boolean keyMatches(ValuePair pair, SecretKey key) {
        try {
            byte[] storageBytes;
            if(pair.getStorageType().equals(byte[].class)) {
                storageBytes = (byte[])pair.getStorage();
            } else if(pair.getStorageType().equals(String.class)) {
                storageBytes = Base64.getDecoder().decode((String)pair.getStorage());
            } else {
                throw ExceptionFactory.wrongTypeForStorage(pair.getStorageType());
            }
            SerializedEncryptedFieldData message = SerializedEncryptedFieldData.parseFrom(storageBytes);
            return byteArrayEncrypter.keyMatches(message.getEncryptedData().toByteArray(), key);
        } catch (InvalidProtocolBufferException ex) {
            throw ExceptionFactory.forInvalidProtobuf(ex);
        }
    }

    private void setAlternativeValue(PDField pdField, ValuePair pair) {
        try {
            byte[] storageBytes;
            if(pair.getStorageType().equals(byte[].class)) {
                storageBytes = (byte[])pair.getStorage();
            } else if(pair.getStorageType().equals(String.class)) {
                storageBytes = Base64.getDecoder().decode((String)pair.getStorage());
            } else {
                throw ExceptionFactory.wrongTypeForStorage(pair.getStorageType());
            }
            SerializedEncryptedFieldData message = SerializedEncryptedFieldData.parseFrom(storageBytes);
            byte[] partialData = byteArrayEncrypter.getPartialValue(message.getEncryptedData().toByteArray());
            pair.setValue(replacementValueProvider.replacementValue(pdField.getClazz(),
                    pdField.getField(), pdField.getField().getGenericType(),
                    pdField.getGroup(), pdField.getReplacement(), partialData));
            pair.setStorage(null);
        } catch (InvalidProtocolBufferException ex) {
            throw ExceptionFactory.forInvalidProtobuf(ex);
        }
    }

    private void replace(PDField pdField, ValuePair pair) {
        byte[] partialData = replacementValueProvider.partialValueForStorage(pdField.getClazz(), pdField.getField(),
                pdField.getField().getGenericType(), pdField.getGroup(), pdField.getReplacement(), pair.getValue());
        pair.setValue(replacementValueProvider.replacementValue(pdField.getClazz(),
                pdField.getField(), pdField.getField().getGenericType(),
                pdField.getGroup(), pdField.getReplacement(), partialData));
        pair.setStorage(null);
    }
}

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
package io.axoniq.framework.extension.dataprotection.internal.encryption.core;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.axoniq.framework.extension.dataprotection.api.ReplacementValueProvider;
import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.extension.dataprotection.internal.messages.EncryptedFieldData;
import io.axoniq.framework.extension.dataprotection.internal.model.IPDField;
import io.axoniq.framework.extension.dataprotection.internal.model.PDField;
import io.axoniq.framework.extension.dataprotection.internal.utils.ExceptionFactory;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Optional;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;

/**
 * Implementation of {@link Encrypter} for byte array data. Handles encryption, decryption, and replacement operations
 * using AES encryption with CBC mode.
 *
 * @author Frans van Buul
 */
public class ByteArrayEncrypter implements Encrypter<byte[]> {

    private static final ThreadLocal<SecureRandom> secureRandom = new ThreadLocal<SecureRandom>() {
        @Override
        protected SecureRandom initialValue() {
            return new SecureRandom();
        }
    };
    private static final ThreadLocal<MessageDigest> md5 = new ThreadLocal<MessageDigest>() {
        @Override
        protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException ex) {
                throw ExceptionFactory.forNoSuchAlg(ex, "MD5");
            }
        }
    };

    private final CryptoEngine cryptoEngine;
    private final ReplacementValueProvider replacementValueProvider;

    public ByteArrayEncrypter(CryptoEngine cryptoEngine, ReplacementValueProvider replacementValueProvider) {
        this.cryptoEngine = cryptoEngine;
        this.replacementValueProvider = replacementValueProvider;
    }

    public byte[] process(PDField pdField, byte[] input, Optional<SecretKey> key, Operation operation) {
        boolean encrypted = isEncrypted(input);
        switch(operation) {
            case ENCRYPT:
                if(encrypted && !(pdField instanceof IPDField && ((IPDField)pdField).isReencrypt())) return input;
                byte[] newPartialValue = replacementValueProvider.partialValueForStorage(pdField.getClazz(), pdField.getField(),
                        byte[].class, pdField.getGroup(), pdField.getReplacement(), input);
                return encrypt(input, key.get(), newPartialValue);
            case DECRYPT:
                if(!encrypted) return input;
                if(key.isPresent() && keyMatches(input, key.get())) {
                    return decrypt(input, key.get());
                } else {
                    byte[] storedPartialValue = getPartialValue(input);
                    return (byte[])replacementValueProvider.replacementValue(pdField.getClazz(), pdField.getField(),
                            byte[].class, pdField.getGroup(), pdField.getReplacement(), storedPartialValue);
                }
            case REPLACE:
                byte[] partialValue;
                if(encrypted) {
                    partialValue = getPartialValue(input);
                } else {
                    partialValue = replacementValueProvider.partialValueForStorage(pdField.getClazz(), pdField.getField(),
                            byte[].class, pdField.getGroup(), pdField.getReplacement(), input);
                }
                return (byte[])replacementValueProvider.replacementValue(pdField.getClazz(), pdField.getField(),
                        byte[].class, pdField.getGroup(), pdField.getReplacement(), partialValue);
            default: throw ExceptionFactory.missingOperation();
        }
    }

    public byte[] encrypt(byte[] clear, SecretKey key, byte[] partialValue) {
        try {
            /* Choose a version. */
            int version = 1;

            /* Calculate an IV. */
            int preIv = secureRandom.get().nextInt();
            md5.get().reset();
            md5.get().update(Ints.toByteArray(preIv));
            IvParameterSpec iv = new IvParameterSpec(md5.get().digest());

            /* Calculate the encrypted value. */
            Cipher cipher = cryptoEngine.createCipher();
            cipher.init(Cipher.ENCRYPT_MODE, key, iv);
            byte[] encryptedValue = cipher.doFinal(clear);

            /* Calculate the digest. */
            md5.get().reset();
            md5.get().update(Ints.toByteArray(preIv));
            md5.get().update(encryptedValue);
            byte[] digestBytes = md5.get().digest();
            long digest = Longs.fromByteArray(digestBytes);
            Cipher digestCipher = cryptoEngine.createDigestCipher();
            digestCipher.init(Cipher.ENCRYPT_MODE, key);
            long encDigest = Longs.fromByteArray(digestCipher.doFinal(digestBytes));

            /* Assemble and serialize a message. */
            EncryptedFieldData.Builder builder = EncryptedFieldData.newBuilder()
                    .setVersion(version)
                    .setPreIv(preIv)
                    .setEncryptedValue(ByteString.copyFrom(encryptedValue))
                    .setDigest(digest)
                    .setEncDigest(encDigest);
            if(partialValue != null) {
                builder.setPartialValue(ByteString.copyFrom(partialValue));
            }
            return builder.build().toByteArray();
        } catch (InvalidKeyException ex) {
            throw ExceptionFactory.forInvalidKey(ex);
        } catch (InvalidAlgorithmParameterException ex) {
            throw ExceptionFactory.forInvalidIv(ex);
        } catch (IllegalBlockSizeException ex) {
            throw ExceptionFactory.forIllegalBlockSizeEnc(ex);
        } catch (BadPaddingException ex) {
            throw ExceptionFactory.forBadPaddingEnc(ex);
        }
    }

    public byte[] decrypt(byte[] encryptedFieldDataBytes, SecretKey key) {
        try {
            /* Deserialize the message. */
            EncryptedFieldData encryptedFieldData = EncryptedFieldData.parseFrom(encryptedFieldDataBytes);

            /* No need to check the version or digest, since we always call hasBeenProcessed before doing decryption. */

            /* Calculate the actual IV. */
            md5.get().reset();
            md5.get().update(Ints.toByteArray(encryptedFieldData.getPreIv()));
            IvParameterSpec iv = new IvParameterSpec(md5.get().digest());

            /* Calculate the decrypted value. */
            Cipher cipher = cryptoEngine.createCipher();
            cipher.init(Cipher.DECRYPT_MODE, key, iv);
            return cipher.doFinal(encryptedFieldData.getEncryptedValue().toByteArray());
        } catch (InvalidProtocolBufferException ex) {
            throw ExceptionFactory.forInvalidProtobuf(ex);
        } catch (InvalidKeyException ex) {
            throw ExceptionFactory.forInvalidKey(ex);
        } catch (InvalidAlgorithmParameterException ex) {
            throw ExceptionFactory.forInvalidIv(ex);
        } catch (IllegalBlockSizeException ex) {
            throw ExceptionFactory.forIllegalBlockSizeDec(ex);
        } catch (BadPaddingException ex) {
            throw ExceptionFactory.forBadPaddingDec(ex);
        }
    }

    public byte[] getPartialValue(byte[] encryptedFieldDataBytes) {
        try {
            EncryptedFieldData encryptedFieldData = EncryptedFieldData.parseFrom(encryptedFieldDataBytes);
            ByteString partialValue = encryptedFieldData.getPartialValue();
            if(partialValue == null) {
                return null;
            } else {
                return partialValue.toByteArray();
            }
        } catch (InvalidProtocolBufferException ex) {
            throw ExceptionFactory.forInvalidProtobuf(ex);
        }
    }

    public boolean isEncrypted(byte[] encryptedFieldDataBytes) {
        /* Attempt to deserialize the message; if this fails, we consider it not encrypted. */
        EncryptedFieldData encryptedFieldData;
        try {
            encryptedFieldData = EncryptedFieldData.parseFrom(encryptedFieldDataBytes);
        } catch (InvalidProtocolBufferException ex) {
            return false;
        }
        /* If the version is not what we expected, return false. */
        if(encryptedFieldData.getVersion() != 1) {
            return false;
        }
        /* Validate the digest; if this fails, we consider it not encrypted. If this succeeds,
         * we assume the message encrypted. */
        md5.get().reset();
        md5.get().update(Ints.toByteArray(encryptedFieldData.getPreIv()));
        md5.get().update(encryptedFieldData.getEncryptedValue().toByteArray());
        long calculatedDigest = Longs.fromByteArray(md5.get().digest());
        return calculatedDigest == encryptedFieldData.getDigest();
    }

    public boolean keyMatches(byte[] encryptedFieldDataBytes, SecretKey key) {
        try {
            /* Deserialize the message. */
            EncryptedFieldData encryptedFieldData = EncryptedFieldData.parseFrom(encryptedFieldDataBytes);

            /* No need to check the version or digest, since we always call hasBeenProcessed before doing decryption. */

            /* Calculate the expected encrypted digest. */
            md5.get().reset();
            md5.get().update(Ints.toByteArray(encryptedFieldData.getPreIv()));
            md5.get().update(encryptedFieldData.getEncryptedValue().toByteArray());
            byte[] digestBytes = md5.get().digest();
            Cipher digestCipher = cryptoEngine.createDigestCipher();
            digestCipher.init(Cipher.ENCRYPT_MODE, key);
            long encDigest = Longs.fromByteArray(digestCipher.doFinal(digestBytes));
            return encryptedFieldData.getEncDigest() == encDigest;
        } catch (InvalidProtocolBufferException ex) {
            throw ExceptionFactory.forInvalidProtobuf(ex);
        } catch (InvalidKeyException ex) {
            throw ExceptionFactory.forInvalidKey(ex);
        } catch (IllegalBlockSizeException ex) {
            throw ExceptionFactory.forIllegalBlockSizeDec(ex);
        } catch (BadPaddingException ex) {
            throw ExceptionFactory.forBadPaddingDec(ex);
        }
    }


}

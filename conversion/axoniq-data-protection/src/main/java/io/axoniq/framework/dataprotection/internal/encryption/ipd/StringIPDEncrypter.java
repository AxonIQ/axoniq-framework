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
package io.axoniq.framework.dataprotection.internal.encryption.ipd;

import io.axoniq.framework.dataprotection.api.ReplacementValueProvider;
import io.axoniq.framework.dataprotection.internal.encryption.core.ByteArrayEncrypter;
import io.axoniq.framework.dataprotection.internal.encryption.core.Encrypter;
import io.axoniq.framework.dataprotection.internal.encryption.core.Operation;
import io.axoniq.framework.dataprotection.internal.model.IPDField;
import io.axoniq.framework.dataprotection.internal.model.PDField;
import io.axoniq.framework.dataprotection.internal.utils.ExceptionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import javax.crypto.SecretKey;

/**
 * Implementation of {@link Encrypter} for String data. Converts strings to byte arrays for encryption
 * and uses Base64 encoding for the encrypted representation.
 *
 * @author Frans van Buul
 */
class StringIPDEncrypter implements Encrypter<String> {

    private final ByteArrayEncrypter byteArrayEncrypter;
    private final ReplacementValueProvider replacementValueProvider;

    public StringIPDEncrypter(ByteArrayEncrypter byteArrayEncrypter, ReplacementValueProvider replacementValueProvider) {
        this.byteArrayEncrypter = byteArrayEncrypter;
        this.replacementValueProvider = replacementValueProvider;
    }

    public String process(PDField pdField, String input, Optional<SecretKey> key, Operation operation) {
        boolean encrypted = isEncrypted(input);
        switch(operation) {
            case ENCRYPT:
                if(encrypted && !(pdField instanceof IPDField && ((IPDField)pdField).isReencrypt())) return input;
                byte[] newPartialValue = replacementValueProvider.partialValueForStorage(pdField.getClazz(), pdField.getField(),
                        String.class, pdField.getGroup(), pdField.getReplacement(), input);
                return Base64.getEncoder().encodeToString(
                    byteArrayEncrypter.encrypt(input.getBytes(StandardCharsets.UTF_8), key.get(), newPartialValue)
                );
            case DECRYPT:
                if(!encrypted) return input;
                byte[] inputBytes = Base64.getDecoder().decode(input);
                if(key.isPresent() && byteArrayEncrypter.keyMatches(inputBytes, key.get())) {
                    return new String(byteArrayEncrypter.decrypt(inputBytes, key.get()), StandardCharsets.UTF_8);
                } else {
                    return (String)replacementValueProvider.replacementValue(pdField.getClazz(), pdField.getField(),
                            String.class, pdField.getGroup(), pdField.getReplacement(), byteArrayEncrypter.getPartialValue(Base64.getDecoder().decode(input)));
                }
            case REPLACE:
                byte[] partialValue;
                if(encrypted) {
                    partialValue = byteArrayEncrypter.getPartialValue(Base64.getDecoder().decode(input));
                } else {
                    partialValue = replacementValueProvider.partialValueForStorage(pdField.getClazz(), pdField.getField(),
                            String.class, pdField.getGroup(), pdField.getReplacement(), input);
                }
                return (String)replacementValueProvider.replacementValue(pdField.getClazz(), pdField.getField(),
                        String.class, pdField.getGroup(), pdField.getReplacement(), partialValue);
            default:
                throw ExceptionFactory.missingOperation();
        }
    }

    private boolean isEncrypted(String crypto) {
        if(crypto == null) return false;
        byte[] cryptoBytes = null;
        try {
            cryptoBytes = Base64.getDecoder().decode(crypto);
        } catch(IllegalArgumentException ex) {
            return false;
        }
        return byteArrayEncrypter.isEncrypted(cryptoBytes);
    }

}

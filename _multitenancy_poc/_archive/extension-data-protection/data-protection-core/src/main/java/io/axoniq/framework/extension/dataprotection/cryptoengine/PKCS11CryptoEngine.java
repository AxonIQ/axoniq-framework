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
package io.axoniq.framework.extension.dataprotection.cryptoengine;

import io.axoniq.framework.extension.dataprotection.internal.utils.ExceptionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.security.KeyStore;
import java.security.Provider;
import java.security.Security;
import java.util.Arrays;

/**
 * Implementation of {@link CryptoEngine} that uses a PKCS#11 backend, such as a Hardware Security Module (HSM). This
 * implementation uses the SunPKCS11 provider. (from Java 7 to 15)
 *
 * @author Frans van Buul
 */
public class PKCS11CryptoEngine extends JavaKeyStoreCryptoEngine {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String SUN_PKCS11_PROVIDER_NAME = "SunPKCS11";

    /**
     * Constructs a new {@link PKCS11CryptoEngine}
     *
     * @param configName the name of a file holding the config for {SunPKCS11}
     * @param password   the password to open the keystore. Will be overwritten with zeroes after successful opening
     */
    public PKCS11CryptoEngine(String configName, char[] password) {
        this(prepareProvider(configName), password);
    }

    private PKCS11CryptoEngine(Provider pkcs11, char[] password) {
        super(getKeyStore(pkcs11, password));
    }

    private static Provider prepareProvider(String configFile) {
        try {
            return Security.getProvider(SUN_PKCS11_PROVIDER_NAME).configure(configFile);
        } catch (Exception ex) {
            logger.error("Unable to configure Hardware Security Module. {}", ex.getMessage());
            throw new RuntimeException("Unable to configure Hardware Security Module!", ex);
        }
    }

    private static KeyStore getKeyStore(Provider pkcs11, char[] password) {
        if (-1 == Security.addProvider(pkcs11)) {
            throw ExceptionFactory.forUnableToAddProvider(pkcs11);
        }
        try {
            KeyStore keyStore = KeyStore.getInstance("pkcs11", pkcs11);
            keyStore.load(null, password);
            Arrays.fill(password, (char) 0);
            return keyStore;
        } catch (Exception ex) {
            throw ExceptionFactory.forUnableToOpenKeyStore(ex);
        }
    }
}

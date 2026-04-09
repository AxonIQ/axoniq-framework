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
package io.axoniq.framework.dataprotection.cryptoengine;

import io.axoniq.framework.dataprotection.utils.TestUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.invoke.MethodHandles;

/**
 * To make this test work on any given machine, install SoftHSM (https://www.opendnssec.org/softhsm/), initialize a
 * token, and adjust the configuration below accordingly.
 * <p>
 * The test will be ignored if the native lib can't be found.
 */
class PKCS11Test extends AbstractEngineTestSet {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String SOFTHSM_CONFIG_FILE = "/usr/local/etc/softhsm/softhsm2_stef.conf";
    private static final String TOKEN_PIN = "1234";
    private static final CryptoEngine cryptoEngine;

    static {
        if (new File(SOFTHSM_CONFIG_FILE).exists()) {
            cryptoEngine = new PKCS11CryptoEngine(SOFTHSM_CONFIG_FILE, TOKEN_PIN.toCharArray());
            cryptoEngine.registerEntitlementManager(TestUtils.mockEntitlementManager());
        } else {
            logger.warn("SoftHSM native lib not found - all PKCS#11 test will be skipped");
            cryptoEngine = null;
        }
    }

    @BeforeEach
    public void beforeMethod() {
        Assumptions.assumeTrue(cryptoEngine != null);
    }

    @Override
    protected CryptoEngine getCryptoEngine() {
        return cryptoEngine;
    }
}

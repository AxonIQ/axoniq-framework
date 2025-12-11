/*
 * Copyright (c) 2010-2025. AxonIQ B.V.
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
 *    https://lp.axoniq.io/axoniq-software-subscription-agreement-terms
 *
 *
 */
package io.axoniq.framework.dataprotection.internal.licensing;

import io.axoniq.framework.dataprotection.internal.utils.ExceptionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.LocalDate;
import java.util.Properties;

/**
 * Singleton that provides access to information from the license key used by this
 * instance of the Axon Data Protection Module. Invoked a couple of times in the code to make overriding it
 * more difficult.
 *
 * Actual complexity of reading/verifying is delegated to the LicensePropertyReader.
 */
public class LicenseConfiguration {

    private static final Logger log = LoggerFactory.getLogger("io.axoniq.framework.dataprotection");
    private static LicenseConfiguration instance;

    static Clock clock = Clock.systemDefaultZone();

    private final LocalDate expiryDate;
    private final LocalDate graceDate;

    public LicenseConfiguration(LocalDate expiryDate, LocalDate graceDate) {
        this.expiryDate = expiryDate;
        this.graceDate = graceDate;
    }

    public LocalDate getExpiryDate() {
        return expiryDate;
    }

    public LocalDate getGraceDate() {
        return graceDate;
    }

    public static LicenseConfiguration getInstance() {
        if (instance == null) {
            Properties properties = new LicensePropertyReader().readLicenseProperties();
            instance = new LicenseConfiguration(
                    LocalDate.parse(properties.getProperty("expiry_date")),
                    LocalDate.parse(properties.getProperty("grace_date")));
            if (LocalDate.now(clock).isAfter(instance.expiryDate)) {
                if (LocalDate.now(clock).isBefore(instance.graceDate)) {
                    log.warn("License has expired! The Data Protection Module will continue working until {}", instance.graceDate);
                } else {
                    throw ExceptionFactory.licenseExpired(instance.expiryDate);
                }
            }
            if (!"AxonIQ GDPR Module".equals(properties.getProperty("product"))
                    && !"Axon Data Protection".equals(properties.getProperty("product"))) {
                throw ExceptionFactory.licenseWrongProduct();
            }
            for (Object key : properties.keySet()) {
                if ("signature".equals(key)) {
                    continue;
                }
                if ("issue_date".equals(key)) {
                    continue;
                }
                if ("product".equals(key)) {
                    continue;
                }
                Object value = properties.get(key);
                log.info("license " + key + " = " + value);
            }
        }
        return instance;
    }


    /* package private */
    static void reset() {
        instance = null;
    }

}

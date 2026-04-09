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
package io.axoniq.framework.dataprotection.internal.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Utility class for detecting if Scala is present on the classpath.
 *
 * @author Frans van Buul
 */
public class ScalaDetector {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final boolean scalaPresent;

    static {
        boolean scalaPresentTmp;
        try {
            Class.forName("scala.collection.Seq");
            scalaPresentTmp = true;
        } catch(Exception ex) {
            scalaPresentTmp = false;
        }
        scalaPresent = scalaPresentTmp;
        logger.debug("scalaPresent = {}", scalaPresent);
    }

    public static boolean isScalaPresent() {
        return scalaPresent;
    }

}

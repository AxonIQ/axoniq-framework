/*
 * Copyright (c) 2010-2026. AxonIQ B.V.
 *
 * Licensed under the AXONIQ TERMS OF SERVICE,
 * Version 29 April 2026 (the "License");
 *
 * The software is available for evaluation use without registration.
 * Continued use beyond the evaluation period requires registration
 * and a commercial license. See the License for the specific language
 * governing permissions and limitations under the License.
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at:
 *  https://www.axoniq.io/legal/terms-of-service
 *
 * For licensing information and to register, visit:
 *  https://www.axoniq.io/pricing
 */

package gettingstarted.order;

import org.axonframework.messaging.core.unitofwork.ProcessingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// tag::payment-service[]
public class PaymentService {

    static Logger logger = LoggerFactory.getLogger(PaymentService.class);

    public static Map<String, Object> initiatePayment(ProcessingContext pc,
                                                       Map<String, Object> payload) {
        logger.info("Initiating payment for order: {}", payload.get("orderId"));
        return Map.of();
    }
}
// end::payment-service[]

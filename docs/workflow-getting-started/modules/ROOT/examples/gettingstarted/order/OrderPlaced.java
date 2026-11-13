/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package gettingstarted.order;

// tag::order-placed[]
public record OrderPlaced(
    String orderId,    // <1>
    String customerId,
    String email,
    double amount
) {}
// end::order-placed[]

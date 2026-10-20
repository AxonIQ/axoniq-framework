/*
 * Copyright (c) 2010-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package snapshotting.index.recordvariant;

// tag::account-record[]
public record Account(String accountId, int balance) {}
// end::account-record[]

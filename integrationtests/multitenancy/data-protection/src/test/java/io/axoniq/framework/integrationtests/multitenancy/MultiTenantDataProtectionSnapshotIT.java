/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.integrationtests.multitenancy;

import org.axonframework.eventsourcing.snapshot.api.Snapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test proving that snapshots use the data-protection converter of their tenant.
 *
 * @author Jan Galinski
 */
@ExtendWith(DisableMultiTenancyTestsWithoutLicense.class)
class MultiTenantDataProtectionSnapshotIT {

    private final TenantDataProtectionFixture fixture = new TenantDataProtectionFixture();

    @BeforeEach
    void setUp() {
        fixture.start();
    }

    @AfterEach
    void tearDown() {
        fixture.stop();
    }

    @Test
    void usesEachTenantsConverterToStoreAndLoadProtectedSnapshots() {
        var tenantASnapshot = new TenantDataProtectionFixture.CustomerDataSnapshot(
                TenantDataProtectionFixture.CUSTOMER_ID, "alice@tenant-a.example");
        var tenantBSnapshot = new TenantDataProtectionFixture.CustomerDataSnapshot(
                TenantDataProtectionFixture.CUSTOMER_ID, "bob@tenant-b.example");

        Snapshot storedForTenantA = fixture.storeAndLoadSnapshot(TenantDataProtectionFixture.TENANT_A,
                                                                  tenantASnapshot);
        Snapshot storedForTenantB = fixture.storeAndLoadSnapshot(TenantDataProtectionFixture.TENANT_B,
                                                                  tenantBSnapshot);

        assertThat(fixture.rawStoredSnapshotPayload(storedForTenantA))
                .contains(TenantDataProtectionFixture.CUSTOMER_ID)
                .doesNotContain("alice@tenant-a.example");
        assertThat(fixture.rawStoredSnapshotPayload(storedForTenantB))
                .contains(TenantDataProtectionFixture.CUSTOMER_ID)
                .doesNotContain("bob@tenant-b.example");
        assertThat(fixture.tenantConverters().get(TenantDataProtectionFixture.TENANT_A)
                          .convert(storedForTenantA.payload(), TenantDataProtectionFixture.CustomerDataSnapshot.class))
                .isEqualTo(tenantASnapshot);
        assertThat(fixture.tenantConverters().get(TenantDataProtectionFixture.TENANT_B)
                          .convert(storedForTenantB.payload(), TenantDataProtectionFixture.CustomerDataSnapshot.class))
                .isEqualTo(tenantBSnapshot);
    }
}

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

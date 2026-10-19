/*
 * Copyright (c) 2022-2026. Axoniq B.V.
 *
 * Licensed under the Axoniq Terms of Service (the "Terms"). You may not
 * use this file except in compliance with the Terms, available at
 * https://www.axoniq.io/legal/terms-of-service
 */

package io.axoniq.framework.messaging.multitenancy.axonserver.api;

import io.axoniq.axonserver.grpc.admin.ContextOverview;
import io.axoniq.axonserver.grpc.admin.ReplicationGroupOverview;
import io.axoniq.framework.messaging.multitenancy.api.TenantDescriptor;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AxonServerTenantUtilsTest {

    @Nested
    class TenantDescriptorExtraction {

        @Test
        void usesContextNameAsTenantId() {
            // given
            ContextOverview contextOverview = contextOverview("tenant-a", "default-rg");

            // when
            TenantDescriptor result = AxonServerTenantUtils.tenantDescriptor(contextOverview);

            // then
            assertThat(result.tenantId()).isEqualTo("tenant-a");
        }

        @Test
        void copiesContextMetadataIntoTenantProperties() {
            // given
            ContextOverview contextOverview = contextOverview(
                    "tenant-a",
                    "default-rg",
                    Map.of(
                            "region", "eu-west",
                            "tier", "gold"
                    )
            );

            // when
            TenantDescriptor result = AxonServerTenantUtils.tenantDescriptor(contextOverview);

            // then
            assertThat(result.properties()).containsEntry("region", "eu-west")
                                           .containsEntry("tier", "gold");
        }

        @Test
        void addsReplicationGroupFromContextWhenMetadataDoesNotProvideIt() {
            // given
            ContextOverview contextOverview = contextOverview(
                    "tenant-a",
                    "default-rg",
                    Map.of("region", "eu-west")
            );

            // when
            TenantDescriptor result = AxonServerTenantUtils.tenantDescriptor(contextOverview);

            // then
            assertThat(result.properties()).containsEntry("replicationGroup", "default-rg")
                                           .containsEntry("region", "eu-west");
        }

        @Test
        void keepsReplicationGroupFromMetadataWhenAlreadyPresent() {
            // given
            ContextOverview contextOverview = contextOverview(
                    "tenant-a",
                    "default-rg",
                    Map.of("replicationGroup", "metadata-rg")
            );

            // when
            TenantDescriptor result = AxonServerTenantUtils.tenantDescriptor(contextOverview);

            // then
            assertThat(result.properties()).containsEntry("replicationGroup", "metadata-rg");
        }
    }

    private static ContextOverview contextOverview(String contextName,
                                                   String replicationGroup) {
        return contextOverview(contextName, replicationGroup, Map.of());
    }

    private static ContextOverview contextOverview(String contextName,
                                                   String replicationGroup,
                                                   Map<String, String> metadata) {
        return ContextOverview.newBuilder()
                              .setName(contextName)
                              .setReplicationGroup(
                                      ReplicationGroupOverview.newBuilder()
                                                              .setName(replicationGroup)
                                                              .build()
                              )
                              .putAllMetaData(metadata)
                              .build();
    }
}

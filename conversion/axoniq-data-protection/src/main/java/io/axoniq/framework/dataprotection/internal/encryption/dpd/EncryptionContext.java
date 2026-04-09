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
package io.axoniq.framework.dataprotection.internal.encryption.dpd;

import io.axoniq.framework.dataprotection.internal.utils.ExceptionFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.crypto.SecretKey;

/**
 * Holds the encryption context during processing, including the encryption keys for each group
 * and the set of groups to process.
 *
 * @author Frans van Buul
 */
public class EncryptionContext {

    private static class Entry {
        private final String group;
        private final Optional<SecretKey> key;

        public Entry(String group, SecretKey key) {
            this.group = group;
            this.key = key == null ? Optional.empty() : Optional.of(key);
        }

        public String getGroup() {
            return group;
        }

        public Optional<SecretKey> getKey() {
            return key;
        }
    }

    private Set<String> groups;
    private LinkedList<Entry> entries = new LinkedList<>();

    public EncryptionContext(Set<String> groups) {
        this.groups = groups;
    }

    public boolean mustProcess(String group) {
        if(groups == null)
            return true;
        else
            return groups.contains(group);
    }
    

    public void push(String group, SecretKey key) {
        entries.push(new Entry(group, key));
    }

    public void pop(int size) {
        for(int i = 0; i < size; i++) {
            entries.pop();
        }
    }

    public Optional<SecretKey> findKey(String group) {
        return entries
                .stream()
                .filter(entry -> entry.getGroup().equals(group))
                .findFirst()
                .orElseThrow(() -> ExceptionFactory.noKeyForGroup(group))
                .getKey();
    }

    private List<Object> processed = new ArrayList<>();

    public void registerAsProcessed(Object obj) {
        processed.add(obj);
    }

    public boolean hasBeenProcessed(Object obj) {
        for(Object processed : processed) {
            if(processed == obj) {
                return true;
            }
        }
        return false;
    }


}

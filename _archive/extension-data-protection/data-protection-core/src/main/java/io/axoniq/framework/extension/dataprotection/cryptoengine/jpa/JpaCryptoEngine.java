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
package io.axoniq.framework.extension.dataprotection.cryptoengine.jpa;

import io.axoniq.framework.extension.dataprotection.cryptoengine.CryptoEngine;
import io.axoniq.framework.extension.dataprotection.cryptoengine.DatabaseBackedCryptoEngine;
import io.axoniq.framework.extension.dataprotection.internal.utils.ExceptionFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.Base64;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Implementation of {@link CryptoEngine} that extends from {@link DatabaseBackedCryptoEngine} and stores
 * its data in a relational database using JPA. It needs a {@link EntityManagerFactory} to operate. This
 * class will manage its own transactions. Therefore, it doesn't need any type of transaction manager,
 * and {@link EntityManagerFactory} must have the <code>RESOURCE_LOCAL</code> transaction type.
 * <p>
 * This class by itself doesn't do any key caching. If this is required, applications may configure a
 * second-level cache on the {@link EntityManagerFactory}.
 *
 * @author Frans van Buul
 *
 */
public class JpaCryptoEngine extends DatabaseBackedCryptoEngine {

    private final EntityManagerFactory emf;
    private final Class<? extends KeyEntity> entityClass;
    private final Constructor<? extends KeyEntity> entityConstructor;

    /**
     * Creates a new {@link JpaCryptoEngine}. Will use {@link DefaultKeyEntity} as the class to store key entities.
     *
     * @param emf the {@link EntityManagerFactory} to use.
     */
    public JpaCryptoEngine(EntityManagerFactory emf) {
        this(emf, DefaultKeyEntity.class);
    }

    /**
     * Creates a new {@link JpaCryptoEngine} for a custom entity class.
     *
     * @param emf         the {@link EntityManagerFactory} to use.
     * @param entityClass the entity used to store key information
     */
    public JpaCryptoEngine(EntityManagerFactory emf, Class<? extends KeyEntity> entityClass) {
        this.emf = emf;
        this.entityClass = entityClass;
        try {
            entityConstructor = entityClass.getDeclaredConstructor();
            entityConstructor.setAccessible(true);
        } catch (NoSuchMethodException ex) {
            throw ExceptionFactory.forJPANoConstructor(entityClass, ex);
        }
    }

    @Override
    protected SecretKey putKeyIfAbsent(String id, SecretKeySpec secretKeySpec) {
        SecretKey secretKey = getKey(id);
        while(secretKey == null) {
            EntityManager em = emf.createEntityManager();
            try {
                em.getTransaction().begin();
                KeyEntity keyEntity = createKeyEntity();
                keyEntity.setKeyId(id);
                keyEntity.setSecretKeyBase64(Base64.getEncoder().encodeToString(secretKeySpec.getEncoded()));
                em.persist(keyEntity);
                em.getTransaction().commit();
                secretKey = secretKeySpec;
            } catch(PersistenceException ex) {
                if(isConstraintViolationException(ex)) {
                    secretKey = getKey(id);
                } else {
                    throw ex;
                }
            } finally {
                em.close();
            }
        }
        return secretKey;
    }

    @Override
    public SecretKey getKey(String id) {
        EntityManager em = emf.createEntityManager();
        try {
            KeyEntity keyEntity = em.find(entityClass(), id);
            if(keyEntity == null) {
                return null;
            } else {
                return new SecretKeySpec(Base64.getDecoder().decode(keyEntity.getSecretKeyBase64()), "AES");
            }
        } finally {
            em.close();
        }
    }

    @Override
    public void deleteKey(String id) {
        EntityManager em = emf.createEntityManager();
        try {
            em.getTransaction().begin();
            em.createQuery("DELETE FROM " + entityClass().getName() + " e WHERE e.keyId = :id")
                    .setParameter("id", id)
                    .executeUpdate();
            em.getTransaction().commit();
        } finally {
            em.close();
        }
    }

    /**
     * Provides access to the entity class specified in the constructor; made <code>protected</code> to
     * make this information available to subclasses
     *
     * @return the entity class
     */
    protected Class<? extends KeyEntity> entityClass() {
        return entityClass;
    }

    private KeyEntity createKeyEntity() {
        try {
            return entityConstructor.newInstance();
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException ex) {
            throw ExceptionFactory.forJPAEntityInstantiation(entityClass, entityConstructor, ex);
        }
    }

    private static boolean isConstraintViolationException(Exception ex) {
        Throwable t = ex;
        while(t != null) {
            if(t instanceof SQLIntegrityConstraintViolationException) return true;
            t = t.getCause();
        }
        return false;
    }

}

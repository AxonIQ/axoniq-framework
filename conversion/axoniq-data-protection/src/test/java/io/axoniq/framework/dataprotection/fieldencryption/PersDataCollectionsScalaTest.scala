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
package io.axoniq.framework.dataprotection.fieldencryption

import io.axoniq.framework.dataprotection.api.{FieldEncrypter, dataSubjectId, personalData}
import io.axoniq.framework.dataprotection.cryptoengine.{CryptoEngine, InMemoryCryptoEngine}
import io.axoniq.framework.dataprotection.utils.TestUtils._
import org.fluttercode.datafactory.impl.DataFactory
import org.junit.Assert.assertTrue
import org.junit.{Before, Test}
import org.slf4j.LoggerFactory

import java.lang.invoke.MethodHandles
import java.util.concurrent.ThreadLocalRandom

class PersDataCollectionsScalaTest {

  case class A(@dataSubjectId id: Int, @personalData names: scala.collection.immutable.Seq[String])
  case class B(@dataSubjectId id: Int, @personalData names: scala.collection.immutable.SortedSet[String])
  case class C(@dataSubjectId id: Int, @personalData names: scala.collection.mutable.Seq[_ <: String]) /* testing whether Scala wildcard upper bounds are accepted */
  case class D(@dataSubjectId id: Int, @personalData names: scala.collection.mutable.SortedSet[String])

  private val logger = LoggerFactory.getLogger(MethodHandles.lookup.lookupClass)

  private var cryptoEngine : CryptoEngine = _
  private var fieldEncrypter : FieldEncrypter = _
  private var dataFactory : DataFactory = _

  @Before
  def setUp(): Unit = {
    cryptoEngine = new InMemoryCryptoEngine
    fieldEncrypter = new FieldEncrypter(cryptoEngine, defaultTestConverter())
    dataFactory = new DataFactory
    dataFactory.randomize(ThreadLocalRandom.current.nextInt)
  }

  @Test
  def personalDataCollFieldsMustGetEncryptedA(): Unit = {
    val a = A(3, dataFactory.getLastName :: dataFactory.getLastName :: dataFactory.getLastName :: Nil)
    logger.info("Before encryption: {}", a)
    fieldEncrypter.encrypt(a)
    logger.info("After encryption: {}", a)
    a.names.foreach((name: String) => assertTrue(isEncrypted(name)))
    fieldEncrypter.decrypt(a)
    a.names.foreach((name: String) => assertTrue(isClear(name)))
  }

  @Test
  def personalDataCollFieldsMustGetEncryptedB(): Unit = {
    val a = B(3, scala.collection.immutable.TreeSet(dataFactory.getLastName, dataFactory.getLastName, dataFactory.getLastName))
    logger.info("Before encryption: {}", a)
    fieldEncrypter.encrypt(a)
    logger.info("After encryption: {}", a)
    a.names.foreach((name: String) => assertTrue(isEncrypted(name)))
    fieldEncrypter.decrypt(a)
    a.names.foreach((name: String) => assertTrue(isClear(name)))
  }

  @Test
  def personalDataCollFieldsMustGetEncryptedC(): Unit = {
    val a = C(3, scala.collection.mutable.Seq(dataFactory.getLastName, dataFactory.getLastName, dataFactory.getLastName))
    logger.info("Before encryption: {}", a)
    fieldEncrypter.encrypt(a)
    logger.info("After encryption: {}", a)
    a.names.foreach((name: String) => assertTrue(isEncrypted(name)))
    fieldEncrypter.decrypt(a)
    a.names.foreach((name: String) => assertTrue(isClear(name)))
  }

  @Test
  def personalDataCollFieldsMustGetEncryptedD(): Unit = {
    val a = D(3, scala.collection.mutable.TreeSet(dataFactory.getLastName, dataFactory.getLastName, dataFactory.getLastName))
    logger.info("Before encryption: {}", a)
    fieldEncrypter.encrypt(a)
    logger.info("After encryption: {}", a)
    a.names.foreach((name: String) => assertTrue(isEncrypted(name)))
    fieldEncrypter.decrypt(a)
    a.names.foreach((name: String) => assertTrue(isClear(name)))
  }
}

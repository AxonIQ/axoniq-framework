/*
 * Copyright (c) 2017-2023. AxonIQ B.V.
 */
package io.axoniq.framework.dataprotection.fieldencryption

import io.axoniq.framework.dataprotection.api.{FieldEncrypter, Scope, dataSubjectId, personalData}
import io.axoniq.framework.dataprotection.cryptoengine.{CryptoEngine, InMemoryCryptoEngine}
import io.axoniq.framework.dataprotection.utils.TestUtils.defaultTestConverter
import org.fluttercode.datafactory.impl.DataFactory
import org.junit.{Assert, Before, Test}
import org.slf4j.LoggerFactory

import java.lang.invoke.MethodHandles
import java.util.concurrent.ThreadLocalRandom

class MapScalaTest {

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

  case class Person(@dataSubjectId id: Int, @personalData(scope = Scope.VALUE) emailAddresses: Map[String, String])

  @Test
  def mapSupportSmokeTest(): Unit = {
    val person = Person(1, Map("private" -> dataFactory.getEmailAddress, "work" -> dataFactory.getEmailAddress))
    logger.info("Person before encryption: {}", person)
    Assert.assertTrue(person.emailAddresses.contains("private"))
    Assert.assertTrue(person.emailAddresses.contains("work"))
    Assert.assertTrue(person.emailAddresses("private").contains("@"))
    Assert.assertTrue(person.emailAddresses("work").contains("@"))
    fieldEncrypter.encrypt(person)
    Assert.assertTrue(person.emailAddresses.contains("private"))
    Assert.assertTrue(person.emailAddresses.contains("work"))
    Assert.assertFalse(person.emailAddresses("private").contains("@"))
    Assert.assertFalse(person.emailAddresses("work").contains("@"))
    logger.info("Person after encryption: {}", person)
    fieldEncrypter.decrypt(person)
    Assert.assertTrue(person.emailAddresses.contains("private"))
    Assert.assertTrue(person.emailAddresses.contains("work"))
    Assert.assertTrue(person.emailAddresses("private").contains("@"))
    Assert.assertTrue(person.emailAddresses("work").contains("@"))
    logger.info("Person after decryption: {}", person)
  }

  case class PersonMutable(@dataSubjectId id: Int, @personalData(scope = Scope.VALUE) emailAddresses: scala.collection.mutable.Map[String, String])

  @Test
  def mutableMapSupportSmokeTest(): Unit = {
    val person = PersonMutable(1, scala.collection.mutable.Map("private" -> dataFactory.getEmailAddress, "work" -> dataFactory.getEmailAddress))
    logger.info("Person before encryption: {}", person)
    Assert.assertTrue(person.emailAddresses.contains("private"))
    Assert.assertTrue(person.emailAddresses.contains("work"))
    Assert.assertTrue(person.emailAddresses("private").contains("@"))
    Assert.assertTrue(person.emailAddresses("work").contains("@"))
    fieldEncrypter.encrypt(person)
    Assert.assertTrue(person.emailAddresses.contains("private"))
    Assert.assertTrue(person.emailAddresses.contains("work"))
    Assert.assertFalse(person.emailAddresses("private").contains("@"))
    Assert.assertFalse(person.emailAddresses("work").contains("@"))
    logger.info("Person after encryption: {}", person)
    fieldEncrypter.decrypt(person)
    Assert.assertTrue(person.emailAddresses.contains("private"))
    Assert.assertTrue(person.emailAddresses.contains("work"))
    Assert.assertTrue(person.emailAddresses("private").contains("@"))
    Assert.assertTrue(person.emailAddresses("work").contains("@"))
    logger.info("Person after decryption: {}", person)
  }

  case class A(@dataSubjectId id: Int, @personalData(scope = Scope.KEY) x: Map[String, String])

  @Test
  def personalDataInKeyTest(): Unit = {
    val a = A(1, Map("test" -> "test"))
    fieldEncrypter.encrypt(a)
    Assert.assertFalse(a.x.contains("test"))
    Assert.assertTrue(a.x.values.exists(_ == "test"))
    fieldEncrypter.decrypt(a)
    Assert.assertEquals("test", a.x("test"))
  }

  case class B(@dataSubjectId id: Int, @personalData(scope = Scope.BOTH) x: Map[String, String])

  @Test
  def personalDataInBothTest(): Unit = {
    val b = B(1, Map("test" -> "test"))
    fieldEncrypter.encrypt(b)
    Assert.assertFalse(b.x.contains("test"))
    Assert.assertFalse(b.x.values.exists(_ == "test"))
    fieldEncrypter.decrypt(b)
    Assert.assertEquals("test", b.x("test"))
  }

}

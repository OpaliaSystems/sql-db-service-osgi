package systems.opalia.service.sql.testing

import java.nio.file.{Files, Paths}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import org.scalatest.flatspec._
import org.scalatest.matchers.should._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import play.api.libs.json.Json
import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import systems.opalia.commons.database.converter.BigDecimalConverter._
import systems.opalia.commons.database.converter.DefaultConverter._
import systems.opalia.commons.database.converter.NativeTypesConverter._
import systems.opalia.commons.database.converter.TimeTypesConverter._
import systems.opalia.commons.io.{FileUtils, PropertiesUtils}
import systems.opalia.commons.json.JsonAstTransformer
import systems.opalia.interfaces.database._
import systems.opalia.launcher.Launcher


class H2DatabaseServiceTest
  extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Matchers {

  val testName = "h2-database-service-test"

  var launcher: Launcher = _
  var transactional: Transactional = _

  override final def beforeAll(): Unit = {

    val testPath = Paths.get("./tmp").resolve(testName)

    if (Files.exists(testPath))
      FileUtils.deleteRecursively(testPath)

    sys.props("config.file") = s"./testing/src/test-it/resources/$testName.conf"
    sys.props("base-path") = testPath.toString

    val properties =
      PropertiesUtils.read("./testing/src/test-it/resources/boot.properties") +
        (Launcher.PROPERTY_WORKING_DIRECTORY -> testPath.toString)

    launcher = Launcher.newLauncher(properties, useSystemProperties = false)

    launcher.setup()

    Await.result(launcher.awaitUp(), Duration.Inf)

    transactional = launcher.serviceManager.getServices(classOf[DatabaseService], "(database=sql)").asScala.head
  }

  override final def afterAll(): Unit = {

    launcher.shutdown()

    Await.result(launcher.awaitDown(), Duration.Inf)
  }

  private def executeMultiline(factory: QueryFactory, statements: String): Unit = {

    statements.split(""";\n""")
      .map(_.trim)
      .filter(_.nonEmpty)
      .foreach {
        statement =>

          factory
            .newQuery(statement)
            .execute()
      }
  }

  override def beforeEach(): Unit = {

    transactional.withTransaction {
      factory =>

        executeMultiline(
          factory,
          """
            |CREATE TABLE _Person (
            |  id INT NOT NULL,
            |  name VARCHAR(120) NOT NULL,
            |  counter INT NOT NULL,
            |  extra VARCHAR(120) NULL,
            |  PRIMARY KEY (id)
            |);
            |
            |CREATE TABLE _Group (
            |  id INT NOT NULL,
            |  name VARCHAR(120) NOT NULL,
            |  PRIMARY KEY (id)
            |);
            |
            |CREATE TABLE _Membership (
            |  id INT NOT NULL,
            |  person_id INT NOT NULL,
            |  group_id INT NOT NULL,
            |  PRIMARY KEY (id),
            |  FOREIGN KEY (person_id) REFERENCES _Person(id),
            |  FOREIGN KEY (group_id) REFERENCES _Group(id)
            |);
            |
            |CREATE TABLE _Relationship (
            |  id INT NOT NULL,
            |  name VARCHAR(120) NOT NULL,
            |  friend BOOLEAN NULL,
            |  person1_id INT NOT NULL,
            |  person2_id INT NOT NULL,
            |  PRIMARY KEY (id),
            |  FOREIGN KEY (person1_id) REFERENCES _Person(id),
            |  FOREIGN KEY (person2_id) REFERENCES _Person(id)
            |);
            |
            |CREATE TABLE _Values (
            |  val_boolean BOOLEAN NOT NULL,
            |  val_byte TINYINT NOT NULL,
            |  val_short SMALLINT NOT NULL,
            |  val_int MEDIUMINT NOT NULL,
            |  val_long BIGINT NOT NULL,
            |  val_float FLOAT4 NOT NULL,
            |  val_double FLOAT8 NOT NULL,
            |  val_char NVARCHAR(1) NOT NULL,
            |  val_string TEXT NOT NULL,
            |  val_bigDecimal NUMBER NOT NULL,
            |  val_localDate DATE NOT NULL,
            |  val_localTime TIME NOT NULL,
            |  val_localDateTime DATETIME NOT NULL,
            |  val_binary BINARY NOT NULL
            |);
          """.stripMargin)

        executeMultiline(
          factory,
          """
            |INSERT INTO _Person (id, name, counter) VALUES (1, 'Armin', 25);
            |INSERT INTO _Person (id, name, counter) VALUES (2, 'Folker', 28);
            |INSERT INTO _Person (id, name, counter) VALUES (3, 'Lorelei', 88);
            |INSERT INTO _Person (id, name, counter) VALUES (4, 'Berthold', 3);
            |INSERT INTO _Person (id, name, counter) VALUES (5, 'Fritz', 42);
            |INSERT INTO _Person (id, name, counter) VALUES (6, 'Dagmar', 73);
            |INSERT INTO _Person (id, name, counter) VALUES (7, 'Emma', 25);
            |INSERT INTO _Person (id, name, counter) VALUES (8, 'Erhard', 109);
            |
            |INSERT INTO _Group (id, name) VALUES (9, 'Group1');
            |INSERT INTO _Group (id, name) VALUES (10, 'Group2');
            |
            |INSERT INTO _Membership (id, person_id, group_id) VALUES (11, 1, 9);
            |INSERT INTO _Membership (id, person_id, group_id) VALUES (12, 2, 9);
            |INSERT INTO _Membership (id, person_id, group_id) VALUES (13, 3, 9);
            |INSERT INTO _Membership (id, person_id, group_id) VALUES (14, 4, 9);
            |INSERT INTO _Membership (id, person_id, group_id) VALUES (15, 4, 10);
            |INSERT INTO _Membership (id, person_id, group_id) VALUES (16, 5, 10);
            |INSERT INTO _Membership (id, person_id, group_id) VALUES (17, 6, 10);
            |INSERT INTO _Membership (id, person_id, group_id) VALUES (18, 7, 10);
            |
            |INSERT INTO _Relationship (id, name, friend, person1_id, person2_id) VALUES (19, 'KNOWS', TRUE, 1, 2);
            |INSERT INTO _Relationship (id, name, friend, person1_id, person2_id) VALUES (20, 'KNOWS', TRUE, 1, 3);
            |INSERT INTO _Relationship (id, name,         person1_id, person2_id) VALUES (21, 'KNOWS', 1, 4);
            |INSERT INTO _Relationship (id, name, friend, person1_id, person2_id) VALUES (22, 'LOVES', FALSE, 1, 3);
            |INSERT INTO _Relationship (id, name, friend, person1_id, person2_id) VALUES (23, 'KNOWS', TRUE, 3, 4);
            |INSERT INTO _Relationship (id, name,         person1_id, person2_id) VALUES (24, 'LOVES', 3, 4);
            |INSERT INTO _Relationship (id, name, friend, person1_id, person2_id) VALUES (25, 'KNOWS', TRUE, 2, 3);
            |INSERT INTO _Relationship (id, name, friend, person1_id, person2_id) VALUES (26, 'KNOWS', FALSE, 2, 5);
            |INSERT INTO _Relationship (id, name, friend, person1_id, person2_id) VALUES (27, 'KNOWS', TRUE, 4, 6);
            |INSERT INTO _Relationship (id, name, friend, person1_id, person2_id) VALUES (28, 'KNOWS', TRUE, 5, 7);
            |INSERT INTO _Relationship (id, name, friend, person1_id, person2_id) VALUES (29, 'KNOWS', TRUE, 6, 7);
            |INSERT INTO _Relationship (id, name, friend, person1_id, person2_id) VALUES (30, 'KNOWS', TRUE, 7, 8);
          """.stripMargin)
    }
  }

  override def afterEach(): Unit = {

    transactional.withTransaction {
      factory =>

        executeMultiline(
          factory,
          """
            |DROP TABLE _Relationship;
            |DROP TABLE _Membership;
            |DROP TABLE _Group;
            |DROP TABLE _Person;
            |DROP TABLE _Values;
          """.stripMargin)
    }
  }

  it should "be able to make a query with multiple arguments" in {

    transactional.withTransaction {
      factory =>

        val result =
          factory.newQuery(
            """
              |SELECT P2.name AS name
              |FROM _Person P1
              |CROSS JOIN _Person P2
              |INNER JOIN _Membership M1 ON P1.id = M1.person_id
              |INNER JOIN _Group G1 ON M1.group_id = G1.id AND G1.name = 'Group1'
              |INNER JOIN _Membership M2 ON P2.id = M2.person_id
              |INNER JOIN _Group G2 ON M2.group_id = G2.id AND G2.name = 'Group1'
              |INNER JOIN _Relationship R1 ON P1.id = R1.person1_id AND P2.id = R1.person2_id AND R1.name = 'KNOWS'
              |WHERE P1.name = :name_1 or P1.name = :name_2
            """.stripMargin)
            .on("name_1", "Folker")
            .on("name_2", "Lorelei")
            .executeAndFetch()
            .cursor
            .transform(row => row[String]("name"))
            .toSet

        result should be(Set("Lorelei", "Berthold"))
    }
  }

  it should "be able to convert result rows to JSON" in {

    transactional.withTransaction {
      factory =>

        val result1 =
          FileUtils.using(factory.newQuery(
            """
              |SELECT P2.name AS name, P2.counter AS counter
              |FROM _Person P1
              |CROSS JOIN _Person P2
              |INNER JOIN _Membership M1 ON P1.id = M1.person_id
              |INNER JOIN _Group G1 ON M1.group_id = G1.id AND G1.name = 'Group1'
              |INNER JOIN _Relationship R1 ON R1.name = 'LOVES' AND
              |((P1.id = R1.person1_id AND P2.id = R1.person2_id) OR (P1.id = R1.person2_id AND P2.id = R1.person1_id))
              |WHERE P1.name = :name_1
            """.stripMargin)
            .on("name_1", "Armin")
            .executeAndFetch().cursor)(_.transform(row => row.toJson).toSingle)

        val result2 =
          FileUtils.using(factory.newQuery(
            """
              |SELECT R2.name AS name, R2.friend AS friend
              |FROM _Person P1
              |CROSS JOIN _Person P2
              |INNER JOIN _Membership M1 ON P1.id = M1.person_id
              |INNER JOIN _Group G1 ON M1.group_id = G1.id AND G1.name = 'Group1'
              |INNER JOIN _Membership M2 ON P2.id = M2.person_id
              |INNER JOIN _Group G2 ON M2.group_id = G2.id AND G2.name = 'Group2'
              |INNER JOIN _Relationship R1 ON R1.name = 'LOVES' AND
              |((P1.id = R1.person1_id AND P2.id = R1.person2_id) OR (P1.id = R1.person2_id AND P2.id = R1.person1_id))
              |INNER JOIN _Relationship R2 ON R2.name = 'KNOWS' AND
              |((P1.id = R2.person1_id AND P2.id = R2.person2_id) OR (P1.id = R2.person2_id AND P2.id = R2.person1_id))
            """.stripMargin)
            .executeAndFetch().cursor)(_.transform(row => row.toJson).toSingle)

        JsonAstTransformer.toPlayJson(result1) should be(
          Json.obj("name" -> "Lorelei", "counter" -> 88)
        )

        JsonAstTransformer.toPlayJson(result2) should be(
          Json.obj("name" -> "KNOWS", "friend" -> true)
        )
    }
  }

  it should "handle optional parameters" in {

    transactional.withTransaction {
      factory =>

        val query1 =
          factory.newQuery(
            """
              |SELECT P1.name AS name, P1.counter AS counter
              |FROM _Person P1
              |INNER JOIN _Membership M1 ON P1.id = M1.person_id
              |INNER JOIN _Group G1 ON M1.group_id = G1.id AND G1.name = 'Group2'
              |WHERE P1.name = :name_1
            """.stripMargin)
            .on("name_1", "Dagmar")

        val query2 =
          factory.newQuery(
            """
              |SELECT P1.name AS name, P1.counter AS counter
              |FROM _Person P1
              |INNER JOIN _Membership M1 ON P1.id = M1.person_id
              |INNER JOIN _Group G1 ON M1.group_id = G1.id AND G1.name = 'Group2'
              |WHERE P1.name = :name_1
            """.stripMargin)
            .on("name_1", Some("Dagmar"))

        val query3 =
          factory.newQuery(
            """
              |SELECT P1.name AS name, P1.counter AS counter
              |FROM _Person P1
              |INNER JOIN _Membership M1 ON P1.id = M1.person_id
              |INNER JOIN _Group G1 ON M1.group_id = G1.id AND G1.name = 'Group2'
              |WHERE P1.name = :name_1
            """.stripMargin)
            .on("name_1", None)

        val query4 =
          factory.newQuery(
            """
              |SELECT P1.name AS name, P1.extra AS extra
              |FROM _Person P1
              |INNER JOIN _Membership M1 ON P1.id = M1.person_id
              |INNER JOIN _Group G1 ON M1.group_id = G1.id AND G1.name = 'Group2'
              |WHERE P1.name = :name_1
            """.stripMargin)
            .on("name_1", "Dagmar")


        val transformer1 =
          (row: Row) => (row[String]("name"), row[Option[Int]]("counter"))

        val transformer2 =
          (row: Row) => (row[String]("name"), row[Option[String]]("counter"))

        val transformer3 =
          (row: Row) => (row[String]("name"), row[Option[String]]("extra"))

        val transformer4 =
          (row: Row) => (row[String]("name"), row[String]("extra"))

        query1.executeAndFetch().cursor.transform(transformer1).toSingleOpt shouldBe Some("Dagmar", Some(73))
        query1.executeAndFetch().cursor.transform(transformer2).toSingleOpt shouldBe Some("Dagmar", None)

        query2.executeAndFetch().cursor.transform(transformer1).toSingleOpt shouldBe Some("Dagmar", Some(73))
        query2.executeAndFetch().cursor.transform(transformer2).toSingleOpt shouldBe Some("Dagmar", None)

        query3.executeAndFetch().cursor.transform(transformer1).toSingleOpt shouldBe None
        query3.executeAndFetch().cursor.transform(transformer2).toSingleOpt shouldBe None

        query4.executeAndFetch().cursor.transform(transformer3).toSingleOpt shouldBe Some("Dagmar", None)

        an[IllegalArgumentException] should be thrownBy query4.executeAndFetch().cursor.transform(transformer4).toSingleOpt
    }
  }

  it should "be able to parse a required number of rows" in {

    transactional.withTransaction {
      factory =>

        val queryExpectEmpty =
          factory.newQuery(
            """
              |SELECT P1.name AS name
              |FROM _Person P1
              |INNER JOIN _Membership M1 ON P1.id = M1.person_id
              |INNER JOIN _Group G1 ON M1.group_id = G1.id AND G1.name = 'Group1'
              |WHERE P1.name = :name_1
            """.stripMargin)
            .on("name_1", "Adam")

        val queryExpectOne =
          factory.newQuery(
            """
              |SELECT P1.name AS name
              |FROM _Person P1
              |INNER JOIN _Membership M1 ON P1.id = M1.person_id
              |INNER JOIN _Group G1 ON M1.group_id = G1.id AND G1.name = 'Group1'
              |WHERE P1.name = :name_1
            """.stripMargin)
            .on("name_1", "Armin")

        val queryExpectMultiple =
          factory.newQuery(
            """
              |SELECT P1.name AS name
              |FROM _Person P1
              |INNER JOIN _Membership M1 ON P1.id = M1.person_id
              |INNER JOIN _Group G1 ON M1.group_id = G1.id AND G1.name = 'Group1'
            """.stripMargin)

        val transformer =
          (row: Row) => row[String]("name")

        // single

        an[IllegalArgumentException] should be thrownBy
          FileUtils.using(queryExpectEmpty.executeAndFetch().cursor)(
            _.transform(transformer).toSingle
          )

        FileUtils.using(queryExpectOne.executeAndFetch().cursor)(
          _.transform(transformer).toSingle
        ) shouldBe a[String]

        an[IllegalArgumentException] should be thrownBy
          FileUtils.using(queryExpectMultiple.executeAndFetch().cursor)(
            _.transform(transformer).toSingle
          )

        // single optional

        FileUtils.using(queryExpectEmpty.executeAndFetch().cursor)(
          _.transform(transformer).toSingleOpt
        ) shouldBe empty

        FileUtils.using(queryExpectOne.executeAndFetch().cursor)(
          _.transform(transformer).toSingleOpt
        ) shouldBe defined

        an[IllegalArgumentException] should be thrownBy
          FileUtils.using(queryExpectMultiple.executeAndFetch().cursor)(
            _.transform(transformer).toSingleOpt
          )

        // sequence

        FileUtils.using(queryExpectEmpty.executeAndFetch().cursor)(
          _.transform(transformer).toIndexedSeq
        ) should have size 0

        FileUtils.using(queryExpectOne.executeAndFetch().cursor)(
          _.transform(transformer).toIndexedSeq
        ) should have size 1

        FileUtils.using(queryExpectMultiple.executeAndFetch().cursor)(
          _.transform(transformer).toIndexedSeq
        ) should have size 4

        // non empty sequence

        an[IllegalArgumentException] should be thrownBy
          FileUtils.using(queryExpectEmpty.executeAndFetch().cursor)(
            _.transform(transformer).toNonEmptyIndexedSeq
          )

        FileUtils.using(queryExpectOne.executeAndFetch().cursor)(
          _.transform(transformer).toNonEmptyIndexedSeq
        ) should have size 1

        FileUtils.using(queryExpectMultiple.executeAndFetch().cursor)(
          _.transform(transformer).toNonEmptyIndexedSeq
        ) should have size 4
    }
  }

  it should "be able to convert native types" in {

    transactional.withTransaction {
      factory =>

        val boolean: Boolean = true
        val byte: Byte = 42
        val short: Short = 1042
        val int: Int = 10042
        val long: Long = 100042
        val float: Float = 0.73f
        val double: Double = 0.073d
        val char: Char = 'Ä'
        val string: String = "test"
        val bigDecimal: BigDecimal = BigDecimal("9182736453733.8376546370383873654670872634998616321013016732842")
        val localDate: LocalDate = LocalDate.now()
        val localTime: LocalTime = LocalTime.now().withNano(0)
        val localDateTime: LocalDateTime = LocalDateTime.now().withNano(0)
        val binary: Seq[Byte] = List(0x00.toByte, 0xFF.toByte, 0xFA.toByte)

        factory.newQuery(
          """
            |INSERT INTO _Values
            |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          """.stripMargin)
          .on("1", boolean)
          .on("2", byte)
          .on("3", short)
          .on("4", int)
          .on("5", long)
          .on("6", float)
          .on("7", double)
          .on("8", char)
          .on("9", string)
          .on("10", bigDecimal)
          .on("11", localDate)
          .on("12", localTime)
          .on("13", localDateTime)
          .on("14", binary)
          .execute()

        val result =
          FileUtils.using(factory.newQuery(
            """
              |SELECT *
              |FROM _Values
            """.stripMargin)
            .executeAndFetch().cursor)(_.transform {
            row =>

              (row[Boolean]("val_boolean"),
                row[Byte]("val_byte"),
                row[Short]("val_short"),
                row[Int]("val_int"),
                row[Long]("val_long"),
                row[Float]("val_float"),
                row[Double]("val_double"),
                row[Char]("val_char"),
                row[String]("val_string"),
                row[BigDecimal]("val_bigDecimal"),
                row[LocalDate]("val_localDate"),
                row[LocalTime]("val_localTime"),
                row[LocalDateTime]("val_localDateTime"),
                row[Seq[Byte]]("val_binary"))

          }.toSingle)

        result._1 should be(boolean)
        result._2 should be(byte)
        result._3 should be(short)
        result._4 should be(int)
        result._5 should be(long)
        result._6 should be(float)
        result._7 should be(double)
        result._8 should be(char)
        result._9 should be(string)

        result._10 should be(bigDecimal)
        result._11 should be(localDate)
        result._12 should be(localTime)
        result._13 should be(localDateTime)
        result._14 should be(binary)
    }
  }

  it should "should support transactions with unspecific lifetime" in {

    FileUtils.using(transactional.createClosableTransaction()) {
      transaction =>

        val result =
          transaction.queryFactory.newQuery(
            """
              |SELECT P2.name AS name
              |FROM _Person P1
              |CROSS JOIN _Person P2
              |INNER JOIN _Membership M1 ON P1.id = M1.person_id
              |INNER JOIN _Group G1 ON M1.group_id = G1.id AND G1.name = 'Group1'
              |INNER JOIN _Membership M2 ON P2.id = M2.person_id
              |INNER JOIN _Group G2 ON M2.group_id = G2.id AND G2.name = 'Group1'
              |INNER JOIN _Relationship R1 ON P1.id = R1.person1_id AND P2.id = R1.person2_id AND R1.name = 'KNOWS'
              |WHERE P1.name = :name_1 or P1.name = :name_2
            """.stripMargin)
            .on("name_1", "Folker")
            .on("name_2", "Lorelei")
            .executeAndFetch()
            .cursor
            .transform(row => row[String]("name"))
            .toSet

        result should be(Set("Lorelei", "Berthold"))
    }
  }
}

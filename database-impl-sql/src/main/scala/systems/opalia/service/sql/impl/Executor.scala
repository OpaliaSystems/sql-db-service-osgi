package systems.opalia.service.sql.impl

import java.sql.{Connection, JDBCType, PreparedStatement, ResultSet}
import java.time.{LocalDate, LocalDateTime, LocalTime}
import org.jooq
import org.jooq.impl.DSL
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.util.{Failure, Success}
import systems.opalia.commons.codec.Hex
import systems.opalia.interfaces.cursor.VersatileCursor
import systems.opalia.interfaces.database._
import systems.opalia.interfaces.json.JsonAst
import systems.opalia.interfaces.logging.Logger


class Executor(logger: Logger,
               connection: Connection,
               useClosableTransaction: Boolean,
               closeables: mutable.ListBuffer[OneTimeCloseable]) {

  def executeAndFetch(statement: String, parameters: Map[String, Any]): Result = {

    val f: (PreparedStatement, Seq[OneTimeCloseable]) => Result =
      (preparedStatement: PreparedStatement, closeablesLocal: Seq[OneTimeCloseable]) => {

        val metadata = getMetadata(preparedStatement)
        val underlying = getRowCursor(preparedStatement, metadata)

        val versatileCursor =
          new VersatileCursor[Row] {

            override def next(): Boolean =
              underlying.next()

            override def get: Row =
              underlying.get

            override def close(): Unit = {

              closeablesLocal.foreach(_.close())
            }
          }

        new Result {

          val columns: IndexedSeq[String] = metadata.keys.toIndexedSeq
          val cursor: VersatileCursor[Row] = versatileCursor
        }
      }

    performQuery(statement, parameters)(f)
  }

  def execute(statement: String, parameters: Map[String, Any]): Unit = {

    val f: (PreparedStatement, Seq[OneTimeCloseable]) => Unit =
      (_: PreparedStatement, closeablesLocal: Seq[OneTimeCloseable]) => {

        closeablesLocal.foreach(_.close())
      }

    performQuery(statement, parameters)(f)
  }

  private def performQuery[T](statement: String, parameters: Map[String, Any])
                             (black: (PreparedStatement, Seq[OneTimeCloseable]) => T): T = {

    val start = System.currentTimeMillis
    val closeablesLocal = mutable.ListBuffer[OneTimeCloseable]()

    try {

      logger.trace(s"Parse dialect free SQL statement:\n$statement")

      val query = DSL.using(connection).parser().parseQuery(statement)

      closeablesLocal.append(new OneTimeCloseable(query))

      findBinding(query, parameters)

      val statementTranslated = query.getSQL()

      logger.debug(s"Apply dialect specific SQL statement:\n$statementTranslated")

      val preparedStatement = connection.prepareStatement(statementTranslated)

      closeablesLocal.append(new OneTimeCloseable(preparedStatement))

      val arguments = query.getBindValues.asScala.toList

      setValues(preparedStatement, arguments)

      preparedStatement.execute()

      val result = black(preparedStatement, closeablesLocal.reverse)

      val end = System.currentTimeMillis

      if (!useClosableTransaction)
        logger.trace(s"A SQL statement was performed in ${end - start} ms.")

      result

    } finally {

      closeables.appendAll(closeablesLocal.reverse)
    }
  }

  def newQueryFactory(): QueryFactory = {

    new QueryFactory {

      def newQuery(statement: String): Query =
        new QueryImpl(statement)
    }
  }

  private def findBinding(query: jooq.Query, parameters: Map[String, Any]): Unit = {

    parameters
      .foreach {
        case (key, value) =>

          findBinding(query, key, value)
      }
  }

  private def findBinding(query: jooq.Query, key: String, value: Any): Unit = {

    value match {
      case null => query.bind(key, null)
      case x: Char => query.bind(key, x.toString)
      case x: BigDecimal => query.bind(key, x.underlying)
      case x: LocalDate => query.bind(key, java.sql.Date.valueOf(x))
      case x: LocalTime => query.bind(key, java.sql.Time.valueOf(x))
      case x: LocalDateTime => query.bind(key, java.sql.Timestamp.valueOf(x))
      case x: Seq[_] if (x.forall(_.isInstanceOf[Byte])) => query.bind(key, x.map(_.asInstanceOf[Byte]).toArray)
      case x: Array[_] if (x.forall(_.isInstanceOf[Byte])) => query.bind(key, x.map(_.asInstanceOf[Byte]))
      case Some(x) => findBinding(query, key, x)
      case None => query.bind(key, null)
      case x => query.bind(key, x)
    }
  }

  private def setValues(statement: PreparedStatement, parameters: Seq[Any]): Unit = {

    parameters
      .zipWithIndex.map(x => (x._2 + 1) -> x._1)
      .foreach {
        case (key, value) =>

          setValue(statement, key, value)
      }
  }

  private def setValue(statement: PreparedStatement, key: Int, value: Any): Unit = {

    value match {
      case null => statement.setNull(key, JDBCType.NULL.getVendorTypeNumber)
      case x: Boolean => statement.setBoolean(key, x)
      case x: Byte => statement.setByte(key, x)
      case x: Short => statement.setShort(key, x)
      case x: Integer => statement.setInt(key, x)
      case x: Long => statement.setLong(key, x)
      case x: Float => statement.setFloat(key, x)
      case x: Double => statement.setDouble(key, x)
      case x: Char => statement.setString(key, x.toString)
      case x: String => statement.setString(key, x)
      case x: BigDecimal => statement.setBigDecimal(key, x.underlying)
      case x: LocalDate => statement.setDate(key, java.sql.Date.valueOf(x))
      case x: LocalTime => statement.setTime(key, java.sql.Time.valueOf(x))
      case x: LocalDateTime => statement.setTimestamp(key, java.sql.Timestamp.valueOf(x))
      case x: java.math.BigDecimal => statement.setBigDecimal(key, x)
      case x: java.sql.Date => statement.setDate(key, x)
      case x: java.sql.Time => statement.setTime(key, x)
      case x: java.sql.Timestamp => statement.setTimestamp(key, x)
      case x: Seq[_] if (x.forall(_.isInstanceOf[Byte])) => statement.setBytes(key, x.map(_.asInstanceOf[Byte]).toArray)
      case x: Array[_] if (x.forall(_.isInstanceOf[Byte])) => statement.setBytes(key, x.map(_.asInstanceOf[Byte]))
      case Some(x) => setValue(statement, key, x)
      case None => statement.setNull(key, JDBCType.NULL.getVendorTypeNumber)
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot put unsupported type with value $value (${value.getClass.getName}) at key $key.")
    }
  }

  private def getColumnValue(resultSet: ResultSet, columnName: String, jdbcType: JDBCType): Any = {

    jdbcType match {

      case JDBCType.NULL => null

      case JDBCType.BIT => resultSet.getBoolean(columnName)
      case JDBCType.BOOLEAN => resultSet.getBoolean(columnName)

      case JDBCType.TINYINT => resultSet.getByte(columnName)
      case JDBCType.SMALLINT => resultSet.getShort(columnName)
      case JDBCType.INTEGER => resultSet.getInt(columnName)
      case JDBCType.BIGINT => resultSet.getLong(columnName)

      case JDBCType.REAL => resultSet.getFloat(columnName)
      case JDBCType.FLOAT => resultSet.getDouble(columnName)
      case JDBCType.DOUBLE => resultSet.getDouble(columnName)

      case JDBCType.NUMERIC => BigDecimal(resultSet.getBigDecimal(columnName))
      case JDBCType.DECIMAL => BigDecimal(resultSet.getBigDecimal(columnName))

      case JDBCType.DATE => resultSet.getDate(columnName).toLocalDate
      case JDBCType.TIME => resultSet.getTime(columnName).toLocalTime
      case JDBCType.TIMESTAMP => resultSet.getTimestamp(columnName).toLocalDateTime

      case JDBCType.BINARY => resultSet.getBytes(columnName).toSeq
      case JDBCType.VARBINARY => resultSet.getBytes(columnName).toSeq
      case JDBCType.LONGVARBINARY => resultSet.getBytes(columnName).toSeq
      case JDBCType.BLOB => resultSet.getBytes(columnName)

      case JDBCType.CHAR => resultSet.getString(columnName)
      case JDBCType.VARCHAR => resultSet.getString(columnName)
      case JDBCType.LONGVARCHAR => resultSet.getString(columnName)
      case JDBCType.CLOB => resultSet.getString(columnName)

      case JDBCType.NCHAR => resultSet.getString(columnName)
      case JDBCType.NVARCHAR => resultSet.getString(columnName)
      case JDBCType.LONGNVARCHAR => resultSet.getString(columnName)
      case JDBCType.NCLOB => resultSet.getString(columnName)

      case x =>
        throw new IllegalArgumentException(
          s"Cannot set unsupported JDBC type ${x.getName} for column $columnName.")
    }
  }

  private def getRowCursor(statement: PreparedStatement, metadata: ListMap[String, JDBCType]): VersatileCursor[Row] = {

    val resultSet = statement.getResultSet
    val row = new RowImpl(resultSet, metadata)

    new VersatileCursor[Row] {

      def next(): Boolean =
        resultSet.next()

      def get: Row =
        row
    }
  }

  private def getMetadata(statement: PreparedStatement): ListMap[String, JDBCType] = {

    val metadata = statement.getMetaData

    if (metadata == null)
      ListMap.empty
    else {

      val result =
        ListMap((for (i <- 1 to metadata.getColumnCount) yield
          (metadata.getColumnLabel(i).toLowerCase, JDBCType.valueOf(metadata.getColumnType(i)))): _*)

      if (result.size != metadata.getColumnCount)
        throw new IllegalArgumentException("Expect unique column names for resulting rows.")

      result
    }
  }

  private class RowImpl(resultSet: ResultSet, metadata: ListMap[String, JDBCType])
    extends Row {

    def apply[T](columnName: String)(implicit reader: FieldReader[T]): T = {

      val key = columnName.toLowerCase

      val jdbcType =
        metadata.get(key)
          .map(x => Success(x))
          .getOrElse(Failure(new IllegalArgumentException(s"Cannot find column $columnName.")))

      (for {
        value <- jdbcType
        result <- reader(columnName, getColumnValue(resultSet, key, value))
      } yield result).get
    }

    def toJson: JsonAst.JsonObject =
      JsonAst.JsonObject(metadata.map(x => (x._1, transformToJson(getColumnValue(resultSet, x._1, x._2)))))

    private def transformToJson(value: Any): JsonAst.JsonValue =
      value match {
        case null => JsonAst.JsonNull
        case x: Boolean => JsonAst.JsonBoolean(x)
        case x: Byte => JsonAst.JsonNumberByte(x)
        case x: Short => JsonAst.JsonNumberShort(x)
        case x: Integer => JsonAst.JsonNumberInt(x)
        case x: Long => JsonAst.JsonNumberLong(x)
        case x: Float => JsonAst.JsonNumberFloat(x)
        case x: Double => JsonAst.JsonNumberDouble(x)
        case x: Char => JsonAst.JsonString(x.toString)
        case x: String => JsonAst.JsonString(x)
        case x: BigDecimal => JsonAst.JsonNumberBigDecimal(x)
        case x: LocalDate => JsonAst.JsonString(x.toString)
        case x: LocalTime => JsonAst.JsonString(x.toString)
        case x: LocalDateTime => JsonAst.JsonString(x.toString)
        case x: Seq[_] if (x.forall(_.isInstanceOf[Byte])) =>
          JsonAst.JsonString(Hex.encode(x.map(_.asInstanceOf[Byte])))
        case _ =>
          throw new IllegalArgumentException(
            s"Cannot build JSON AST with value $value (${value.getClass.getName}).")
      }
  }

  private class QueryImpl(statement: String, parameters: Map[String, Any])
    extends Query {

    def this(statement: String) =
      this(statement, Map.empty)

    def on[T](key: String, value: T)(implicit writer: FieldWriter[T]): Query =
      new QueryImpl(statement, parameters + (key -> writer(key, value).get))

    def executeAndFetch(): Result =
      Executor.this.executeAndFetch(statement, parameters)

    def execute(): Unit =
      Executor.this.execute(statement, parameters)
  }
}

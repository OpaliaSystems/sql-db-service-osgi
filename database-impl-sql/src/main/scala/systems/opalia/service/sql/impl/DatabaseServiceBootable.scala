package systems.opalia.service.sql.impl

import org.apache.commons.dbcp2.BasicDataSource
import scala.collection.mutable
import systems.opalia.interfaces.database._
import systems.opalia.interfaces.logging.LoggingService
import systems.opalia.interfaces.module.Bootable


final class DatabaseServiceBootable(config: BundleConfig,
                                    loggingService: LoggingService)
  extends DatabaseService
    with Bootable[Unit, Unit] {

  private val logger = loggingService.newLogger(classOf[DatabaseService].getName)
  private val dataSource = new BasicDataSource()

  dataSource.setDriverClassName(config.driver)
  dataSource.setUrl(config.url)
  dataSource.setUsername(config.user)
  dataSource.setPassword(config.password)
  dataSource.setMinIdle(config.minIdle)
  dataSource.setMaxIdle(config.maxIdle)
  dataSource.setMaxTotal(config.maxTotal)
  dataSource.setMaxOpenPreparedStatements(config.maxOpenPreparedStatements)
  dataSource.setDefaultTransactionIsolation(config.isolationLevel)

  sys.props("org.jooq.no-logo") = "true"

  def withTransaction[T](block: (QueryFactory) => T): T = {

    val start = System.currentTimeMillis
    val connection = dataSource.getConnection
    val closeables = mutable.ListBuffer[OneTimeCloseable]()
    val executor = new Executor(logger, connection, useClosableTransaction = false, closeables)

    val result =
      try {

        connection.setAutoCommit(false)

        val result = block(executor.newQueryFactory())

        closeResources(closeables)
        connection.commit()

        result

      } catch {

        case e: Throwable => {

          closeResources(closeables)
          connection.rollback()

          throw e
        }

      } finally {

        connection.close()
      }

    val end = System.currentTimeMillis

    logger.trace(s"A transaction was performed in ${end - start} ms.")

    result
  }

  def createClosableTransaction(): ClosableTransaction = {

    val start = System.currentTimeMillis
    val connection = dataSource.getConnection
    val closeables = mutable.ListBuffer[OneTimeCloseable]()
    val executor = new Executor(logger, connection, useClosableTransaction = true, closeables)

    connection.setAutoCommit(false)

    new ClosableTransaction {

      def queryFactory: QueryFactory =
        executor.newQueryFactory()

      def close(): Unit = {

        try {

          closeResources(closeables)
          connection.commit()

        } finally {

          connection.close()
        }

        val end = System.currentTimeMillis

        logger.trace(s"A closable transaction was performed in ${end - start} ms.")
      }

      def close(cause: Exception): Unit = {

        try {

          closeResources(closeables)
          connection.rollback()

        } finally {

          connection.close()
        }

        throw cause
      }
    }
  }

  private def closeResources(closeables: mutable.ListBuffer[OneTimeCloseable]): Unit = {

    closeables.toList.foreach {
      closeable =>

        closeable.close()
        closeables -= closeable
    }
  }

  protected def setupTask(): Unit = {
  }

  protected def shutdownTask(): Unit = {

    dataSource.close()
  }
}

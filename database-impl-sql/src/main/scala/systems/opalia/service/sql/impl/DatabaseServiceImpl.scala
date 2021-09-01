package systems.opalia.service.sql.impl

import org.osgi.framework.BundleContext
import org.osgi.service.component.annotations._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import systems.opalia.interfaces.database._
import systems.opalia.interfaces.logging.LoggingService


@Component(service = Array(classOf[DatabaseService]), immediate = true, property = Array("database=sql"))
class DatabaseServiceImpl
  extends DatabaseService {

  private var bootable: DatabaseServiceBootable = _

  @Reference
  private var loggingService: LoggingService = _

  @Activate
  def start(bundleContext: BundleContext): Unit = {

    bootable =
      new DatabaseServiceBootable(
        new BundleConfig(),
        loggingService,
      )

    bootable.setup()
    Await.result(bootable.awaitUp(), Duration.Inf)
  }

  @Deactivate
  def stop(bundleContext: BundleContext): Unit = {

    bootable.shutdown()
    Await.result(bootable.awaitUp(), Duration.Inf)

    bootable = null
  }

  def withTransaction[T](block: (QueryFactory) => T): T =
    bootable.withTransaction(block)

  def createClosableTransaction(): ClosableTransaction =
    bootable.createClosableTransaction()
}

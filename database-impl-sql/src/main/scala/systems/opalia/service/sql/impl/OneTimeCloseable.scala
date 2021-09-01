package systems.opalia.service.sql.impl

import java.io.Closeable


final class OneTimeCloseable(closeable: AutoCloseable)
  extends Closeable {

  private var closed = false

  def close(): Unit = {

    if (!closed) {

      closeable.close()
      closed = true
    }
  }
}

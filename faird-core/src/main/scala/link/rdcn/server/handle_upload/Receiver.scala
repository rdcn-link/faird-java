package link.rdcn.server.handle_upload

trait Receiver extends AutoCloseable {
  /** Called once before receiving any rows */
  def start(schema: org.apache.arrow.vector.types.pojo.Schema): Unit

  /** Called for each received batch of rows */
  def receiveRow(root: org.apache.arrow.vector.VectorSchemaRoot): Unit

  /** Called after all batches are received successfully */
  def finish(): Unit

  /** Called on error to clean up */
  override def close(): Unit = {}
}


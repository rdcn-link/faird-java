package link.rdcn.server.handle_upload

import java.io.File
import java.nio.channels.Channels
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.pojo.Schema

class FileStorageReceiver(file: File) extends Receiver {
  private var fos: java.io.FileOutputStream = _
  private var channel: java.nio.channels.WritableByteChannel = _
  private var writer: ArrowFileWriter = _

  override def start(schema: Schema): Unit = {
    fos = new java.io.FileOutputStream(file)
    channel = Channels.newChannel(fos)
    // Arrow Writer 会在接收到 root 后再赋值
  }

  override def receiveRow(root: VectorSchemaRoot): Unit = {
    if (writer == null) {
      writer = new ArrowFileWriter(root, null, channel)
      writer.start()
    }
    writer.writeBatch()
  }

  override def finish(): Unit = {
    if (writer != null) {
      writer.end()
      writer.close()
      writer = null
    }
  }

  override def close(): Unit = {
    if (fos != null) fos.close()
    if (channel != null) channel.close()
  }
}

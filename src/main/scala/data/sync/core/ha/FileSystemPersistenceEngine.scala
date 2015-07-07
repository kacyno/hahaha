package data.sync.core.ha

import java.io._
import akka.serialization.Serialization
import data.sync.common.Logging
import scala.reflect.ClassTag


class FileSystemPersistenceEngine(
    val dir: String,
    val serialization: Serialization)
  extends PersistenceEngine with Logging {

  new File(dir).mkdir()

  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(new File(dir + File.separator + name), obj)
  }

  override def unpersist(name: String): Unit = {
    new File(dir + File.separator + name).delete()
  }

  override def read[T: ClassTag](prefix: String) = {
    val files = new File(dir).listFiles().filter(_.getName.startsWith(prefix))
    files.map(deserializeFromFile[T])
  }

  private def serializeIntoFile(file: File, value: AnyRef) {
    val created = file.createNewFile()
    if (!created) { throw new IllegalStateException("Could not create file: " + file) }
    val serializer = serialization.findSerializerFor(value)
    val serialized = serializer.toBinary(value)
    val out = new FileOutputStream(file)
    try {
      out.write(serialized)
    } finally {
      out.close()
    }
  }

  private def deserializeFromFile[T](file: File)(implicit m: ClassTag[T]): T = {
    val fileData = new Array[Byte](file.length().asInstanceOf[Int])
    val dis = new DataInputStream(new FileInputStream(file))
    try {
      dis.readFully(fileData)
    } finally {
      dis.close()
    }
    val clazz = m.runtimeClass.asInstanceOf[Class[T]]
    val serializer = serialization.serializerFor(clazz)
    serializer.fromBinary(fileData).asInstanceOf[T]
  }

}

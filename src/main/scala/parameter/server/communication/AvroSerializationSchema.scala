package parameter.server.communication

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.java.typeutils.TypeExtractor

class AvroSerializationSchema[T] extends SerializationSchema[T] with DeserializationSchema[T] {

  val avroType: Class[T] = ???

  private val reader = null
  private val decoder = null
  private val writer = null
  private val encoder = null
  private val obj = null

  override def serialize(element: T) = {
    def serialize(elem: Any) = {
      obj = elem.asInstanceOf[Nothing]
      ensureWriterInitialized()
      // TODO Auto-generated method stub
      //return SerializationUtils.serialize((Serializable) obj);
      val out = new Nothing
      encoder = EncoderFactory.get.binaryEncoder(out, null)
      var serializedBytes = null
      try {
        writer.write(obj, encoder)
        encoder.flush
        serializedBytes = out.toByteArray
        out.close
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
      serializedBytes
    }
  }

  override def isEndOfStream(nextElement: T) = false

  override def deserialize(message: Array[Byte]) = {
    ensureReaderInitialized()

    try {
      decoder = DecoderFactory.get.binaryDecoder(message, decoder)
      return reader.read(null, decoder)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        //throw new RuntimeException(e)
    }
  }

  override def getProducedType = {
    TypeExtractor.getForClass(avroType)
  }

  private def ensureReaderInitialized(): Unit = {
    if (reader == null) if (classOf[Nothing].isAssignableFrom(avroType)) reader = new Nothing(avroType)
    else reader = new Nothing(avroType)
  }

  private def ensureWriterInitialized(): Unit = {
    if (writer == null) if (classOf[Nothing].isAssignableFrom(avroType)) {
      writer = new Nothing(avroType)
      //if(obj instanceof GenericRecord) {
      //	writer = new GenericDatumWriter(((GenericRecord)obj).getSchema());
      //}else {
      //	writer = new SpecificDatumWriter<T>(avroType);
      //}
    }
    else writer = new Nothing(avroType)
  }

}

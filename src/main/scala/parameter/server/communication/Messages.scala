package parameter.server.communication

import parameter.server.utils.Types.Parameter

object Messages {

  abstract class Message[K1, K2, M](src: K1, dest: K2, msg: Option[M]){
    def source: K1 = src
    def destination: K2= dest
    def message: Option[M] = msg
  }

  case class Push[WK, SK, P <: Parameter](src: WK, dest: SK, msg: P) extends Message(src, dest, Some(msg)) {
    override def toString: String =
      s"Push:$src:$dest:$msg"
  }
  case class Pull[WK, SK, P <: Parameter](src: WK, dest: SK) extends Message[WK, SK, P](src, dest, None) {
    override def toString: String =
      s"Pull:$src:$dest"
  }
  case class PullAnswer[WK, SK, P <: Parameter](src: SK, dest: WK, msg: P) extends Message(src, dest, Some(msg)) {
    override def toString: String =
      s"$src:$dest:$msg"
  }

  class NotSupportedWorkerInput extends Exception
  class NotSupportedMessage extends Exception
  class NotSupportedOutput extends Exception
}

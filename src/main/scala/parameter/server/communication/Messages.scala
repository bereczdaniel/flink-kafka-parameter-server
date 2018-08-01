package parameter.server.communication

import parameter.server.utils.Types.{Parameter, ParameterUpdate}

object Messages {

  abstract class Message(src: AnyVal, dest: AnyVal, msg: Option[Any]){
    def source: AnyVal = src
    def destination: AnyVal = dest
    def message: Option[Any] = msg
  }

  case class Push(src: AnyVal, dest: AnyVal, msg: ParameterUpdate) extends Message(src, dest, Some(msg)) {
    override def toString: String =
      s"Push:$src:$dest:$msg"
  }
  case class Pull(src: AnyVal, dest: AnyVal) extends Message(src, dest, None) {
    override def toString: String =
      s"Pull:$src:$dest"
  }
  case class PullAnswer(src: AnyVal, dest: AnyVal, msg: Parameter) extends Message(src, dest, Some(msg)) {
    override def toString: String =
      s"$src:$dest:$msg"
  }

  class NotSupportedWorkerInput extends Exception
  class NotSupportedMessage extends Exception
  class NotSupportedOutput extends Exception
}

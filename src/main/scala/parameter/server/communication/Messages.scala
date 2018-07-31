package parameter.server.communication

import parameter.server.utils.Types.{Parameter, ParameterUpdate}

object Messages {

  abstract class Message(src: AnyVal, dest: AnyVal, msg: Option[AnyVal]){
    def source: AnyVal = src
    def destination: AnyVal = dest
    def message: Option[AnyVal] = msg
  }

  case class Push(src: AnyVal, dest: AnyVal, msg: ParameterUpdate) extends Message(src, dest, Some(msg))
  case class Pull(src: AnyVal, dest: AnyVal, msg: Parameter) extends Message(src, dest, Some(msg))

  class NotSupportedWorkerInput extends Exception
  class NotSupportedMessage extends Exception
  class NotSupportedOutput extends Exception
}

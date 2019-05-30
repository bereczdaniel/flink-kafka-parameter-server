package parameter.server.utils

object Types {

  // Represents the input of the parameter server
  abstract class WorkerInput(val destination: AnyVal)

  trait ParameterServerOutput


}

package parameter.server.utils

object Types {

  // Represents an arbitrary element of a machine learning model (e.g. weight of feature)
  trait Parameter

  // Represents the input of the parameter server
  abstract class WorkerInput(val destination: AnyVal)

  trait ParameterServerOutput


}

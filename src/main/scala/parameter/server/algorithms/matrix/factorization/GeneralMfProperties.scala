package parameter.server.algorithms.matrix.factorization

case class GeneralMfProperties(learningRate: Double, numFactors: Int, negativeSampleRate: Int, rangeMin: Double, rangeMax: Double,
                     workerK: Int, bucketSize: Int) {

}

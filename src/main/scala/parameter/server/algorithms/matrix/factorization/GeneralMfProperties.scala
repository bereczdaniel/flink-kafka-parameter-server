package parameter.server.algorithms.matrix.factorization

case class GeneralMfProperties(learningRate: Double, numFactors: Int, negativeSampleRate: Int, randomInitRangeMin: Double, randomInitRangeMax: Double,
                               workerK: Int, bucketSize: Int) {

}

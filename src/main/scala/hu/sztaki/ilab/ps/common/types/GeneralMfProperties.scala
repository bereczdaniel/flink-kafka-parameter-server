package hu.sztaki.ilab.ps.common.types

case class GeneralMfProperties(learningRate: Double, numFactors: Int, negativeSampleRate: Int, randomInitRangeMin: Double,
                               randomInitRangeMax: Double, workerK: Int, bucketSize: Int, memorySize: Int)

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import StreamingLinearRegressionWithSGD
import sys



if __name__ == "__main__":
    if len(sys.argv) != 2:
        exit(-1)

    sc = SparkContext(appName="PythonLogisticRegressionWithSGDExample")
    ssc = StreamingContext(sc, 100)

    # $example on$
    def parse(lp):
        label = float(lp[lp.find('(') + 1: lp.find(',')])
        vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))
        return LabeledPoint(label, vec)

    trainingData = ssc.textFileStream(sys.argv[1]).map(parse).cache()
    testData = ssc.textFileStream(sys.argv[1]).map(parse)

    numFeatures = 54
    model = StreamingLinearRegressionWithSGD()
    model.setInitialWeights([float(0) for i in range(54)])

    model.trainOn(trainingData)
    print(model.predictOnValues(testData.map(lambda lp: (lp.label, lp.features))))

    ssc.start()
    ssc.awaitTermination()
    print "weights!!!", repr(model._model.weights)
    print "END!"
    # $example off$
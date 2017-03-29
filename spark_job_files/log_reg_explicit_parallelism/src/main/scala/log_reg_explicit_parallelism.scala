
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.util.MLUtils

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
//import org.apache.spark.mllib.regression.{RidgeRegressionWithSGD, RidgeRegressionModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.ml.util.MLWriter
import org.apache.spark.storage.StorageLevel._

import sys.process.stringSeqToProcess


object LogRegExplicitParallelism {

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def main(args: Array[String]) {
    val t1 = System.nanoTime

    val cluster_type=args(0)
    val num_features=args(1)
    var file_path=""
    var save_file_path=""
    var time_path=""
    var iter_str=""
    var explicit_parallelism=""
    if ( cluster_type == "cluster" ) {
        val host_name=args(2)
        val host_port=args(3)
        val file_name=args(4)
        iter_str=args(5)
        explicit_parallelism=args(6)
        file_path="hdfs://" + host_name + ":" + host_port + "/" + file_name
        save_file_path="file:///home/ubuntu/model/scalaSVMWithSGDModel"
        time_path="/home/ubuntu/"
    } else {
        val data_set=args(2)
        iter_str =args(3)
        file_path="/Users/Kevin/MLSchedule/" + data_set
        save_file_path="file:///Users/Kevin/MLSchedule/tmp_model_save"
        time_path=""
    }
    val iterations = iter_str.toInt
    val conf = new SparkConf().setAppName("MlTest").set("spark.shuffle.blockTransferService", "nio")

    var warmstart_file_path=file_path + "_warmstart"


    val sc = new SparkContext(conf)

    val t2 = System.nanoTime

    val data = MLUtils.loadLibSVMFile(sc, file_path, num_features.toInt, explicit_parallelism.toInt).persist(MEMORY_ONLY)
    //val data = MLUtils.loadLibSVMFile(sc, file_path, num_features.toInt, explicit_parallelism.toInt)

    val t3 = System.nanoTime

    val initialWeightsVec = Vectors.zeros(num_features.toInt)

    val t4 = System.nanoTime


    val setup_model = new LogisticRegressionWithSGD()
    setup_model.optimizer.setNumIterations(iterations)
    setup_model.optimizer.setConvergenceTol(0)

    // val setup_model = new LogisticRegressionWithLBFGS()
    // setup_model.optimizer.setNumIterations(iterations)
    // setup_model.optimizer.setConvergenceTol(0)
    // setup_model.optimizer.setNumCorrections(iterations)


    val t5 = System.nanoTime

    val model = setup_model.run(data, initialWeightsVec)

    val t6 = System.nanoTime



    val setup = (t2 - t1) / 1e9d
    val load_data = (t3 - t2) / 1e9d
    val initial_weights = (t4 - t3) / 1e9d
    val model_setup = (t5 - t4) / 1e9d
    val all_iters = (t6 - t5) / 1e9d

    val times_to_print = Array(setup, load_data, initial_weights, model_setup, all_iters)
    printToFile(new java.io.File(time_path + "time_file")) { p =>
      times_to_print.foreach(p.println)
    }
    sc.stop()
  }
}

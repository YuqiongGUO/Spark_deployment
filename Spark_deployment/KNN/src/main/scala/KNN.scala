// package gyq.KNN

import math._
import scala.util.Try
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Spark_Knn {

    def main(argv: Array[String]) = {

        if (argv.length < 2) {
            println("Wrong number of arguments")
            System.exit(1)
        }

        val pathTrain = argv(0)
        val K = argv(1).toInt

        //Basic setup
        val jobName = "Joanna - KNN -> K = " + K
        var startTime = System.currentTimeMillis
        //Spark Configuration
        val conf = new SparkConf().setAppName(jobName)
        val sc = new SparkContext(conf)

        val broadcastK = sc.broadcast(K)
        val train = sc.textFile(pathTrain: String)
                                .map(
                                        _.split(",")
                                        .map(v => Try(v.toDouble).getOrElse(0.0))
                                    )
        

        val test = train.zipWithIndex.map{case (k,v) => (v,k)}

        val cart = test.cartesian(train)
        
        val mapped = cart.map{case (testValue, trainValue) => { 
                val testId = testValue._1
                val testTokens = testValue._2
                val trainTokens = trainValue

                val distance = euclideanDistance(testTokens, trainTokens) 
                val classification = trainValue.last.toInt

                (testId, (distance, classification))
            }
        }
    
        val grouped = mapped.aggregateByKey(List[Tuple2[Double,Int]]())(
            (aggr, value) => aggr ::: (value :: Nil),
            (aggr1, aggr2) => aggr1 ::: aggr2
        )

        val output = grouped.mapValues(neighbors => {
            val k = broadcastK.value 
            val nearestK = findNearest(neighbors, k)
            val majority = buildClassification(nearestK)
            val selectedClassification = majority.valuesIterator.max

            selectedClassification
        })

        var runTime = System.currentTimeMillis - startTime

        println("="*25)
        println("%, runtime "+ runTime + "ms")
        println("="*25)
        //output.saveAsTextFile("./output");
    }

    def euclideanDistance(test: Array[Double], train: Array[Double]) = {
        sqrt(
            (train.dropRight(1) zip test.dropRight(1))
                    .map { case (x,y) => pow(y - x, 2) }
                    .sum
        )
    }

    def findNearest(neighbors: Iterable[Tuple2[Double,Int]], k: Int): Map[Double,Int] = {
        var nearest = new HashMap[Double,Int]() 
        neighbors.foreach{ case (distance, classification) =>

            nearest += (distance -> classification)

            if (nearest.size > k) {
                nearest.drop(k)
            }      
        }

        return nearest
    }

    def buildClassification(nearest: Map[Double,Int]): Map[Int,Int] = {
        var majority = HashMap[Int,Int]()
        
        nearest.foreach { case (distance, classification) =>
            if (majority contains classification) {
                majority(classification) = majority(classification) + 1
            } else {
                majority(classification) = 1
            }
        }

        return majority
    }
} 

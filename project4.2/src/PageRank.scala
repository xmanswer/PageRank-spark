--driver-memory 10G --executor-memory 20G

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

val conf = new SparkConf().
set("spark.driver.memory", "12g").
set("spark.executor.memory", "20g").
set("spark.driver.cores", "4").
set("spark.executor.cores", "24")

val sc = new SparkContext(conf)


//val file = sc.textFile("hdfs:///input/input")
//var links = file.map(line => (line.split(" ")(0), line.split(" ")(1))).distinct.groupByKey()

val file = sc.textFile("s3n://f15-p42/twitter-graph.txt")
val edges = file.map(line => (line.split(" ")(0), Set(line.split(" ")(1)))).reduceByKey(_ ++ _)
val edges0 = file.flatMap(line => line.split(" ")).distinct.map(id => (id, Set.empty[String]))
val allEdges = edges.union(edges0).reduceByKey(_ ++ _).map(f => (f._1, f._2.toList))
var scores = file.flatMap(line => line.split(" ")).distinct.map(id => (id, 1.0))

allEdges.persist()
for (i <- 1 to 10) {  
            val dl = sc.accumulator(0.0)  
            val followeeScores = allEdges.join(scores).flatMap {  
                case (id, (followees, score)) => { 
                    val followeeSize = followees.size
                    if (followeeSize == 0) dl += score
                    followees.map(followees => (followees, score/followeeSize)) 
                } 
            }
            followeeScores.count()
            val v = dl.value
            scores = followeeScores.reduceByKey(_ + _).map(s => (s._1, 0.15 + 0.85 * (s._2 + v/2546953)))
            scores.coalesce(128)
}
scores.map{case(x,y) => x + "\t" + y}.saveAsTextFile("hdfs:///output")   

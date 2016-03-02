class FindEdgesVertices {
  val file = sc.textFile("s3n://f15-p42/twitter-graph.txt")
  val file2 = sc.textFile("hdfs:///input")
  val vertCounts = file.flatMap(line => line.split(" ")).distinct.count
  val edgeCounts =  file.map(line => (line.split(" ")(0), line.split(" ")(1))).distinct.count
  
  val file = sc.textFile("s3n://f15-p42/twitter-graph.txt")
  val followerCount0 = file.flatMap(line => line.split(" ")).distinct.map(id => (id, 0))
  val followerCounts = file.map(line => (line.split(" ")(0), line.split(" ")(1))).distinct.map(_.swap).map{case (k,v) => (k,1)}.union(followerCount0).reduceByKey(_ + _).map(f => f._1+"\t"+f._2)
  followerCounts.saveAsTextFile("hdfs:///output2")
}

2546953
627475356
627475664

LOAD DATA INFILE 'task2_output' INTO TABLE follower FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n';

val fike = sc.textFile("s3n://f15-p42/twitter-graph.txt") // RDD of (url, neighbors) pairs
val edges = file.map(line => (line.split(" ")(0), line.split(" ")(1))).distinct
var ranks = // RDD of (url, rank) pairs
for (i <- 1 to 10) {
 val contribs = links.join(ranks).flatMap {
 case (url, (links, rank)) =>
 links.map(dest => (dest, rank/links.size))
 }
 ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
}
ranks.saveAsTextFile(...)


The IP of the master is: 172.31.7.20
The IP list of Samza brokers in the cluster is given below for your reference. Copy it for pasting into .properties file!
172.31.2.128:9092,172.31.15.209:9092,172.31.7.20:9092

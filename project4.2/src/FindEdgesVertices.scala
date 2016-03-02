object FindEdgesVertices {
  val file = sc.textFile("s3n://f15-p42/twitter-graph.txt")
  val file2 = sc.textFile("hdfs:///input")
  val vertCounts = file.flatMap(line => line.split(" ")).distinct.count
  val edgeCounts =  file.map(line => (line.split(" ")(0), line.split(" ")(1))).distinct.count
  
  val file = sc.textFile("s3n://f15-p42/twitter-graph.txt")
  val followerCount0 = file.flatMap(line => line.split(" ")).distinct.map(id => (id, 0))
  val followerCounts = file.map(line => (line.split(" ")(0), line.split(" ")(1))).distinct.map(_.swap).map{case (k,v) => (k,1)}.union(followerCount0).reduceByKey(_ + _).map(f => f._1+"\t"+f._2)
  followerCounts.saveAsTextFile("hdfs:///output2")
}
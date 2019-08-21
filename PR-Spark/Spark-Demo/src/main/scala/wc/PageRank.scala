package wc

import javax.annotation.Resource
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object PageRank {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.PageRank <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Page Rank")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================

    val k: Int = args(0).toInt
    val iters: Int = 10
    val maxK: Int = k*k
    val initialRank: Float = (1/maxK.toFloat)
    val vertices = List.range(1,maxK+1)
    val graph = sc.parallelize(vertices.map(vertex =>{
      if(vertex%k==0)
        (vertex,0)
      else
        (vertex,vertex+1)
    }))

    val dummyRDD = List(0).map(x=>(x,0.0f))
    val verRanks = vertices.map(vertex=>(vertex,initialRank))
    var ranks = sc.parallelize(verRanks.union(dummyRDD))
    //ranks.persist()

    // Code inspired from http://www.ccs.neu.edu/home/mirek/code/SparkPageRank.scala

    for (i <- 1 to iters) {
      val contribs = graph.join(ranks)
        .values.map{
        case(u,v)=>(u,v)
      }


      val combineRanks = contribs.reduceByKey(_ + _)
      val dummyRank = combineRanks.lookup(0).head.toFloat
      val PRMass = dummyRank/maxK


      val nextRanks  = ranks.leftOuterJoin(combineRanks).map({
        case (k,(u,None)) =>
          {
            if(k==0)
              (k,0.0f)
            else
              (k,PRMass)
          }

        case (k,(u,Some(v))) =>
          {
            if(k==0)
              (k,0.0f)
            else
              (k,v+PRMass)
          }


      })

      ranks = nextRanks
      var pagerankvalue = ranks.values.sum()
      logger.info("Page Rank Value at-"+i+" iteration is "+pagerankvalue)

    }
    logger.info(ranks.toDebugString)
    val output = ranks.filter(_._1<=100)
    output.saveAsTextFile(args(1))

  }
}

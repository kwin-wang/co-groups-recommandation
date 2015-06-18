package recommend

import java.io.PrintWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable
/**
 * Created by yangqing on 3/24/15.
 */
object CollaborateFilterForBaoz extends App{
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val conf = new SparkConf().setAppName("spark_word2vector").setMaster("local")
    .set("spark.executor.memory", "4g")

  val sc = new SparkContext(conf)
  val data = sc.textFile("Fri Mar 20 2015 15_16_03 GMT+0800 (CST).csv")
  val group_user = mutable.Map.empty[Long, Set[Long]].withDefaultValue(Set.empty[Long])
  data.foreach(_.split(",") match {
    case Array(user, group, score) =>
      group_user(group.toLong) += user.toLong
    case _ =>
  })

  println("group_user load ok!")
  val usedKey = mutable.Set.empty[Long]
  val relationGroups = mutable.Map.empty[Long, List[(Long, Double)]].withDefaultValue(List.empty[(Long, Double)])
  val valueThreashold = 0.01
  val total_cal = (group_user.size * group_user.size / 2).toDouble
  println(s"total count is $total_cal")
  var count = 0
  group_user.keys.foreach {group =>
//    println(s"$group for ${usedKey.mkString(",")}")
//    println(s"current group is $group")
    usedKey += group
    group_user.keys.filter(!usedKey.contains(_)).foreach {unused_group =>
      count+=1
      if (count % 100 == 0)
        println(s"current procedure:\t ${count / total_cal * 100} %")
      // distance between group_user(group) group_user(unused_group)
      val molecular = (group_user(group) & group_user(unused_group)).size
      val denominator = (group_user(group) ++ group_user(unused_group)).size.toDouble
      val rate = molecular / denominator
      if (rate > valueThreashold)
        relationGroups(group) = (unused_group, rate)::relationGroups(group)
    }
  }

  val out = new PrintWriter("relation_groups.txt")
  relationGroups.foreach{group =>
    out.println(s"${group._1} => ${group._2.sortBy(-_._2).take(12).map(_._1).mkString(",")}")
    out.flush()
  }
  out.close()
}

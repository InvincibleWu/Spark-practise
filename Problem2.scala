package comp9313.ass3

import org.apache.spark.SparkConf  
import org.apache.spark.SparkContext  
import Array._

object Problem2 {
  def main(args:Array[String]){  
   // set spark conf
   val conf = new SparkConf().setAppName("Reverse Graph edge direction").setMaster("local")  
   val sc = new SparkContext(conf)  

   // read input file
   val srcData = sc.textFile(args(0)) 

   // split by line
   val line = srcData.flatMap(line => line.split("\n"))

   // for each line or edge
   //   1. make the target nodeID to key, and then store the source nodeID in an ArrayList as the key
   //   2. reduceByKey, combine and sort all the ArrayList with the same key
   // example:
   //            map            reduceByKey                   sort
   //   "1  2"   =>  (2, [1])       =>      (2, [1])           =>   (2, [1])
   //   "1  3"   =>  (3, [1])       =>      (3, [2, 4, 1])     =>   (3, [1, 2, 4])
   //   "2  1"   =>  (1, [2])       =>      (1, [2])           =>   (1, [2])
   //   "2  3"   =>  (3, [2])       =>      
   //   "4  3"   =>  (3, [4])       =>      
   val reverse = line.map(edge => (edge.split("\t")(1).toInt, Array[Int](edge.split("\t")(0).toInt))).reduceByKey((a, b) =>concat(a, b).sorted)
   
   // sortByKey
   val sortKey = reverse.sortByKey(true)

   // convert ArrayList to String by using "combine" function
   val transform = sortKey.map{ case (k, v) => (k, combine(v)) }

   // convert to output format
   val finalOutput = transform.map{ case (k, v) => "" + k + "\t" + v}

   // save file
   finalOutput.saveAsTextFile(args(1))
 } 
 
  // a function used to convert ArrayList to String
  // input an ArrayList
  // output a String
  // example:
  //  [1, 2, 3, 4, 5]   =>    "1,2,3,4,5"
  def combine(t:Array[Int]) : String = {
    var output:String = ""
    for( i <- 0 to (t.length-2)){
      output += t(i) + ","
    }
    output += t(t.length - 1)
    return output
  }
}
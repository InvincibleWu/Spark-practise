package comp9313.ass3

import org.apache.spark.SparkConf  
import org.apache.spark.SparkContext  

object Problem1 {
  def main(args:Array[String]){  
   // set spark conf
   val conf = new SparkConf().setAppName("Find top-k terms").setMaster("local")  
   val sc = new SparkContext(conf)

   // read input file
   val srcData = sc.textFile(args(0)) 

   // every word appear several times only count once
   // use split and sidtinct to get that
   // example:
   //    "apple apple apple banana banada" => "apple", "banana"
   val splitLine = srcData.flatMap(line => line.toLowerCase().split("[\\s*$&#/\"'\\,.:;?!\\[\\](){}<>~\\-_]+").distinct)

   // combine the result
   // exaple:
   //    "banada", "banana"
   //    "apple", "apple", "apple"
   // after map:
   //    (banana, 1), (banana, 1) => (banana, 2)
   //    (apple, 1), (apple, 1), (apple) => (apple, 3)
   // after sortByKey:
   //    (apple, 1), (apple, 1), (apple) => (apple, 3)
   //    (banana, 1), (banana, 1) => (banana)
   val countedData = splitLine.map(word => (word, 1)).reduceByKey((a,b) => a+b).sortByKey(true)

   // reverse the key and value and sort by new key
   val sortedData = countedData.map{ case (k,v) => (v,k) }.sortByKey(false)

   // convert to the output format
   val output = sortedData.map{case (v, k) => ""+k+"\t"+v}.filter { word => word.charAt(0).isLetter }.take(args(2).toInt)
   val file = sc.parallelize(output, 1)

   // save file
   file.saveAsTextFile(args(1))
 }  
}
package S3Reader

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

case class RDDHelper() {
  def load_rdd_aws(s3_path: String): RDD[String] = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext.textFile(s3_path)
  }

  def filter_curly_brackets(rdd: RDD[String]): RDD[String] = {
     rdd.filter(f => !f.equals("{") && !f.equals("}"))
  }

  def split_two_elements_by_colon(rdd: RDD[String]) = {
    rdd.map(x => x.split(":")).map(x => Element(x(0).strip(),x(1).replace(",","")))
  }

  def filter_by_key(rdd: RDD[Element], key: String) = {
    rdd.filter(x => x.key.equals(key)).map(x => x.value)
  }

  // zip the RDDs into an RDD of Seq[Int]
  def makeZip(s: Seq[RDD[String]]): RDD[Seq[String]] = {
    if (s.length == 1)
      s.head.map(e => Seq(e))
    else {
      val others = makeZip(s.tail)
      val all = s.head.zip(others)
      all.map(elem => Seq(elem._1) ++ elem._2)
    }
  }

  // zip and apply arbitrary function from Seq[Int] to Int
  def applyFuncToZip(s: Seq[RDD[String]], f:Seq[String] => String): RDD[String] = {
    val z = makeZip(s)
    z.map(f)
  }
}

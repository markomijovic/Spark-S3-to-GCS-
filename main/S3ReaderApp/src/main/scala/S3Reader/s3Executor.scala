package S3Reader

import org.apache.spark.rdd.RDD
//  ref https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/
object s3Executor {
  def execute(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val accessKeyID = args(0) //  accessKeyID
    val secretAccessKey = args(1) //  secretAccessKey
    val s3Path = args(2) // s3Path
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkByExamples.com")
      .getOrCreate()
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", accessKeyID)
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", secretAccessKey)
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("google.cloud.auth.service.account.email", System.getenv("ACCOUNT_EMAIL"))
    spark.conf.set("google.cloud.auth.service.account.keyfile", System.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    val rdd_helper = new RDDHelper();
    val s3RDD = rdd_helper.load_rdd_aws(s3Path);

    val temp = rdd_helper.filter_curly_brackets(s3RDD);
    val rdd_final = rdd_helper.split_two_elements_by_colon(temp);
    val rdd1 = rdd_helper.filter_by_key(rdd_final, "\"order_id\"");
    val rdd2 = rdd_helper.filter_by_key(rdd_final, "\"custo_id\"");
    val rdd3 = rdd_helper.filter_by_key(rdd_final, "\"order.item_grp_id\"")
    val rdd4 = rdd_helper.filter_by_key(rdd_final, "\"order.qty\"");
    val allRDDs = Seq(rdd1, rdd2, rdd3, rdd4)
    val res = rdd_helper.applyFuncToZip(allRDDs, (s: Seq[String]) => s.toString())
    res.foreach(s => println(s)) // TODO: construct an object with right format and save to gcs instead of println
  }

  def main(args: Array[String]): Unit = {
    // define commons, best to get credentials from environment variables
    val accessKeyID = System.getenv("ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("SECRET_ACCESS_KEY")
    val s3Path = System.getenv("S3_PATH")
    execute(Array(accessKeyID,
      secretAccessKey,
      s3Path
    ))
  }
}

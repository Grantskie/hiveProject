import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine
import scala.io.StdIn.readInt

object hiveObject {

  def generateTables(spark:SparkSession): Unit ={
    spark.sql("CREATE TABLE IF NOT EXISTS bev_brancha(type STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE bev_brancha")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_branchb(type STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE bev_branchb")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_branchc(type STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE bev_branchc")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_branchd(type STRING, branch STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchD.txt' INTO TABLE bev_branchd")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_conscounta(type STRING, amount INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE bev_conscounta")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_conscountb(type STRING, amount INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE bev_conscountb")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_conscountc(type STRING, amount INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE bev_conscountc")
  }

  def userInputCheck(input:String): Int ={
    var retInt : Int = 0
    var tryInt : Boolean = false
    try{input.toInt}
    catch{case e: Exception => tryInt = true}

    if(input == "<") retInt = 0         //exit input
    else if(tryInt == true) retInt = 1 //wrong input
    else retInt = 2                    //correct input

    return retInt
  }

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    println("created spark session")
    println("Generating tables")
    generateTables(spark)
    println("Tables Successfully Generated")
    var userInput: String = null
    do{
      Aesthetics.printHeader("Menu")
      Aesthetics.printBorderVert(1)
      for(i<-1 to 6) Aesthetics.printBorderVert(s"$i) Problem Scenario $i")
      Aesthetics.printBorderVert("7) Extras")
      Aesthetics.printBorderVert(1)
      Aesthetics.printHeader("< to exit")
      userInput = readLine(">Input<")
      val test = userInputCheck(userInput)
      if(test == 2) {
        userInput.toInt match {
          case 1 => "zero"
          case 2 => "one"
          case 3 => "two"
          case 4 => "a"
          case 5 => "a"
          case 6 => "a"
          case 7 => "a"
          case _ => "other"
        }
      }
    }while(userInput != "<")

    //spark.sql("select * from bev_brancha").show()
    //spark.sql("select * from bev_branchb").show()
    //spark.sql("select * from bev_branchc").show()
    //spark.sql("select * from bev_branchd").show()
    //spark.sql("select * from bev_conscounta").show()
    //spark.sql("select * from bev_conscountb").show()
    //spark.sql("select * from bev_conscountc").show()
  }
}

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

  def dropTables(spark:SparkSession){
    spark.sql("DROP TABLE bev_brancha")
    spark.sql("DROP TABLE bev_branchb")
    spark.sql("DROP TABLE bev_branchc")
    spark.sql("DROP TABLE bev_branchd")
    spark.sql("DROP TABLE bev_conscounta")
    spark.sql("DROP TABLE bev_conscountb")
    spark.sql("DROP TABLE bev_conscountc")
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

  def problemScenarioOne(spark:SparkSession){
    var userInput : String = ""
    do{
      Aesthetics.printHeader("Total # of consumers in branch1")
      spark.sql("SELECT SUM(conscount.a) FROM bev_brancha INNER JOIN (SELECT type as t, amount as a FROM bev_conscounta UNION ALL SELECT * FROM bev_conscountb UNION ALL SELECT * FROM bev_conscountc) as conscount  ON bev_brancha.type = conscount.t WHERE branch = \'Branch1\'").show
      Aesthetics.printHeader("Total # of consumers in branch2")
      spark.sql(raw"SELECT SUM(conscount.a) FROM (SELECT * FROM bev_brancha UNION ALL SELECT * FROM bev_branchc) AS bev_branch INNER JOIN (SELECT type as t, amount as a FROM bev_conscounta UNION ALL SELECT * FROM bev_conscountb UNION ALL SELECT * FROM bev_conscountc) as conscount ON bev_branch.type = conscount.t WHERE branch = 'Branch2'").show
      Aesthetics.printHeader("< to go back")
      userInput = readLine(">Input<")
    }while(userInput != "<")
  }

  def problemScenarioTwo(spark:SparkSession){
    var userInput:String = ""
    do{
      Aesthetics.printHeader("Most consumed beverage on Branch1")
      spark.sql(raw"SELECT t as MaxType, SUM(conscount.a) as MaxTotal FROM bev_brancha INNER JOIN (SELECT type as t, amount as a FROM bev_conscounta UNION ALL SELECT * FROM bev_conscountb UNION ALL SELECT * FROM bev_conscountc) as conscount  ON bev_brancha.type = conscount.t WHERE branch = 'Branch1' GROUP BY conscount.t ORDER BY MaxTotal desc LIMIT 1").show
      Aesthetics.printHeader("Least consumed beverage on Branch2")
      spark.sql(raw"SELECT t as MaxType, SUM(conscount.a) as MaxTotal FROM (SELECT * FROM bev_brancha UNION ALL SELECT * FROM bev_branchc) AS bev_branch INNER JOIN (SELECT type as t, amount as a FROM bev_conscounta UNION ALL SELECT * FROM bev_conscountb UNION ALL SELECT * FROM bev_conscountc) as conscount ON bev_branch.type = conscount.t WHERE branch = 'Branch2' GROUP BY conscount.t ORDER BY MaxTotal asc LIMIT 1").show
      Aesthetics.printHeader("< to go back")
      userInput = readLine(">Input<")
    }while(userInput != "<")
  }

  def problemScenarioThree(spark:SparkSession){
    var userInput:String = ""
    do{
      Aesthetics.printHeader("Beverages available on Branch10, Branch8, and Branch1")
      spark.sql(raw"SELECT DISTINCT * FROM (SELECT * FROM bev_brancha UNION SELECT * FROM bev_branchb UNION SELECT * FROM bev_branchd) AS x1 WHERE branch='Branch8' OR branch='Branch10' OR branch='Branch1' ORDER BY branch, type").show()
      Aesthetics.printHeader("Common beverages available in Branch4,Branch7")
      spark.sql(raw"SELECT DISTINCT * FROM (SELECT * FROM (SELECT * FROM bev_branchb where branch = 'branch7' UNION SELECT * FROM bev_branchc where branch = 'Branch7') as x1 INNER JOIN (SELECT * FROM bev_branchc where branch='Branch4') as x2 ON x1.type = x2.type) as x3").show()
      Aesthetics.printHeader("< to go back>")
      userInput = readLine(">Input<")
    }while(userInput != "<")
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
    //generateTables(spark)
    //dropTables(spark)
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
          case 1 => problemScenarioOne(spark)
          case 2 => problemScenarioTwo(spark)
          case 3 => problemScenarioThree(spark)
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

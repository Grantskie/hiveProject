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
    println("Generating partitioned and bucketed tables")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_branchaPB(type STRING) PARTITIONED BY (branch STRING) CLUSTERED BY (type) INTO 20 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("INSERT OVERWRITE TABLE bev_branchaPB PARTITION (branch) SELECT * FROM bev_brancha")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_branchbPB(type STRING) PARTITIONED BY (branch STRING) CLUSTERED BY (type) INTO 20 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("INSERT OVERWRITE TABLE bev_branchbPB PARTITION (branch) SELECT * FROM bev_branchb")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_branchcPB(type STRING) PARTITIONED BY (branch STRING) CLUSTERED BY (type) INTO 20 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("INSERT OVERWRITE TABLE bev_branchcPB PARTITION (branch) SELECT * FROM bev_branchc")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_branchdPB(type STRING) PARTITIONED BY (branch STRING) CLUSTERED BY (type) INTO 20 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("INSERT OVERWRITE TABLE bev_branchdPB PARTITION (branch) SELECT * FROM bev_branchd")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_conscountaPB(amount INT) PARTITIONED BY (type STRING) CLUSTERED BY (amount) INTO 20 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("INSERT OVERWRITE TABLE bev_conscountaPB PARTITION (type) SELECT amount, type FROM bev_conscounta")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_conscountbPB(amount INT) PARTITIONED BY (type STRING) CLUSTERED BY (amount) INTO 20 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("INSERT OVERWRITE TABLE bev_conscountbPB PARTITION (type) SELECT amount, type FROM bev_conscountb")
    spark.sql("CREATE TABLE IF NOT EXISTS bev_conscountcPB(amount INT) PARTITIONED BY (type STRING) CLUSTERED BY (amount) INTO 20 BUCKETS ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
    spark.sql("INSERT OVERWRITE TABLE bev_conscountcPB PARTITION (type) SELECT amount, type FROM bev_conscountc")
    spark.sql("CREATE TABLE IF NOT EXISTS timeTest (id INT, normal INT, partitioned INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \',\' STORED AS TEXTFILE")
  }

  def dropTables(spark:SparkSession){
    spark.sql("DROP TABLE bev_brancha")
    spark.sql("DROP TABLE bev_branchb")
    spark.sql("DROP TABLE bev_branchc")
    spark.sql("DROP TABLE bev_branchd")
    spark.sql("DROP TABLE bev_conscounta")
    spark.sql("DROP TABLE bev_conscountb")
    spark.sql("DROP TABLE bev_conscountc")

    spark.sql("DROP TABLE bev_branchapb")
    spark.sql("DROP TABLE bev_branchbpb")
    spark.sql("DROP TABLE bev_branchcpb")
    spark.sql("DROP TABLE bev_branchdpb")
    spark.sql("DROP TABLE bev_conscountapb")
    spark.sql("DROP TABLE bev_conscountbpb")
    spark.sql("DROP TABLE bev_conscountcpb")

    spark.sql("DROP TABLE timetest")
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
      spark.sql("SELECT SUM(conscount.a) FROM bev_branchapb INNER JOIN (SELECT type as t, amount as a FROM bev_conscountapb UNION ALL SELECT type, amount FROM bev_conscountbpb UNION ALL SELECT type, amount FROM bev_conscountcpb) as conscount  ON bev_branchapb.type = conscount.t WHERE branch = \'Branch1\'").show
      Aesthetics.printHeader("Total # of consumers in branch2")
      spark.sql(raw"SELECT SUM(conscount.a) FROM (SELECT * FROM bev_branchapb UNION ALL SELECT * FROM bev_branchcpb) AS bev_branch INNER JOIN (SELECT type as t, amount as a FROM bev_conscountapb UNION ALL SELECT type, amount FROM bev_conscountbpb UNION ALL SELECT type, amount FROM bev_conscountcpb) as conscount ON bev_branch.type = conscount.t WHERE branch = 'Branch2'").show
      Aesthetics.printHeader("< to go back")
      userInput = readLine(">Input<")
    }while(userInput != "<")
  }

  def problemScenarioTwo(spark:SparkSession){
    var userInput:String = ""
    do{
      Aesthetics.printHeader("Most consumed beverage on Branch1")
      spark.sql(raw"SELECT t as MaxType, SUM(conscount.a) as MaxTotal FROM bev_branchapb INNER JOIN (SELECT type as t, amount as a FROM bev_conscountapb UNION ALL SELECT type, amount FROM bev_conscountbpb UNION ALL SELECT type, amount FROM bev_conscountcpb) as conscount  ON bev_branchapb.type = conscount.t WHERE branch = 'Branch1' GROUP BY conscount.t ORDER BY MaxTotal desc LIMIT 1").show
      Aesthetics.printHeader("Least consumed beverage on Branch2")
      spark.sql(raw"SELECT t as MaxType, SUM(conscount.a) as MaxTotal FROM (SELECT * FROM bev_branchapb UNION ALL SELECT * FROM bev_branchcpb) AS bev_branch INNER JOIN (SELECT type as t, amount as a FROM bev_conscountapb UNION ALL SELECT type, amount FROM bev_conscountbpb UNION ALL SELECT type, amount FROM bev_conscountcpb) as conscount ON bev_branch.type = conscount.t WHERE branch = 'Branch2' GROUP BY conscount.t ORDER BY MaxTotal asc LIMIT 1").show
      Aesthetics.printHeader("Average consumed beverage on Branch2")
      spark.sql(raw"SELECT t as MaxType, SUM(conscount.a) as MaxTotal FROM (SELECT * FROM bev_branchapb UNION ALL SELECT * FROM bev_branchcpb) AS bev_branch INNER JOIN (SELECT type as t, amount as a FROM bev_conscountapb UNION ALL SELECT type, amount FROM bev_conscountbpb UNION ALL SELECT type, amount FROM bev_conscountcpb) as conscount ON bev_branch.type = conscount.t WHERE branch = 'Branch2' GROUP BY conscount.t ORDER BY MaxTotal desc LIMIT 1").show
      Aesthetics.printHeader("< to go back")
      userInput = readLine(">Input<")
    }while(userInput != "<")
  }

  def problemScenarioThree(spark:SparkSession){
    var userInput:String = ""
    do{
      Aesthetics.printHeader("Beverages available on Branch10, Branch8, and Branch1")
      spark.sql(raw"SELECT DISTINCT type FROM (SELECT * FROM bev_branchapb UNION SELECT * FROM bev_branchbpb UNION SELECT * FROM bev_branchdpb) AS x1 WHERE branch='Branch8' OR branch='Branch10' OR branch='Branch1' ORDER BY type").show()
      Aesthetics.printHeader("Common beverages available in Branch4,Branch7")
      spark.sql(raw"SELECT DISTINCT x3.type, x3.branch, x3.branch2 FROM (SELECT * FROM (SELECT * FROM bev_branchbpb where branch = 'branch7' UNION SELECT * FROM bev_branchcpb where branch = 'Branch7') as x1 INNER JOIN (SELECT type as type2, branch as branch2 FROM bev_branchcpb where branch='Branch4') as x2 ON x1.type = x2.type2) as x3").show()
      Aesthetics.printHeader("< to go back")
      userInput = readLine(">Input<")
    }while(userInput != "<")
  }

  def problemScenarioFour(spark:SparkSession){
    var userInput:String=""
    do{
      Aesthetics.printHeader("Partition of common beverages in branches 4 & 7")
      spark.sql(raw"DROP TABLE IF EXISTS problemFour")
      spark.sql(raw"CREATE TABLE problemFour (branch1 STRING, branch2 STRING) PARTITIONED BY (type STRING)")
      spark.sql(raw"INSERT OVERWRITE TABLE problemFour PARTITION (type) SELECT DISTINCT x3.branch, x3.branch2, x3.type FROM (SELECT * FROM (SELECT * FROM bev_branchbpb where branch = 'branch7' UNION SELECT * FROM bev_branchcpb where branch = 'Branch7') as x1 INNER JOIN (SELECT type as type2, branch as branch2 FROM bev_branchcpb where branch='Branch4') as x2 ON x1.type = x2.type2) as x3")
      spark.sql(raw"describe formatted problemfour").show
      spark.sql("DROP VIEW IF EXISTS problemFourView")
      spark.sql(raw"create view problemFourView as SELECT type, branch, branch2 FROM (SELECT DISTINCT x3.type, x3.branch, x3.branch2 FROM (SELECT * FROM (SELECT * FROM bev_branchbpb where branch = 'branch7' UNION SELECT * FROM bev_branchcpb where branch = 'Branch7') as x1 INNER JOIN (SELECT type as type2, branch as branch2 FROM bev_branchcpb where branch='Branch4') as x2 ON x1.type = x2.type2) as x3) as x4")
      Aesthetics.printHeader("View of common beverages in branches 4 & 7")
      spark.sql(raw"select * from problemFourView").show
      Aesthetics.printHeader("< to go back")
      userInput = readLine(">Input<")
    }while(userInput != "<")
  }

  def problemScenarioFive(spark:SparkSession): Unit ={
    var userInput:String=""
    var propertyInput:String = ""
    do{
      Aesthetics.printHeader("Menu")
      Aesthetics.printBorderVert("1) Add a comment to table properties")
      Aesthetics.printBorderVert("2) Add a note to table properties")
      Aesthetics.printHeader("< to go back>")
      userInput = readLine(">Input<")
      val test = userInputCheck(userInput)
      if(test == 2) {
        userInput.toInt match{
          case 1 => {
            Aesthetics.printHeader("What is your comment")
            propertyInput = readLine(">Input<")
            spark.sql(s"ALTER TABLE bev_brancha SET TBLPROPERTIES(\'comments\' = \'$propertyInput\')")
            spark.sql(raw"SHOW TBLPROPERTIES bev_brancha").show()
          }
          case 2 => {
            Aesthetics.printHeader("What is your note")
            propertyInput = readLine(">Input<")
            spark.sql(s"ALTER TABLE bev_brancha SET TBLPROPERTIES(\'note\' = \'$propertyInput\')")
            spark.sql(raw"SHOW TBLPROPERTIES bev_brancha").show()
          }
          case _ => "c"
        }
      }
    }while(userInput != "<")
  }

  def problemScenarioSix(spark:SparkSession){
    var userInput = 0
    spark.sql(raw"DROP TABLE IF EXISTS problemsix")
    spark.sql(raw"CREATE TABLE IF NOT EXISTS problemsix (amount INT) PARTITIONED BY (row_num INT)")
    spark.sql(raw"INSERT OVERWRITE TABLE problemsix PARTITION (row_num) SELECT conscount.a as amount, ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) as row_num FROM bev_brancha INNER JOIN (SELECT type as t, amount as a FROM bev_conscounta UNION ALL SELECT * FROM bev_conscountb UNION ALL SELECT * FROM bev_conscountc) as conscount  ON bev_brancha.type = conscount.t WHERE branch = 'Branch1' LIMIT 20")
    spark.sql(raw"SELECT * FROM problemsix ORDER BY row_num").show()
    Aesthetics.printHeader("Delete row number")
    println(">Input<")
    userInput = readInt()
    spark.sql(s"ALTER TABLE problemsix DROP IF EXISTS PARTITION (row_num=$userInput)")
    spark.sql(raw"SELECT * FROM problemsix order by row_num").show()
    readLine("Press anything to back")
  }

  def extraOne(spark:SparkSession): Unit = {
    var userInput: String = ""
    do {
      Aesthetics.printHeader("Complete Optimization")
      Aesthetics.printBorderVert("SET hive.cbo.enable = true")
      Aesthetics.printBorderVert("SET hive.compute.query.using.stats = true")
      Aesthetics.printBorderVert("SET hive.stats.fetch.column.stats = true")
      Aesthetics.printBorderVert("SET hive.stats.fetch.partition.stats = true")
      Aesthetics.printBorderHorz(1)
      spark.sql(raw"describe formatted bev_branchapb").show
      Aesthetics.printHeader("< to go back")
      userInput = readLine(">Input<")
    } while (userInput != "<")
  }

  def extraTwo(spark:SparkSession): Unit ={
    var userInput = ""
    var insertStatement = "INSERT INTO TABLE timeTest VALUES "
    var maxId = 0
    var id: Int = 0
    var time1: Long = 0
    var time2: Long = 0
    if(spark.sql(raw"SELECT MAX(id) as max FROM timeTest").select("max").take(1)(0)(0) == null) maxId = 0
    else maxId = spark.sql(raw"SELECT MAX(id) as max FROM timeTest").select("max").take(1)(0)(0).toString.toInt
    do{
      Aesthetics.printHeader("Menu")
      Aesthetics.printBorderVert("1) Run test")
      Aesthetics.printBorderVert("2) View test")
      Aesthetics.printHeader("< to go back")
      userInput = readLine(">input<")
      val test = userInputCheck(userInput)
      if(test == 2){
        userInput.toInt match {
          case 1 => {
            for(i<-(maxId+1) to (maxId + 1000)){
              id = i
              time1 = System.currentTimeMillis()
              spark.sql(raw"SELECT SUM(conscount.a) FROM bev_branchapb INNER JOIN (SELECT type as t, amount as a FROM bev_conscountapb UNION ALL SELECT type, amount FROM bev_conscountbpb UNION ALL SELECT type, amount FROM bev_conscountcpb) as conscount  ON bev_branchapb.type = conscount.t WHERE branch = 'Branch1'").show
              time1 = System.currentTimeMillis() - time1
              time2 = System.currentTimeMillis()
              spark.sql(raw"SELECT SUM(conscount.a) FROM bev_brancha INNER JOIN (SELECT type as t, amount as a FROM bev_conscounta UNION ALL SELECT type, amount FROM bev_conscountb UNION ALL SELECT type, amount FROM bev_conscountc) as conscount  ON bev_brancha.type = conscount.t WHERE branch = 'Branch1'").show
              time2 = System.currentTimeMillis() - time2
              //println(s"$id | $time1 | $time2")
              insertStatement = insertStatement + s"($id,$time2,$time1),"
            }
            insertStatement = insertStatement.substring(0,insertStatement.length-1)
            spark.sql(insertStatement)
            spark.sql(raw"SELECT * FROM timeTest").show
          }
          case 2 => {
            spark.sql("SELECT AVG(normal), AVG(partitioned), MAX(id) as Total FROM timetest").show
          }
          case _ => 'a'
        }
      }
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
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("SET hive.enforce.bucketing=false")
    spark.sql("SET hive.enforce.sorting=false")
    spark.sql("SET hive.cbo.enable = true")
    spark.sql("SET hive.compute.query.using.stats = true")
    spark.sql("set hive.stats.fetch.column.stats = true")
    spark.sql("set hive.stats.fetch.partition.stats = true")
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
      Aesthetics.printBorderVert("7) First Extra")
      Aesthetics.printBorderVert("8) Second Extra")
      Aesthetics.printBorderVert(1)
      Aesthetics.printHeader("< to exit")
      userInput = readLine(">Input<")
      val test = userInputCheck(userInput)
      if(test == 2) {
        userInput.toInt match {
          case 1 => problemScenarioOne(spark)
          case 2 => problemScenarioTwo(spark)
          case 3 => problemScenarioThree(spark)
          case 4 => problemScenarioFour(spark)
          case 5 => problemScenarioFive(spark)
          case 6 => problemScenarioSix(spark)
          case 7 => extraOne(spark)
          case 8 => extraTwo(spark)
          case _ => "other"
        }
      }
    }while(userInput != "<")
  }
}

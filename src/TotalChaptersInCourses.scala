import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object TotalChaptersInCourses extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "TotalChaptersInCourses")

  // val initialRdd = sc.textFile(
  //   "/home/gaurav/Documents/BigData/week-10/Assignments/datasets/chapters-201108-004545.csv"
  // )
  // val chapterRdd = initialRdd.map(x => (x.split(",")(0), x.split(",")(1).toInt))
  // val courseKeyRdd = chapterRdd.map(x => (x._2, 1))
  // val chaptersInCourseRdd =
  //   courseKeyRdd.reduceByKey((x, y) => x + y).sortBy(x => x._1)
  // chaptersInCourseRdd.collect.foreach(println)

  //Create Base RDD for chapters data
  val chapterDataRDD = sc.textFile("/home/gaurav/Documents/BigData/week-10/Assignments/datasets/chapters-201108-004545.csv").map(x => {
    val chapterDataFields = x.split(",")
    (chapterDataFields(0).toInt,chapterDataFields(1).toInt)
  })

  //Create Base RDD for views data
  val viewDataRDD = sc.textFile("/home/gaurav/Documents/BigData/week-10/Assignments/datasets/views*.csv").map(x => {
    (x.split(",")(0).toInt, x.split(",")(1).toInt)
  })

  //Create Base RDD for titles data
  val titlesDataRDD = sc.textFile("/home/gaurav/Documents/BigData/week-10/Assignments/datasets/titles-201108-004545.csv").map( x =>
    (x.split(",")(0).toInt, x.split(",")(1))
  )

  val chapterCountRDD = chapterDataRDD.map(x => (x._2,1)).reduceByKey((x,y) => x + y)

  //Exercise 2:

  //Step 1:Removing Duplicate Views from views RDD
  val viewDataDistinctRDD = viewDataRDD.distinct()

  //Step 2: Joining chapterDataRDD with viewDataDistinctRDD, to get CourseID also.Join key is chapterID
  //First flip the viewDataDistinctRDD to make chapterId as the key
  val flippedviewDataRDD = viewDataDistinctRDD.map(x => (x._2,x._1))

  //JOIN flippedviewDataRDD with chapterDataRDD to get the courseIDs as well
  val joinedRDD = flippedviewDataRDD.join (chapterDataRDD)

  //Step 3: Dropping off the chapterIds and appending 1 as the value
  val pairRDD = joinedRDD.map( x => ((x._2._1, x._2._2),1))

  //Step 4 - Count Views for User/Course -Finding out count of number of chapters a user has watched per course
  val userPerCourseViewRDD = pairRDD.reduceByKey(_ + _)

  //Step 5 Dropping the UserID going forward
  val courseViewsCountRDD = userPerCourseViewRDD.map( x => (x._1._2,x._2))

  //Step-6 Join the chapterCountRDD with courseViewsCountRDD to integrate total chapters in a course
  val newJoinedRDD = courseViewsCountRDD.join(chapterCountRDD)

  //Step-7 Calculating Percentage of course completion
  val CourseCompletionpercentRDD = newJoinedRDD.mapValues(x => (x._1.toDouble/x._2))

  // formatting the RDD output :
  val formattedpercentageRDD = CourseCompletionpercentRDD.mapValues(x => f"$x%01.5f".toDouble)

  //Step-8 Map Percentages to Scores
  val scoresRDD = formattedpercentageRDD.mapValues (x => {
    if(x >= 0.9) 10l
    else if(x >= 0.5 && x < 0.9) 4l
    else if(x >= 0.25 && x < 0.5) 2l
    else 0l
  })

  //Step -9 Adding up the total scores for a course
  val totalScorePerCourseRDD = scoresRDD.reduceByKey((V1,V2) => V1 + V2)

  // Exercise -3 Associate Titles with Courses and getting rid of courseIDs
  val title_score_joinedRDD = totalScorePerCourseRDD.join(titlesDataRDD).map( x => (x._2._1, x._2._2))

  //Displaying courses starting with the most popular course
  val popularCoursesRDD = title_score_joinedRDD.sortByKey(false)
  popularCoursesRDD.collect.foreach(println)
}

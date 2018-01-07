def Friends_intersection(r1:String, r2:String) : Int = {
  val tr1=r1.stripPrefix("[").stripSuffix("]").trim.split(",")
  val tr2=r2.stripPrefix("[").stripSuffix("]").trim.split(",")
  val rtrn: Int = (tr1.intersect(tr2)).size
  return rtrn
}
//function working perfectly

val df = sqlContext.read.format("csv").option("inferSchema", "true").option("delimiter", "\\t").load("/FileStore/tables/05ij6e2b1508193248710/soc_LiveJournal1Adj-2d179.txt")                //Converting file to dataframe
val df_filtered=df.filter(df("_c1").isNotNull)
val dup_df_filtered=df.filter(df("_c1").isNotNull)
val paired_df=df_filtered.crossJoin(dup_df_filtered)
val newNames = Seq("user1", "f1", "user2", "f2")
val paired_df_renamed=paired_df.toDF(newNames:_*)
val pairs_filtered=paired_df_renamed.filter(!(paired_df_renamed("user1").contains(paired_df_renamed("user2"))))     //eliminating the self pairs
//checkpoint 1
val temp=pairs_filtered.map(s=>((s.getInt(0),s.getInt(2)),(Friends_intersection(s.getString(1),s.getString(3))))).toDF()      //performing intersection
val displayNames = Seq("User_Pair", "Number_of_common_friends")
val final_display=temp.toDF(displayNames:_*)
//val s=pairs_filtered.select("f1").take(1).mkString(",").stripPrefix("[").stripSuffix("]").trim.split(",")
final_display.show()
//df.printSchema()
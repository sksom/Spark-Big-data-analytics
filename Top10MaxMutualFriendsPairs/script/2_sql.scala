def F_Sort(a:Int, b:Int) : List[Int]   = {
  if(a<b){
    val rtrn: List[Int] = List(a,b)
    return rtrn
  }
  else{
    val rtrn: List[Int] = List(b,a)
     return rtrn
  }
}

val List_Friends=sc.textFile("/FileStore/tables/05ij6e2b1508193248710/soc_LiveJournal1Adj-2d179.txt").cache()
val Friends_Pair_raw = List_Friends.map(line=>line.split("\\t")).filter(line => (line.size == 2))
val Friends_Pair=Friends_Pair_raw.map(line=>(line(0),line(1).split(",")))
val pair_n_List=Friends_Pair.flatMap(x=>(x._2.map(z=>((x._1.toInt,z.toInt),  x._2.flatMap(a=>a.split(" "))))))
val reduce_phase=pair_n_List.map(x=>(F_Sort(x._1._1, x._1._2),x._2)).groupByKey()
val reduce_phase_filtered=reduce_phase.filter(line => (line._2.size == 2)) //this means there are two lists to compare in the first place
val common_friends=reduce_phase_filtered.map(x=>(x._1, x._2.toList(1).toList.intersect( x._2.toList(0).toList ))) //trying to get only common elements out of 2 lists and associating it with the pair itself
val final_output=common_friends.sortBy(_._2.size,false).map(x=>(x._1(0),x._1(1),x._2.size))  //final output is already a RDD in [String]
//final_output.first()
//checkpoint 1 , data pre-processing done
val final_output_df=final_output.toDF().limit(10)       //taking only top 10 rows from the dataframe and saving into new dataframe
val df=sqlContext.read.format("csv").option("inferSchema","true").option("delimiter",",").load("/FileStore/tables/9yohuo0h1508444384897/userdata.txt")
//final_output_df.show()
//checkpoint 2 , tables to be joined are ready and formatted
val result_table1=final_output_df.join(df,final_output_df("_1") === df("_c0")).distinct
val restemp1=result_table1.drop("_c4","_c5","_c6","_c7","_c8","_c9")
val result_table2=final_output_df.join(df,final_output_df("_2") === df("_c0")).distinct
val restemp2=result_table2.drop("_c4","_c5","_c6","_c7","_c8","_c9")
//all i need now is to join above two dataframes
val newNames = Seq("user1", "user2", "FriendsNum", "d1","user1fname","user1lname","user1add","d2","d3","d4","d5","user2fname","user2lname","user2add")
//val paired_df_renamed=paired_df.toDF(newNames:_*)
val final_join=restemp1.join(restemp2,restemp1("_1")===restemp2("_1")).toDF(newNames:_*).drop("d1","d2","d3","d4","d5").distinct
final_join.createOrReplaceTempView("displaytable")
val formatted_table=sqlContext.sql("SELECT DISTINCT displaytable.FriendsNum,displaytable.user1fname,displaytable.user1lname,displaytable.user1add,displaytable.user2fname,displaytable.user2lname,displaytable.user2add FROM displaytable")
formatted_table.show()
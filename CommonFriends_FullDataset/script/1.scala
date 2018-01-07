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
val final_output=common_friends.map(x=>(x._1(0)+","+x._1(1)+"\t"+x._2.size))
//dbutils.fs.put("/FileStore/my-stuff/outputQ1.txt", final_output.collect().mkString("\n"))
final_output.take(100).foreach((println))
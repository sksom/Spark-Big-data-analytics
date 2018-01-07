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
val final_output=common_friends.sortBy(_._2.size,false).map(x=>(x._1(0)+","+x._1(1)+"\t"+x._2.size))  //final output is already a RDD in [String] 

val user_data=sc.textFile("/FileStore/tables/9yohuo0h1508444384897/userdata.txt").cache()

//final_output.take(10).foreach((println))
//Now I need 2 different RDDs filtered on the basis of final_output's pair's first and another with the second value
val temp01=final_output.map(line=>line.split(",")(0))     //pair's first id
val temp02=final_output.map(line=>line.split(",")(1).split("\\t")(0))     //pair's second id
//checkpoint 1
val temp01set=temp01.take(10).toSet
val temp02set=temp02.take(10).toSet
val temp1=user_data.map(line=>line.split(",")).filter(line=>temp01set(line(0))).map(l=>l.mkString(","))      //convert into single string
val temp2=user_data.map(line=>line.split(",")).filter(line=>temp02set(line(0))).map(l=>l.mkString(","))      //convert into single string
//println(temp02set)
//Checkpoint 2
val top10=final_output.take(10)
//in below line I try to format output as desired by the question
val q2_output=top10.map(x=>((x.split(",")(1).split("\\t")(1))+"\t"+(temp1.map(y=>(y.split(",")(0),(y.split(",")(1)+","+y.split(",")(2)+","+y.split(",")(3))))).filter(z=>(z._1==x.split(",")(0))).map(a=>a._2.mkString).take(1).mkString+"\t"+(temp2.map(y=>(y.split(",")(0),(y.split(",")(1)+","+y.split(",")(2)+","+y.split(",")(3))))).filter(z=>(z._1==x.split(",")(1).split("\\t")(0))).map(a=>a._2.mkString).take(1).mkString))
q2_output.take(10).foreach((println))
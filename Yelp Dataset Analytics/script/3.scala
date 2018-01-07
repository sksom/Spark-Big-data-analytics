val reviews_rdd=sc.textFile("/FileStore/tables/03036f9z1508828300948/review.csv")
val reviews_raw0 = reviews_rdd.map(line=>(line.split("::")(2),1)).reduceByKey((x,y)=>x+y).sortBy(_._2,false).take(10)     //I have my first joining rdd
val reviews_raw1=sc.parallelize(reviews_raw0)     // convert array from take(10) to RDD again
val business_rdd=sc.textFile("/FileStore/tables/03036f9z1508828300948/business.csv")
val business_raw=business_rdd.map(line=>(line.split("::")(0),(line.split("::")(1)+"\t"+line.split("::")(2))))      //I have my second joining rdd
val final_rdd=business_raw.join(reviews_raw1).distinct().sortBy(_._2._2,false)
val formatted_res=final_rdd.map(x=>(x._1+"\t"+x._2._1+"\t"+x._2._2))
formatted_res.collect().foreach(println)
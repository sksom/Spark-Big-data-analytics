val business_rdd=sc.textFile("/FileStore/tables/03036f9z1508828300948/business.csv")
val business_raw=business_rdd.map(line=>(line.split("::")(0),line.split("::")(1))).filter(x=>x._2.contains("Palo Alto"))     //I have my first joining rdd

val reviews_rdd=sc.textFile("/FileStore/tables/03036f9z1508828300948/review.csv")
val reviews_raw = reviews_rdd.map(line=>(line.split("::")(2),(line.split("::")(1)+"\t"+line.split("::")(3))))     //I have my second joining rdd

val final_rdd=reviews_raw.join(business_raw).distinct()
val formatted_res=final_rdd.map(x=>(x._2._1))
formatted_res.collect().foreach(println)
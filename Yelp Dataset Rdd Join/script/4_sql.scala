val business_rdd=sc.textFile("/FileStore/tables/03036f9z1508828300948/business.csv")
val business_df=business_rdd.map(line=>(line.split("::")(0),line.split("::")(1))).filter(x=>x._2.contains("Palo Alto")).toDF()     //rdd is needed because :: will not be parsed as delimiter by sqlContext since it's actually two characters and sqlContext only takes one as delimiter

val reviews_rdd=sc.textFile("/FileStore/tables/03036f9z1508828300948/review.csv")
val reviews_df = reviews_rdd.map(line=>(line.split("::")(2),line.split("::")(1),line.split("::")(3))).toDF()     //rdd is needed because :: will not be parsed as delimiter by sqlContext since it's actually two characters and sqlContext only takes one as delimiter

//reviews_df.show()
//business_df.show()
val final_df=reviews_df.join(business_df,reviews_df("_1")===business_df("_1")).distinct()
val newNames = Seq("d1", "userId", "rating", "d2", "d3")
val formatted_res=final_df.toDF(newNames:_*).drop("d1","d2","d3")
formatted_res.show()
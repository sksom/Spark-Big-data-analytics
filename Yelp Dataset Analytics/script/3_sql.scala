val reviews_rdd=sc.textFile("/FileStore/tables/03036f9z1508828300948/review.csv") //rdd is needed because :: will not be parsed as delimiter by sqlContext since it's actually two characters and sqlContext only takes one as delimiter
val reviews_df = reviews_rdd.map(line=>(line.split("::")(2))).toDF()
val temp1=reviews_df.groupBy("value").count()
val temp2=temp1.orderBy(temp1("count").desc).limit(10)
val business_rdd=sc.textFile("/FileStore/tables/03036f9z1508828300948/business.csv")  //rdd is needed because :: will not be parsed as delimiter by sqlContext since it's actually two characters and sqlContext only takes one as delimiter
val business_df=business_rdd.map(line=>(line.split("::")(0),line.split("::")(1),line.split("::")(2))).toDF()
val final_join=business_df.join(temp2,business_df("_1")===temp2("value")).distinct
val final_join_sorted=final_join.orderBy(final_join("count").desc)
val newNames = Seq("businessId", "fullAddress", "categories", "numberRated")
val formatted_display=final_join_sorted.drop("value").toDF(newNames:_*)
formatted_display.show()
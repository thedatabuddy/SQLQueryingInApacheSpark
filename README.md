## SQL Querying in Apache Spark
## Setting up the Environment

Reading Input Tweets file
```
val df=spark.read.json("hdfs://localhost:9000/user/aadi22796/ne/finalfile.json")df.registerTempTable("cs490")or we can also usedf.createOrReplaceTempView("cs490")QUESTION 1
```

## Hitting those tweets with some SQL!

1. Retrieve the id and creation time of each tweet
```
val q1=spark.sql("select id, timestamp_ms from cs490")

q1.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q1")
```

2. Retrieve the user id, followers count, and friends count for each tweet and order the results by user id

```
val q2=spark.sql("select user.id, user.followers_count, user.friends_count from cs490 order by user.id")
q2.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q2")
```

3. Retrieve the tweet id, user id for those tweets whose lang = “en”

```
val q3=spark.sql("select user.id, id from cs490 where lang='en'")
q3.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q3")
```

4. Retrieve all the hashtags in the tweets and output the list of unique hashtags and their total frequency of occurrence in the tweets

```
// CAN ALSO BE SOLVED VIA EXPLODE STATEMENT
val q4n=spark.sql("select explode (entities.hashtags.text) from cs490")

val q4rdd=q4n.rdd

val q4temp=q4rdd.map(s=>(s,1))

val q4counts=q4temp.reduceByKey((a,b)=>a+b)

val q4res=q4counts.collect()

q4res.foreach(println)


sc.parallelize(q4res).saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q4")
```

5. Retrieve all the URLs posted by users in the tweets and output the list of unique URLs and their total frequency of occurrence in the tweets, order by the URL frequency (high to low) 

```
val q5=spark.sql("select explode(entities.urls.expanded_url) from cs490")

val q5rdd=q5.rdd

val q5temp=q5rdd.map(s=>(s,1))

val q5counts=q5temp.reduceByKey((a,b)=>a+b)

val q5sorted=q5counts.sortBy(x => -x._2)

q5sorted.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q5")
```

6. Retrieve the start position of every hashtag in the tweets and output them in sorted order (ascending order)

```
val q6=spark.sql("select explode (entities.hashtags.indices[0]) from cs490")

val q6ordered=q6.orderBy(asc("col"))


q6ordered.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q6")
```

7. Select tweets where isFavorited is false and only output the average of the followers count by grouping the tweets based on time zone

```
val q7=spark.sql("select user.followers_count,user.time_zone from cs490 where favorited=false")

val q7temp=q7.groupBy("time_zone").mean("followers_count")
```

8. Select tweets where isRetweeted is false and the user is not verified, and only output the average of the friends count by grouping the tweets based on time zone

```
val q8=spark.sql("select user.friends_count, user.time_zone from cs490 where retweeted=false and user.verified=false")

val q8temp=q8.groupBy("time_zone").mean("friends_count")

q8temp.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q8")

```


9. Create a table X by selecting tweets that are in the English language. Create a table Y by selecting tweets that are from verified users. Join the two tables X and Y by time zone and output the tweet ID in X and tweet ID in Y.

```
val q9t1=spark.sql("select * from cs490 where lang='en'")

val q9t2=spark.sql("select * from cs490 where user.verified=true")

q9t1.createOrReplaceTempView("q9x")

q9t2.createOrReplaceTempView("q9y")

val q9res=spark.sql("select q9x.id,q9y.id from q9x inner join q9y on (q9x.user.time_zone=q9y.user.time_zone)")

q9res.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q9")

```


10. Create a table X by selecting hashtags from tweets where the user’s time zone is US Pacific Time. Create a table Y by selecting from tweets where the user’s time zone is US Eastern Time. Join X and Y by hashtag and output the entire join result with new attribute names ‘hashtag1’ and ‘hashtag2’.

```
val q10t1=spark.sql("select explode (entities.hashtags.text) from cs490 where user.time_zone='Pacific Time (US & Canada)'")

val q10t2=spark.sql("select explode (entities.hashtags.text) from cs490 where user.time_zone='Eastern Time (US & Canada)'")

val q10res=spark.sql("select q10x.col as hashtag1 ,q10y.col as hashtag2 from q10x inner join q10y on (q10x.col=q10y.col)")


q10res.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q10")
```


11. Create a DataFrame X by extracting a user’s id, location, language, friends count, and followers count from all the tweets. Select the id and location of every record in X where language is not English and  also output (along with id and location) the average friends and followers count computed over all records in X where language is not English.

```
val q11x=spark.sql("select user.id, user.location, user.lang, user.friends_count, user.followers_count from cs490")

val q11ans1=q11x.filter($"lang"=!="en").select($"id",$"location")

q11ans1.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q11/ans1")

val q11ans2=q11x.filter($"lang"=!="en").select(avg($"friends_count"),avg($"followers_count"))

q11ans2.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q11/ans2")
```


12. Create a Dataset X by extracting a user’s id, location, language, friends count, and followers count from all the tweets. Select the id and location of every record in X where the followers count is greater than 10.

```
case class x(id: BigInt, location: String, lang:String, friends_count:BigInt, followers_count:BigInt)

val q12x=q11x.as[x]

val q12res=q12x.filter(q12x("followers_count")>10).select("id","location")


q12res.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q12")
```


13. Create a Dataset X by extracting a user’s id, location, language, friends count, and followers count from all the tweets. Select the id and location of every record in X and also output (along with id and location) the max friends count computed over those records that are of the same language as the record being output.

```
val q13x=q12x

val q13res=q13x.groupBy("lang").max("friends_count")


q13res.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q13")
```


14. Create a DataFrame X by extracting a user’s id, location, language, friends count, and followers count from all the tweets. Create a DataFrame Y by extracting the id of every verified user from all the tweets. Select all records in X where the id does not exists in Y.

```
val q14x=q11x

val q14y=spark.sql("select user.id from cs490 where user.verified=true")

val q14ans=q14x.join(q14y,q14x("id")===q14y("id"),"leftouter").where(q14y("id").isNull).drop(q14y("id"))


q14ans.rdd.saveAsTextFile("hdfs://localhost:9000/user/aadi22796/ne/solutions/q14")

```


15. Create a Dataset X by extracting a user’s id, location, and language from all the tweets. Create a Dataset Y by extracting the id and friends count of every non-verified user from all the tweets. Select all records in X if the id exists in Y and friends count is greater than 10.

```
val q15x=q12x

case class y(id:BigInt, friends_count:BigInt)

val q15y=spark.sql("select user.id, user.friends_count from cs490 where user.verified=false").as[y]

q15ans=q15x.join(q15y,q15x("id")===q15y("id"),"leftouter").drop(q15y("id")).drop(q15y("friends_count")).filter($"friends_count">10)
```

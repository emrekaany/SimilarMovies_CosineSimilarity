#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep  7 13:42:46 2024

@author: emrekaanyilmaz
"""
#In this project, movie similarity is calculated
#by using the cosine similarity. 


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, \
    IntegerType, LongType, StringType
import codecs


spark=SparkSession.builder.appName("MovieSimilarity").getOrCreate()

#Part1: We calculate movie similarity based on user ratings
#rating data is imported
schema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
df=spark.read.schema(schema).option("sep", "\t").csv("/Users/emrekaanyilmaz/python/course/ml-100k/u.data")

df.createOrReplaceTempView("my_table")

#df created to bring names of the movies
names= spark.read.option("sep", "|")\
    .csv("/Users/emrekaanyilmaz/python/course/ml-100k/u.item")\
        .select("_c0", "_c1")
names.createOrReplaceTempView("names")


#rating data is self-joined in order to compute cosine similarity
df2=spark.sql(" SELECT t1.movieID as movie1, t2.movieID as movie2, t1.rating  \
              as rating1, t2.rating as rating2 from my_table t1 left outer join \
                  my_table t2 on t1.userID=t2.userID and t1.movieID<t2.movieID\
                      ")
                      
df2.createOrReplaceTempView("my_table2")

#cosine similarity is calculated according to the ratings
df3=spark.sql(" select movie1, movie2, sum(rating1*rating1) as x2 , \
              sum(rating2*rating2) as y2 , sum(rating1*rating2) as xy, \
              count(*) amount \
              from my_table2 group by movie1, movie2 \
              ")   
df3.createOrReplaceTempView("my_table3")
df4=spark.sql("SELECT movie1, movie2, xy/ sqrt(x2)/sqrt(y2) similarity, amount from my_table3")
df4.createOrReplaceTempView("my_table4")

#names of the movies are joined from names dataframe. 
#This is the final version of the cosine similarity done according to the ratings
ratings_similarity=spark.sql( " SELECT movie1, movie2, similarity as similarity_ratings,n1._c1 as name1 \
             ,n2._c1 as name2,  amount from my_table4 t \
              left outer join names n1 on t.movie1=n1._c0 \
                left outer join names n2 on t.movie2=n2._c0 where \
                    similarity>0.97 and amount>50 and (movie1 =1 or movie2=1 )order by similarity desc")           

ratings_similarity.createOrReplaceTempView("ratings_similarity")



#Part2: We calculate movie similarity according to their genre information.
# Define the schema
schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("movie_title", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("video_release_date", StringType(), True),
    StructField("IMDb_URL", StringType(), True),
    StructField("unknown", IntegerType(), True),
    StructField("Action", IntegerType(), True),
    StructField("Adventure", IntegerType(), True),
    StructField("Animation", IntegerType(), True),
    StructField("Childrens", IntegerType(), True),
    StructField("Comedy", IntegerType(), True),
    StructField("Crime", IntegerType(), True),
    StructField("Documentary", IntegerType(), True),
    StructField("Drama", IntegerType(), True),
    StructField("Fantasy", IntegerType(), True),
    StructField("Noir", IntegerType(), True),
    StructField("Horror", IntegerType(), True),
    StructField("Musical", IntegerType(), True),
    StructField("Mystery", IntegerType(), True),
    StructField("Romance", IntegerType(), True),
    StructField("SciFi", IntegerType(), True),
    StructField("Thriller", IntegerType(), True),
    StructField("War", IntegerType(), True),
    StructField("Western", IntegerType(), True)
])

# Load the data with the defined schema
movie_df = spark.read.option("sep", "|").schema(schema)\
    .csv("/Users/emrekaanyilmaz/python/course/ml-100k/u.item")

movie_df.createOrReplaceTempView("movie_df")

#we self join the dataframe in order to calculate cosine similarity
genre_joined=spark.sql("SELECT t1.movie_id id1, t2.movie_id id2, \
                       t1.movie_title as movie_title1 , t2.movie_title as \
                       movie_title2, t1.unknown * t2.unknown + \
t1.Action * t2.Action + \
t1.Adventure * t2.Adventure + \
t1.Animation * t2.Animation + \
t1.Childrens * t2.Childrens +\
t1.Crime * t2.Crime + \
t1.Comedy * t2.Comedy + \
t1.Documentary * t2.Documentary +\
t1.Drama * t2.Drama + \
t1.Fantasy * t2.Fantasy + \
t1.Noir * t2.Noir + \
t1.Horror * t2.Horror + \
t1.Musical * t2.Musical +\
t1.Mystery * t2.Mystery + \
t1.Romance * t2.Romance + \
t1.SciFi * t2.SciFi + \
t1.Thriller * t2.Thriller + \
t1.War * t2.War + \
t1.Western * t2.Western as adotb, \
    t1.unknown * t1.unknown + \
t1.Action * t1.Action + \
t1.Adventure * t1.Adventure + \
t1.Animation * t1.Animation + \
t1.Childrens * t1.Childrens + \
t1.Comedy * t1.Comedy + \
t1.Crime * t1.Crime + \
t1.Documentary * t1.Documentary + \
t1.Drama * t1.Drama + \
t1.Fantasy * t1.Fantasy + \
t1.Noir * t1.Noir + \
t1.Horror * t1.Horror + \
t1.Musical * t1.Musical + \
t1.Mystery * t1.Mystery + \
t1.Romance * t1.Romance + \
t1.SciFi * t1.SciFi + \
t1.Thriller * t1.Thriller + \
t1.War * t1.War + \
t1.Western * t1.Western as lenght_a, \
     t2.unknown * t2.unknown + \
t2.Action * t2.Action + \
t2.Adventure * t2.Adventure + \
t2.Animation * t2.Animation + \
t2.Childrens * t2.Childrens + \
t2.Comedy * t2.Comedy + \
t2.Crime * t2.Crime + \
t2.Documentary * t2.Documentary + \
t2.Drama * t2.Drama + \
t2.Fantasy * t2.Fantasy + \
t2.Noir * t2.Noir + \
t2.Horror * t2.Horror + \
t2.Musical * t2.Musical + \
t2.Mystery * t2.Mystery + \
t2.Romance * t2.Romance + \
t2.SciFi * t2.SciFi + \
t2.Thriller * t2.Thriller + \
t2.War * t2.War + \
t2.Western * t2.Western as length_b \
    from movie_df t1 left outer join \
    movie_df t2 on t1.movie_id< t2.movie_id\
")

genre_joined.createOrReplaceTempView("genre_joined_data")

#we calculate cosine similarity
genre_similarity=spark.sql("SELECT id1, id2 , movie_title1, movie_title2 , \
                           adotb/lenght_a/length_b as genre_similarity from\
                               genre_joined_data")
                               
genre_similarity.createOrReplaceTempView("genre_similarity")



#part3: we join similarity results from part 1 and part 2 according to alfa=0.1
similarity_result=spark.sql("select movie1, movie2,name1, name2 , \
                            similarity_ratings*0.9 +genre_similarity*0.1 as similarity from ratings_similarity t1\
           left outer join genre_similarity t2 on \
               t1.movie1=t2.id1 and t1.movie2=t2.id2")
               
similarity_result.show()

import json
import math
import re
import csv
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


sc = SparkContext(appName="Project")

# Create an sql context so that we can query data files in sql like syntax
sqlContext = SQLContext(sc)
 
 
# read the CSV data file and select only the field labeled as "text"
# this returns a spark data frame


############################### Data Loading ###################################

movieReview = sqlContext.read.format("com.databricks.spark.csv") \
							 .option("header", "true") \
							 .option("quote", '"') \
							 .option("escape", '"') \
							 .load("/user/juyoungj/ProjectFolder/movieReviews.csv")


movieReview.registerTempTable("movieReviewTable")


movieInfo = sqlContext.read.format("com.databricks.spark.csv") \
                             .option("header", "true") \
                             .option("quote", '"') \
                             .option("escape", '"') \
                             .load("/user/juyoungj/ProjectFolder/movieInfodf.csv")

movieInfo.registerTempTable("movieInfoTable")



#################### First Analysis: genre vs fresh ###########################################



genreFresh = sqlContext.sql("""SELECT DISTINCT MIT.genre_per, MRT.fresh, COUNT(*) AS cnt
    FROM movieReviewTable MRT, movieInfoTable MIT
    WHERE MRT.fresh IS NOT NULL AND MIT.genre_per IS NOT NULL AND MRT.id = MIT.id
    GROUP BY MIT.genre_per, MRT.fresh
    ORDER BY MIT.genre_per
    """)


genreFresh_1a = genreFresh.rdd.map(lambda row: row.asDict())

def convert_dict_to_tuples_genreFresh(d):
    genre = d['genre_per']
    rating = d['fresh']
    count = d['cnt']
    # tokens = WORD_RE.findall(text)
    tuples = []
    tuples.append((rating, genre, count))
    return tuples



genreFresh_1b = genreFresh_1a.flatMap(lambda x: convert_dict_to_tuples_genreFresh(x))
# [(u'fresh', u'Action and Adventure', 7334), (u'rotten', u'Action and Adventure', 5578), (u'fresh', u'Animation', 1796)]


genreFresh_totalnum = genreFresh_1b.map(lambda x: (x[1], x[2])).reduceByKey(lambda a,b: a + b)
#[(u'Mystery and Suspense', 11746), (u'Sports and Fitness', 407), (u'Cult Movies', 54)]

genreFresh_fresh = genreFresh_1b.filter(lambda x: x[0] == 'fresh')
genreFresh_freshnum = genreFresh_fresh.map(lambda x: (x[1], x[2])).reduceByKey(lambda a,b: a + b)
# [(u'Mystery and Suspense', 6874), (u'Sports and Fitness', 295), (u'Cult Movies', 46)]

genreFresh_rotten = genreFresh_1b.filter(lambda x: x[0] == 'rotten')
genreFresh_rottennum = genreFresh_rotten.map(lambda x: (x[1], x[2])).reduceByKey(lambda a,b: a + b)
#[(u'Mystery and Suspense', 4872), (u'Sports and Fitness', 112), (u'Cult Movies', 8)]

genreFresh_freshTotal = genreFresh_totalnum.join(genreFresh_freshnum)
#[(u'Mystery and Suspense', (11746, 6874)), (u'Sports and Fitness', (407, 295)), (u'Cult Movies', (54, 46))]
genreFresh_rottenTotal = genreFresh_totalnum.join(genreFresh_rottennum)
#[(u'Mystery and Suspense', (11746, 4872)), (u'Sports and Fitness', (407, 112)), (u'Cult Movies', (54, 8))]

# unsorted_freshPerGenre = genreFresh_freshTotal.map(lambda x: (x[0], float(x[1][1]/x[1][0])))
# both denominator and numerator has to be converted to float before division.
unsorted_freshPerGenre = genreFresh_freshTotal.map(lambda x: (x[0], float(x[1][1])/float(x[1][0]) ))
sorted_freshPerGenre = unsorted_freshPerGenre.sortBy(lambda x: x[1], ascending = False)

sorted_freshPerGenre.saveAsTextFile("freshReviewPerGenreRatio")

unsorted_rottenPerGenre = genreFresh_rottenTotal.map(lambda x: (x[0], float(x[1][1])/float(x[1][0]) ))
sorted_rottenPerGenre = unsorted_rottenPerGenre.sortBy(lambda x: x[1], ascending = False)

sorted_rottenPerGenre.saveAsTextFile("rottenReviewPerGenreRatio")


##################### Second Analysis: box_office vs fresh ##############################

movieInfoBoxOffice = sqlContext.read.format("com.databricks.spark.csv") \
                             .option("header", "true") \
                             .option("quote", '"') \
                             .option("escape", '"') \
                             .load("/user/juyoungj/ProjectFolder/movieInfoBoxOffice.csv")

movieInfoBoxOffice.registerTempTable("movieInfoBoxOfficeTable")


freshBoxOffice = sqlContext.sql(""" SELECT MRT.id, MRT.fresh, MITE.box_office
    FROM movieReviewTable MRT, movieInfoBoxOfficeTable MITE
    WHERE MRT.fresh IS NOT NULL AND MITE.box_office IS NOT NULL AND MRT.id = MITE.id 
    """)

# freshBoxOfficeGenre = sqlContext.sql(""" SELECT MITE.box_office, MITE.genre_per, MRT.fresh
#     FROM movieReviewTable MRT, movieInfoBoxOfficeTable MITE
#     WHERE MRT.fresh IS NOT NULL AND MITE.box_office IS NOT NULL AND MITE.genre_per IS NOT NULL AND MRT.id = MITE.id
#  """)

from pyspark.sql.types import FloatType

freshBoxOfficeFloat = freshBoxOffice.withColumn("box_office", freshBoxOffice["box_office"].cast(FloatType()))
freshBoxOfficeFloat.registerTempTable("freshBoxOfficeTable")

boxOfficeRatioFreshRotten = sqlContext.sql(""" SELECT TR.id, TR.box_office*(FR.freshReviewCount/TR.totalReviewCount) AS freshRatio, TR.box_office*(RR.rottenReviewCount/TR.totalReviewCount) AS rottenRatio
    FROM (SELECT DISTINCT FBO1.id, COUNT(*) AS totalReviewCount, FBO1.box_office AS box_office
          FROM freshBoxOfficeTable FBO1
          GROUP BY FBO1.id, FBO1.box_office) TR,
          (SELECT FBO2.id, COUNT(*) AS freshReviewCount
          FROM freshBoxOfficeTable FBO2
          WHERE FBO2.fresh = 'fresh'
          GROUP BY FBO2.id) FR,
          (SELECT FBO3.id, COUNT(*) AS rottenReviewCount
          FROM freshBoxOfficeTable FBO3
          WHERE FBO3.fresh = 'rotten'
          GROUP BY FBO3.id) RR
    WHERE TR.id = FR.id AND FR.id = RR.id
    """)

boxOfficeRatioFreshRotten.registerTempTable("boxOfficeRatioFreshRottenTable")

avgBoxOfficeFreshRotten = sqlContext.sql(""" SELECT AVG(freshRatio) AS freshAvgBoxOffice, AVG(rottenRatio) AS rottenAvgBoxOffice
    FROM boxOfficeRatioFreshRottenTable
    """)

avgBoxOfficeFreshRotten.collect()
avgBoxOfficeFreshRotten.rdd.map(lambda i: ','.join(str(j) for j in i))


with open('boxOfficeRatioFreshRottenTable.csv', 'wb') as csvFile:
    freshRottenBoxOfficeAvg = csv.writer(csvFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    freshRottenBoxOfficeAvg.writerow(["freshBoxOffice", "rottenBoxOffice"])
    for row in avgBoxOfficeFreshRotten.collect():
        freshRottenBoxOfficeAvg.writerow(row)

############################### Third Analysis: genre vs review #################################

top3Genre = sqlContext.sql(""" SELECT RG.genre_per, COUNT(*) AS reviewCount
    FROM (SELECT DISTINCT MRT.review, MIT.genre_per
    FROM movieReviewTable MRT, movieInfoTable MIT
    WHERE MRT.review IS NOT NULL AND MIT.genre_per IS NOT NULL AND MRT.id = MIT.id) RG
    GROUP BY RG.genre_per
    ORDER BY reviewCount DESC
    """)

##### top4 genres that reviews are written are Drama(29885), Comedy(19262), Action and Adventure(11452)

movieGenreReview = sqlContext.sql("""
    SELECT DISTINCT MRT.review, MIT.genre_per
    FROM movieReviewTable MRT, movieInfoTable MIT
    WHERE MRT.review IS NOT NULL AND MIT.genre_per IS NOT NULL AND MRT.id = MIT.id AND MIT.genre_per = 'Drama'
    UNION
SELECT DISTINCT MRT.review, MIT.genre_per
    FROM movieReviewTable MRT, movieInfoTable MIT
    WHERE MRT.review IS NOT NULL AND MIT.genre_per IS NOT NULL AND MRT.id = MIT.id AND MIT.genre_per = 'Comedy'
    UNION
    SELECT DISTINCT MRT.review, MIT.genre_per
    FROM movieReviewTable MRT, movieInfoTable MIT
    WHERE MRT.review IS NOT NULL AND MIT.genre_per IS NOT NULL AND MRT.id = MIT.id AND MIT.genre_per = 'Action and Adventure'
    """)
#60599 rows

movieGenreReview_1a  = movieGenreReview.rdd.map(lambda row: row.asDict())
# [{'review': u'It helps that Pattinson interacts with truly great performances from the supporting cast. ', 'genre_per': u'Drama'}, {'review': u'A smug, academic thesis on the evils of capitalism, Cosmopolis is as exciting as someone reading you the entire book in a flat, unvarying monotone.', 'genre_per': u'Science Fiction and Fantasy'}]



frequent_word_threshold=50

WORD_RE = re.compile(r'\b[\w]+\b') 
def convert_dict_to_tuples_genreReview(d):
        text = d['review']
        genre = d['genre_per']
        tokens = WORD_RE.findall(text)
        tuples = []
        for w in tokens:
            tuples.append((genre, w))
        return tuples

movieGenreReview_1b = movieGenreReview_1a.flatMap(lambda x : convert_dict_to_tuples_genreReview(x))
# [(u'Drama', u'It'), (u'Drama', u'helps')]


# count all words from all movieReviews
movieGenreReview_2a2 = movieGenreReview_1b.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
# [(u'Sarah', 48), (u'funnyman', 4)]
movieGenreReview_2a2.count() # 39621


# filter out all word-tuples from Drama reviews
movieGenreReview_Drama_2b1= movieGenreReview_1b.filter(lambda x:x[0]== 'Drama')
# [(u'Drama', u'Caligula'), (u'Drama', u'turns')]
# count all words from Drama reviews
movieGenreReview_Drama_2b2 = movieGenreReview_Drama_2b1.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
# [(u'Sarah', 11), (u'Lessons', 1)]

# filter out all word-tuples from Comedy reviews
movieGenreReview_Comedy_2c1 = movieGenreReview_1b.filter(lambda x: x[0] == 'Comedy')
# [(u'Comedy', u'Orange'), (u'Comedy', u'County')]
# count all words from negative reviews
movieGenreReview_Comedy_2c2=movieGenreReview_Comedy_2c1.map(lambda x:(x[1],1)).reduceByKey(lambda a,b:a+b)
# [(u'Sarah', 16), (u'glories', 3)]

movieGenreReview_Action_2c1 = movieGenreReview_1b.filter(lambda x: x[0] == 'Action and Adventure')
movieGenreReview_Action_2c2=movieGenreReview_Action_2c1.map(lambda x:(x[1],1)).reduceByKey(lambda a,b:a+b)


# get total word count for all, drama, and comedy reviews
all_review_word_count = movieGenreReview_2a2.map(lambda x:x[1]).sum() 
drama_review_word_count = movieGenreReview_Drama_2b2.map(lambda x:x[1]).sum() 
comedy_review_word_count = movieGenreReview_Comedy_2c2.map(lambda x:x[1]).sum()
action_review_word_count = movieGenreReview_Action_2c2.map(lambda x:x[1]).sum()

# filter to keep only frequent words, i.e. those with
# count greater than frequent_word_threshold.
nonfreq_words = movieGenreReview_2a2.filter(lambda x:x[1]<frequent_word_threshold).cache()
# [(u'here', 788), (u'music', 352)]

# filter to keep only those word count tuples whose word can
# be found in the frequent list
movieGenre_3drama=nonfreq_words.join(movieGenreReview_Drama_2b2)
# [(u'looking', (310, 182)), (u'here', (788, 440))]

movieGenre_3comedy=nonfreq_words.join(movieGenreReview_Comedy_2c2)
# [(u'here', (788, 348)), (u'music', (352, 110))]

movieGenre_3action=nonfreq_words.join(movieGenreReview_Action_2c2)


# compute the log ratio score for each drama review word
unsorted_drama_words = movieGenre_3drama.map(lambda x: (x[0], math.log(float(x[1][1])/drama_review_word_count ) - math.log(float(x[1][0])/all_review_word_count)))
# [(u'looking', -0.047547627722522634), (u'here', -0.09770538026569664)]

# sort by descending score to get the top-scoring drama words
sorted_drama_words = unsorted_drama_words.sortBy(lambda x: x[1], ascending = False)

# compute the log ratio score for each comedy review word
unsorted_comedy_words = movieGenre_3comedy.map(lambda x:(x[0],math.log(float(x[1][1])/comedy_review_word_count) - math.log(float(x[1][0])/all_review_word_count)))
# [(u'here', 0.1390000673173084), (u'music', -0.20685513240496878)]

# sort by descending score to get the top-scoring comedy words
sorted_comedy_words = unsorted_comedy_words.sortBy(lambda x: x[1], ascending = False)

unsorted_action_words = movieGenre_3action.map(lambda x:(x[0],math.log(float(x[1][1])/action_review_word_count) - math.log(float(x[1][0])/all_review_word_count)))
sorted_action_words = unsorted_action_words.sortBy(lambda x: x[1], ascending = False)

# write out the top-scoring positive words to a text file
sorted_drama_words.saveAsTextFile("movieGenre_drama_words_output_nonfreq")
# write out the top-scoring negative words to a text file
sorted_comedy_words.saveAsTextFile("movieGenre_comedy_words_output_nonfreq")

sorted_action_words.saveAsTextFile("movieGenre_action_words_output_nonfreq")



################# Bonus Analysis without merging datasets: review vs fresh ##################################

movieReviewq1 = sqlContext.sql("""SELECT id, review, fresh
    FROM movieReviewTable
    WHERE review IS NOT NULL
    """)


movieReview_1a  = movieReviewq1.rdd.map(lambda row: row.asDict())

# [{'fresh': u'fresh', 'review': u"A distinctly gallows take on contemporary financial mores, as one absurdly rich man's limo ride across town for a haircut functions as a state-of-the-nation discourse. ", 'id': u'3'}]


frequent_word_threshold_negative=50
frequent_word_threshold_positive=200

WORD_RE = re.compile(r'\b[\w]+\b') 
def convert_dict_to_tuples(d):
        text = d['review']
        rating = d['fresh']
        tokens = WORD_RE.findall(text)
        tuples = []
        for w in tokens:
            tuples.append((rating, w))
        return tuples

movieReview_1b = movieReview_1a.flatMap(lambda x : convert_dict_to_tuples(x))
# [(u'fresh', u'A'), (u'fresh', u'distinctly')]


# count all words from all movieReviews
movieReview_2a2 = movieReview_1b.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
# [(u'fawn', 1), (u'unimaginative', 27)]
movieReview_2a2.count() # 39640


# filter out all word-tuples from positive reviews
movieReview_2b1=movieReview_1b.filter(lambda x:x[0]== 'fresh')
# [(u'fresh', u'A'), (u'fresh', u'distinctly')]


# count all words from positive reviews
movieReview_2b2 = movieReview_2b1.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
# [(u'funereal', 1), (u'Giammetti', 2)]
movieReview_2b2.count() # 31187

# filter out all word-tuples from negative reviews
movieReview_2c1 = movieReview_1b.filter(lambda x: x[0] == 'rotten')
# [(u'rotten', u'It'), (u'rotten', u's')]


# count all words from negative reviews
movieReview_2c2=movieReview_2c1.map(lambda x:(x[1],1)).reduceByKey(lambda a,b:a+b)
# [(u'fawn', 1), (u'funereal', 2)]
movieReview_2c2.count() # 26678


# get total word count for all, positive, and negative reviews
all_review_word_count = movieReview_2a2.map(lambda x:x[1]).sum() #1022802
pos_review_word_count = movieReview_2b2.map(lambda x:x[1]).sum() #629725
neg_review_word_count = movieReview_2c2.map(lambda x:x[1]).sum() #393077

# filter to keep only frequent words, i.e. those with
# count greater than frequent_word_threshold.
freq_words_negative=movieReview_2a2.filter(lambda x:x[1]>frequent_word_threshold_negative).cache()
freq_words_postive=movieReview_2a2.filter(lambda x:x[1]>frequent_word_threshold_positive).cache()
# [(u'here', 788), (u'music', 352)]

# filter to keep only those word count tuples whose word can
# be found in the frequent list
movieReview_3pos=freq_words_postive.join(movieReview_2b2)
# [(u'looking', (310, 182)), (u'here', (788, 440))]

movieReview_3neg=freq_words_negative.join(movieReview_2c2)
# [(u'here', (788, 348)), (u'music', (352, 110))]


# compute the log ratio score for each positive review word
unsorted_positive_words = movieReview_3pos.map(lambda x: (x[0], math.log(float(x[1][1])/pos_review_word_count ) - math.log(float(x[1][0])/all_review_word_count)))
# [(u'looking', -0.047547627722522634), (u'here', -0.09770538026569664)]

# sort by descending score to get the top-scoring positive words
sorted_positive_words = unsorted_positive_words.sortBy(lambda x: x[1], ascending = False)

# compute the log ratio score for each negative review word
unsorted_negative_words = movieReview_3neg.map(lambda x:(x[0],math.log(float(x[1][1])/neg_review_word_count) - math.log(float(x[1][0])/all_review_word_count)))
# [(u'here', 0.1390000673173084), (u'music', -0.20685513240496878)]

# sort by descending score to get the top-scoring negative words
sorted_negative_words = unsorted_negative_words.sortBy(lambda x: x[1], ascending = False)

# write out the top-scoring positive words to a text file
sorted_positive_words.saveAsTextFile("movieReview_positive_words_output")
# write out the top-scoring negative words to a text file
sorted_negative_words.saveAsTextFile("movieReview_negative_words_output")
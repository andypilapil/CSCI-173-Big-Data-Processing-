# SUBMITTED BY: ANDREA PILAPIL & MARI VALLE

# Imports necessary libraries
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark import SQLContext
from pyspark.sql.functions import col, udf

# import necessary libraries
from itertools import chain
import collections

# ML stuff
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer, IDF, Tokenizer, StopWordsRemover, NGram, RegexTokenizer, StringIndexer, VectorIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# create spark context
sc = SparkContext(appName="SparkMLEMRLRApp")

# create spark sql context
sqlContext = SQLContext(sc)

# read data from filesystem
contraceptionData = sqlContext.read.csv("s3://assignmeny14bucket/IR Data_Editted_ver3 - IR Data_Editted_ver3.csv", header = True, inferSchema = True).cache() 

# clean data
# let us start cleaning it.
# give all null values a value of -1
contraceptionDataStringReplaced = contraceptionData.na.replace("", "-1")

# cast a string column to float
contraceptionDataCasted = contraceptionDataStringReplaced.withColumn("Religion", col("Religion").cast("float"))

# use 2 to mean no answer since the classification model only accepts labels from 0,1,9
notNull = contraceptionDataCasted.fillna({ 'Used method in last 12 months':2 })

# Other features will be relabeled to -1
df = notNull.fillna(-1)

# split data
train, test = df.randomSplit([0.7, 0.3], seed=30)
train.show(5)

# assemble ML pipeline using LR
assembler = VectorAssembler(
    inputCols=['Age', 'Residence Type', 'Religion', 'Educ in single years', 'Encouraged FP', 'HH Head', 'Land Owner', 'Earns more', 'DM Contraception', 'Depression/ anxiety', 'Total CEB', 'Number of SC'],
    outputCol="features")
lr = LogisticRegression(featuresCol = 'features', labelCol = 'Used method in last 12 months', maxIter=10)
classificationPipeline = Pipeline(stages=[assembler, lr])
classificationPipelineModel = classificationPipeline.fit(train)
classified = classificationPipelineModel.transform(train)

# create ParamGrid for Cross Validation
paramGrid = (ParamGridBuilder()
             .addGrid(lr.regParam, [0.01, 0.25, 0.5, 1.0, 2.0])
             .addGrid(lr.elasticNetParam, [0.0, 0.25, 0.5, 0.75, 1.0])
             .addGrid(lr.maxIter, [1, 5, 10, 15, 20])
             .build())
prediction = classificationPipelineModel.transform(test)

# evaluate model based on default parameters and start training!
evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction", labelCol = "Used method in last 12 months")
print('Accuracy before Cross Validation ', evaluator.evaluate(prediction))

cv = CrossValidator(estimator=classificationPipeline, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=5)

cvModel = cv.fit(train)

# evaluate model after CV validation
predictions_cvModel = cvModel.transform(test)

print('Accuracy after Cross Validation: ', evaluator.evaluate(predictions_cvModel))

# destroy spark context
sc.stop()

# SUBMITTED BY: ANDREA PILAPIL & MARI VALLE
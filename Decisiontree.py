#importing the libraries for use.
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
import pandas as pd
from numpy import array
import math

#setting up spark configuration
conf=SparkConf().setMaster("local").setAppName("SparkDecisionTree")
sc=SparkContext(conf=conf)

#Reading dataset
df=pd.read_csv('data.csv')

#creating sql context of sc
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#Creating Data frame
s_df=sqlContext.createDataFrame(df)

#Creating traing data with runs column as labeledpoint
train_set  = s_df.rdd.map(lambda x: LabeledPoint(x[7], x[:7]))

#creating our decion tree model.
model1=DecisionTree.trainRegressor(train_set,categoricalFeaturesInfo={},impurity='variance',maxDepth=30,maxBins=128)


#saving our trained model in model_1
model1.save(sc,"model_1")






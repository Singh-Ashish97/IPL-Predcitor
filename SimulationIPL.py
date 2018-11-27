#importing libraries
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.util import MLUtils
from numpy import array
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import IntegerType
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import IntegerType
import numpy as np
import pandas as pd
import random
import matplotlib.pyplot as plt

#setting up spark configurations
conf=SparkConf().setMaster("local").setAppName("SparkDecsisionTree2")
sc=SparkContext(conf=conf)


#function to return Batsman stats
def batsman_function(name):
    test = pd.read_csv('stat_batsman.csv')
    ip_avg = 0
    ip_strike = 0
    newname = name
    flag = 0
    for i in range(len(test['Player'])):
        if (newname == test['Player'][i]):
            #print('yes')
            flag = 1
            ip_avg = test['Ave'][i]
            ip_strike = test['SR'][i]

    return ip_avg, ip_strike


#function to simulate the matches
def simulation(result,batsman_order,bowler_order):
    global bat_bowl_pair
    global strike
    global nstrike
    global wickets
    global ball
    global overs
    if(overs<len(bowler_order) and wickets < 11):
        bat_bowl_pair.append(strike + '-'+bowler_order[overs])
        print(strike + '-'+bowler_order[overs] +' '+str(overs)+'.'+str(ball) +' '+str(result) )
        if (result == 1 or result == 3 or result == 5):
            temp = strike
            strike = nstrike
            nstrike = temp
        elif(result == 0 or result ==2 or result ==4 or result ==6):
            pass
        else:
            wickets = wickets + 1
            strike = batsman_order[wickets]    
        
        ball = ball + 1
        if(ball > 6):
            ball = 1
            overs = overs + 1
            temp = strike
            strike = nstrike
            nstrike = temp
  


#function to return Bowler stats
def bowler_function(name):
    #print(name)
    newname2 = name
    test2 = pd.read_csv('stat_bowler.csv')
    flag = 0
    for i in range(len(test2['Player'])):
        if (newname2==test2['Player'][i]):
            flag = 1
            #print('resulti')
            avg2 = test2['Ave'][i]
            eco2 = test2['Econ'][i]
            strike2 = test2['SR'][i]
            return avg2, eco2, strike2
    
   

#loading the decision tree model saved from the Decisiontree.py
model = DecisionTreeModel.load(sc, "model_1")



#Inneings 1
bat_bowl_pair = []
batsman_order = bat_ord
bowler_order = bow_ord
strike = batsman_order[0]  
nstrike = batsman_order[1]
wickets = 1
bat_bowl_pair = [strike + '-' + bowler_order[0]]#creating batsman-bowler pair
ball = 1
overs = 0
#details of each ball appened into innings_det
innings_det = []
print(bat_bowl_pair)

for k in bat_bowl_pair:
    
    
        new_batsman = k.split('-')[0]
        new_bowler = k.split('-')[1]
        ba_av,ba_sr = batsman_function(new_batsman)#getting bowler stat
        bo_av,bo_eco,bo_sr = bowler_function(new_bowler)#getting batsman stat
        
        o_ball=float(str(overs)+'.'+str(ball))
        testing=[array([ba_av,ba_sr,bo_av,bo_eco,bo_sr,o_ball,1])]
        testing=sc.parallelize(testing)
        predicitons=model.predict(testing).collect()
        print(predicitons)
        
        result=round(predicitons[0])
        simulation(result,batsman_order,bowler_order)#calling function to get runs or wicket.
        if result==7:
            innings_det.append('out')
        else:    
            innings_det.append(result)
        

bat_bowl_pair.remove(bat_bowl_pair[0])
wkt = 0
runs = 0
for i in innings_det:
    if(i == 'out'):
        wkt = wkt + 1
    else:
        runs = runs + i

print(innings_det)
print('Runs: ',runs)        
print('wicketss: ',wkt)
print('Balls: ',len(innings_det)-1)


#Innings 2
bat_bowl_pair = []
batsman_order = bat22
bowler_order = bowl22
strike = batsman_order[0]  
nstrike = batsman_order[1]
wickets = 1
bat_bowl_pair = [strike + '-' + bowler_order[0]]
ball = 1
overs = 0

innings_det = []
print(bat_bowl_pair)

for k in bat_bowl_pair:
    
    
        new_batsman = k.split('-')[0]
        new_bowler = k.split('-')[1]
        ba_av,ba_sr = batsman_function(new_batsman)
        bo_av,bo_eco,bo_sr = bowler_function(new_bowler)
        
        o_ball=float(str(overs)+'.'+str(ball))
        testing=[array([ba_av,ba_sr,bo_av,bo_eco,bo_sr,o_ball,2])]
        testing=sc.parallelize(test_1)
        predicitons=model.predict(testing).collect()
        print(predicitons)
        
        result=round(predicitons_1[0])
        simulation(result,batsman_order,bowler_order)
        if result==7:
            innings_det.append('out')
        else:    
            innings_det.append(result)
        

bat_bowl_pair.remove(bat_bowl_pair[0])
wkt = 0
runs = 0
for i in innings_det:
    if(i == 'out'):
        wkt = wkt + 1
    else:
        runs = runs + i

print(innings_det)
print('Runs: ',runs)        
print('wicketss: ',wkt)
print('Balls: ',len(innings_det)-1)






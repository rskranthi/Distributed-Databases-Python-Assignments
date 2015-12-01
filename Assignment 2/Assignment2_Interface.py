#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
from Assignment1 import *



# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    name1 = "RangeRatingsPart"
    name2 = "RangeRatingsPart"
    name3 = "RoundRobinRatingsPart"
    conn = openconnection
    cur = conn.cursor()
    f = open("RangeQueryOut.txt",'a')
    
    # find tuples satisfying minimum and maximum values
    
    if ratingMaxValue == 0:
        return "No negative ratings supported"
    
    if ratingMinValue == 0:
                cur.execute("SELECT * FROM RangeRatingsMetadata WHERE '%f' >= MinRating AND '%f' <= MaxRating " %(ratingMinValue, ratingMinValue))
                partition_start = cur.fetchall()[0][0]
                
                cur.execute("SELECT PartitionNum FROM RangeRatingsMetadata WHERE PartitionNum > '%d' AND '%f' >= MinRating" %(partition_start,ratingMaxValue))
                other_partitions = cur.fetchall()
                max_value = 0
                for low in other_partitions:
                    if(low[0]>= max_value):
                        max_value = low[0]
                    
                
                
                name1 = name1 + str(partition_start)
                
                cur.execute("SELECT * FROM %s WHERE rating >= '%f' " %(name1,ratingMinValue))
                tuples = cur.fetchall()
                for value in tuples:
                    
                    s = name1 + "," + str(value[0]) + "," + str(value[1]) + "," + str(value[2]) + "\n"
                    
                    f.write(s)
                    
                while max_value > partition_start:
                    
                    cur.execute("SELECT * FROM %s WHERE rating <= '%f'" %(name2+str(max_value),ratingMaxValue))
                    tuples2 = cur.fetchall()
                    max_value = max_value-1
                    for value in tuples2:
                    
                        s = name2+str(max_value) + "," + str(value[0]) + "," + str(value[1]) + "," + str(value[2]) + "\n"
                        f.write(s)
                    
                
    
    if ratingMinValue > 0:
                cur.execute("SELECT * FROM RangeRatingsMetadata WHERE '%f' > MinRating AND '%f' <= MaxRating " %(ratingMinValue, ratingMinValue))
                partition_start = cur.fetchall()[0][0]
                
                cur.execute("SELECT PartitionNum FROM RangeRatingsMetadata WHERE PartitionNum > '%d' AND '%f' >= MinRating" %(partition_start,ratingMaxValue))
                other_partitions = cur.fetchall()
                max_value = 0
                for low in other_partitions:
                    if(low[0]>= max_value):
                        max_value = low[0]
                    
                
                
                name1 = name1 + str(partition_start)
                
                cur.execute("SELECT * FROM %s WHERE rating >= '%f' " %(name1,ratingMinValue))
                tuples3 = cur.fetchall()
                for value in tuples3:
                    
                    s = name1 + "," + str(value[0]) + "," + str(value[1]) + "," + str(value[2]) + "\n"
                    
                    f.write(s)
                while max_value > partition_start:
                    
                    cur.execute("SELECT * FROM %s WHERE rating <='%f'" %(name2+str(max_value),ratingMaxValue))
                    max_value = max_value-1
                    tuples4 = cur.fetchall()
                    for value in tuples4:
                    
                        s = name2+str(max_value) + "," + str(value[0]) + "," + str(value[1]) + "," + str(value[2]) + "\n"
                    
                        f.write(s)
    #To find round robin partitions
    cur.execute("SELECT * from RoundRobinRatingsMetadata")
    count = cur.fetchall()[0][0]
    
    
    count = count -1
    
    while count >= 0:
        
        cur.execute("SELECT * FROM %s WHERE rating >='%f' and rating <='%f'" %(name3+str(count),ratingMinValue,ratingMaxValue))
        count = count-1
        tuples_round_robin= cur.fetchall()
        for value in tuples_round_robin:
                    
                    s = name3+str(count) + "," + str(value[0]) + "," + str(value[1]) + "," + str(value[2]) + "\n"
                    
                    f.write(s)
    conn.commit()
    
    pass #Remove this once you are done with implementation

def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.
    conn = openconnection
    cur = conn.cursor()
    #query for finding values between min and max
    name1 = "RangeRatingsPart"
    
    name2 = "RoundRobinRatingsPart"
    conn = openconnection
    cur = conn.cursor()
    
    # find tuples satisfying minimum and maximum values
    f = open("PointQueryOut.txt",'a')
    if ratingValue < 0:
        return "No negative ratings supported"
    #To find round robin partitions
    cur.execute("SELECT * from RoundRobinRatingsMetadata")
    count = cur.fetchall()[0][0]
    #print count
    
    
    count = count -1
    
    while count >= 0:
        
        cur.execute("SELECT * FROM %s WHERE Rating ='%s'" %(name2+str(count),ratingValue))
        
        tuples_point_query = cur.fetchall()
        for value in tuples_point_query:
                    
                    s = name2+str(count) + "," + str(value[0]) + "," + str(value[1]) + "," + str(value[2]) + "\n"
                    
                    f.write(s)
        
        
        
        
        count = count-1
    
    
        
    cur.execute("SELECT * FROM RangeRatingsMetadata WHERE '%f' >= MinRating AND '%f' <= MaxRating " %(ratingValue, ratingValue))
    partition_start = cur.fetchall()[0][0]
    cur.execute("SELECT * FROM %s WHERE Rating ='%f'" %(name1+str(partition_start),ratingValue))
    tuples_point_query2 = cur.fetchall()
    for value in tuples_point_query2:
                    
                    s = name1+str(partition_start) + "," + str(value[0]) + "," + str(value[1]) + "," + str(value[2]) + "\n"
                    
                    f.write(s)
    pass # Remove this once you are done with implementation
if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        

        createDB(DATABASE_NAME)

        

        with getOpenConnection() as conn:
            

            # Here is where I will start calling your functions to test them. For example,
            loadRatings("ratings", "C:/Users/Kranthi/Downloads/Assignment2(1)/Assignment2/ratings2.txt", conn)
            rangePartition("ratings", 5, conn)
            roundRobinPartition("ratings", 5, conn)
            RangeQuery("ratings", 0, 3.4, conn)
            PointQuery("ratings", 0.8, conn)
            deleteTables("ratings", conn)
            
            
            
            pass #Remove this once you are done with implementation  
            #deleteTables("Ratings",con)
            
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
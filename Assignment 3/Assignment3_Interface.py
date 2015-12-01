#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    cur = openconnection.cursor()
    try:
       
        
        openconnection.commit()
        
        
        
        RangePart(InputTable,SortingColumnName,openconnection)
        Threads = []
        i=1
        while i<=5:
        
            t =threading.Thread(target=parallelSortFunc,args=(InputTable+"partition"+str(i),SortingColumnName,OutputTable+"partition"+str(i),con))
            Threads.append(t)
            Threads[i-1].start()
            i=i+1
            
        i=1
        while i<=5:
                
                Threads[i-1].join()
                i=i+1
        
        openconnection.commit()
        
       
        cur.execute("create table %s (like %s including defaults including constraints including indexes)"%(OutputTable,InputTable))
        
        openconnection.commit()
        
        i=1
        while i<=5:
            partition =OutputTable+"partition"+str(i) 
            
            cur.execute("Insert INTO %s (SELECT * FROM %s)"%(OutputTable,partition))
            i=i+1
        openconnection.commit() 
       # cur.execute("SELECT * FROM %s"%(OutputTable))
        #print cur.fetchall()
    except Exception as message:
            print(message)
    pass #Remove this once you are done with implementation

def parallelSortFunc (partitionTable,SortingColumnName,OutputTable,con):
    cur = con.cursor()
    cur.execute("drop table if exists %s"%(OutputTable))
    cur.execute("create table %s as (select * from %s where 1=2)"%(OutputTable,partitionTable))
    cur.execute("Insert INTO %s (SELECT * FROM %s order by %s)"%(OutputTable,partitionTable,SortingColumnName))
    con.commit()    
    
def RangePart(InputTable,SortingColumnName,openconnection):
    
    number_of_partitions = 5
    cur = openconnection.cursor()
    cur.execute("select max("+SortingColumnName+") from "+InputTable)
    
    max_value = cur.fetchone()[0]
   
    cur.execute("select min("+SortingColumnName+") from "+InputTable)
    min_value = cur.fetchone()[0]
    partition_variable = (float)(max_value - min_value)/number_of_partitions
    i=1
    
    partitionName = InputTable + "partition"
    while i<=5:
        tablename=partitionName+str(i)
        
        
        try:
            cur.execute("DROP TABLE IF EXISTS %s"%(tablename))
            
            
            if i ==1:
                
                cur.execute("select * into %s from %s where %s>='%f' AND %s<='%f'"%(tablename,InputTable,SortingColumnName,min_value,SortingColumnName,(min_value+partition_variable)))
                min_value = min_value + partition_variable
                openconnection.commit()
            else:
                 cur.execute("select * into %s from %s where %s>'%f' AND %s<='%f'"%(tablename,InputTable,SortingColumnName,min_value,SortingColumnName,(min_value+partition_variable)))
                 min_value = min_value + partition_variable
                 openconnection.commit()
                
           
        except Exception as message:
            print(message)
        openconnection.commit()
        i=i+1

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    cur=openconnection.cursor()
   
    # To Partition the input tables into specified number of partitions
    RangePart(InputTable1,Table1JoinColumn,openconnection)
    RangePart(InputTable2,Table2JoinColumn,openconnection)
    cur.execute("DROP TABLE IF EXISTS %s"%(OutputTable))
       
    i=1
    
    Threads=[]    
    
    cur.execute("SELECT * FROM %s LIMIT 0"%(InputTable1))
    colnames1 = [desc[0] for desc in cur.description]
    cur.execute("SELECT * FROM %s LIMIT 0"%(InputTable2))
    colnames2 = [desc[0] for desc in cur.description]
    
    if Table1JoinColumn==Table2JoinColumn:
        for Table1_Column in colnames1:
            
            for Table2_Column in colnames2:
                if (Table1_Column == Table2_Column) and  Table2_Column != Table2JoinColumn:
                    cur.execute("ALTER TABLE %s RENAME COLUMN %s to %s"%(InputTable2,Table2_Column,Table2_Column+str(2)))
                    openconnection.commit()
        
    else:
        for Table1_Column in colnames1:
            for Table2_Column in colnames2:
                if (Table1_Column == Table2_Column):
                    cur.execute("ALTER TABLE %s RENAME COLUMN %s to %s"%(InputTable2,Table2_Column,Table2_Column+str(2)))
    cur.execute("SELECT * FROM %s LIMIT 0"%(InputTable1))
    colnames1 = [desc[0] for desc in cur.description]
    cur.execute("SELECT * FROM %s LIMIT 0"%(InputTable2))
    colnames2 = [desc[0] for desc in cur.description]
    
    
    
    select_string = ""
    
    for Table1_Column in colnames1:
        select_string=select_string+Table1_Column+","
    for Table2_Column in colnames2:
        if(Table2_Column!=Table1JoinColumn):
            select_string=select_string+Table2_Column+","
    
    select_string = select_string[:-1]
    
    if Table1JoinColumn==Table2JoinColumn:
    
                
                cur.execute("create table %s as (select * from %s inner join %s using (%s) where 1=2)"%(OutputTable,InputTable1,InputTable2,Table1JoinColumn))
                
                openconnection.commit()
    else:
        
                cur.execute("create table %s as (select * from %s inner join %s on %s=%s and 1=2)"%(OutputTable,InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn))
                openconnection.commit()
    # To create multiple threads
    while(i<=5):
        partitionName1 = InputTable1 + "partition"+str(i)
        partitionName2 = InputTable2 + "partition"+str(i)
        
        t=threading.Thread(target=parallelJoinFunc,args=(partitionName1,partitionName2,Table1JoinColumn,Table2JoinColumn,OutputTable,openconnection))
        Threads.append(t)
        Threads[i-1].start()
        i=i+1
   
    
        
        
    while i<=5:
                
                Threads[i-1].join()
                i=i+1
    
    openconnection.commit()
    pass # Remove this once you are done with implementation

def parallelJoinFunc(partitionName1,partitionName2,Table1JoinColumn,Table2JoinColumn,OutputTable,openconnection):
    cur = openconnection.cursor()
    
    
    cur.execute("SELECT * FROM %s LIMIT 0"%(partitionName2))
   
    if Table1JoinColumn == Table2JoinColumn:
        #cur.execute("SELECT * FROM %s INNER JOIN %s USING (%s)"%(partitionName1,partitionName2,Table1JoinColumn))
        
        cur.execute("INSERT INTO %s (SELECT * FROM %s INNER JOIN %s USING (%s))"%(OutputTable,partitionName1,partitionName2,Table1JoinColumn))
        
        openconnection.commit()
        
    else:
        cur.execute("INSERT INTO %s (SELECT * FROM %s INNER JOIN %s ON %s=%s)"%(OutputTable,partitionName1,partitionName2,Table1JoinColumn,Table2JoinColumn))
        openconnection.commit()
        pass
################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    
    print "error"

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            con.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            con.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
    # Creating Database ddsassignment2
        print "Creating Database named as ddsassignment3"
        createDB();
    
    # Getting connection to the database
        print "Getting connection from the ddsassignment3 database"
        con = getOpenConnection();

    # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, "parallelSortOutputTable", con);

    # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
    
    # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

    # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail

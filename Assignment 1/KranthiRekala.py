import psycopg2
import math

DATABASE_NAME = 'dds_assgn1'
#round_robin_index = 0


def getopenconnection(user='postgres', password='tiger', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    con = openconnection
    db = con.cursor()
    db.execute("CREATE TABLE "+str(ratingstablename)+" (UserID int ,MovieID int,Rating float);")
    con.commit()
    with open(ratingsfilepath,'r') as f:
        for line in f:
            data = line.split('::') #data[0] has UserID,data[1] has MovieID,data[2] has Rating
            query_insert = "INSERT INTO "+str(ratingstablename)+" (UserID,MovieID,Rating) VALUES(%s,%s,%s);"
            values = (data[0],data[1],data[2])
            db.execute(query_insert,values)
            con.commit()
        db.close() 
        f.close()
    pass


def rangepartition(ratingstablename, numberofpartitions, openconnection):
     con = openconnection
     db = con.cursor() 
     range_variable = (5.0/numberofpartitions) # calculating the range partition variable
     i=0
     for i in range(0,numberofpartitions):
         db.execute("create table partition"+str(i+1)+"(UserId int,MovieId int,Rating float)")
         con.commit()
         if i==0:
            db.execute("insert into partition"+str(i+1)+ " select * from "+str(ratingstablename)+" where rating >= %s and rating <=%s" %((i*range_variable),((i+1)*range_variable)))
         else:
            db.execute("insert into partition"+str(i+1)+ " select * from "+str(ratingstablename)+" where rating > %s and rating <=%s" %((i*range_variable),((i+1)*range_variable)))
         con.commit()
     
     pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    db = con.cursor() 
     
     
    
    for i in range(0,numberofpartitions):
         db.execute("create table round_robin_partition"+str(i)+"(UserId int,MovieId int,Rating float)")
         con.commit()
    db.execute("SELECT * FROM "+str(ratingstablename))
    records = db.fetchall()
    robin = 0
    tuples = len(records)
    
    for i in range(0,tuples):
        query = "insert into round_robin_partition"+str(robin)+" VALUES(%s,%s,%s);"
        db.execute(query,records[i])
        robin += 1
        if(robin == numberofpartitions):
            robin = 0
        con.commit()
   
        
    db.execute("create table round_robin_value (robin int)")
    db.execute("insert into round_robin_value VALUES(%s)",str(robin))
    con.commit()
    return robin
    
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    if (rating >=0 and rating <=5.0):
        con = openconnection
        db = con.cursor()
        db.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'round_robin_partition%'")
        robin_partitions = db.fetchone()[0] #total number of round robin partitions
        db.execute("SELECT robin FROM round_robin_value")
        current_robin_partition = db.fetchone()[0]
       
        
       
       
        
        
    
        if (current_robin_partition < robin_partitions):
                current_robin_partition +=1
                db.execute("insert into round_robin_partition"+str((current_robin_partition-1))+" VALUES(%s,%s,%s);",(userid, itemid, rating))
           
            
        elif (current_robin_partition == robin_partitions):
            current_robin_partition = 0
            db.execute("insert into round_robin_partition"+str((current_robin_partition ))+" VALUES(%s,%s,%s);",(userid, itemid, rating))
            current_robin_partition +=1
            con.commit()
            
    db.execute("update round_robin_value set robin= %s"% (current_robin_partition))
    con.commit()
            
            
    pass
   
    


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if (rating >=0 and rating <=5.0):
        con = openconnection
        db = con.cursor()
        db.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'partition%'")
        range_partitions = db.fetchone()[0] #fetches the number of partitions that exist for range partition
    
        range_variable = 5.0/range_partitions
        partition_number = int(math.floor(rating/range_variable)) # getting the partition to which tuple is to be inserted
        db.execute("insert into partition"+str(partition_number)+" VALUES(%s,%s,%s);",(userid, itemid, rating))
        con.commit()
        pass
    else:
        return "Invalid Rating Entered"
    
def deletepartitionsandexit(openconnection):
    con = openconnection
    db = con.cursor()
    
    db.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'partition%'")
    range_partitions = db.fetchone()[0]
    db.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'round_robin_partition%'")
    round_robin_partitions = db.fetchone()[0]
    con.commit()
    for i in range(0,range_partitions):
        db.execute("DROP TABLE partition"+str(i))
        con.commit()
    for i in range(0,round_robin_partitions):
        db.execute("DROP TABLE round_robin_partition"+str(i))
        con.commit()
    
    
    con.commit()
    
    pass
def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass
    
if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings("Ratings", "C:/Users/Kranthi/Downloads/ml-10m/ml-10M100K/ratings2.txt", con)
            #rangepartition("Ratings", 5, con)
            #rangeinsert("Ratings",1,176,3.99,con)
            roundrobinpartition("Ratings", 5, con)
            roundrobininsert("Ratings", 1,115,1.25, con)
            roundrobininsert("Ratings", 2,115,1.25, con)
            roundrobininsert("Ratings", 3,115,1.25, con)
            roundrobininsert("Ratings", 4,115,1.25, con)
            #deletepartitionsandexit(con)
            
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
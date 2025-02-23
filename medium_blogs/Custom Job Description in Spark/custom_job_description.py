from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("CustomJobDescription").getOrCreate()
sc = spark.sparkContext  # Get SparkContext

### Run with default job description

df = spark.range(1, 1000000)  # Creating a dummy DataFrame
df.count()               # Triggering an action
df.distinct().count()     # Triggering another action

######## Let's set custom job description now

#First job trigger
sc.setJobDescription("First Count Trigger")
df.count()

#second job trigger
sc.setJobDescription("Second Count Trigger")
df.distinct().count()  

#Third job trigger
sc.setJobDescription("Third Count Trigger")
df.select('id').count()


############ If we don't update the job description, every other action we call will have the latest set job description itself

df.count()               ## Still shows 'Third Count Trigger' as job desc, as it is the latest set value
df.distinct().count()    ## Still shows 'Third Count Trigger' as job desc, as it is the latest set value
df.show()                ## Still shows 'Third Count Trigger' as job desc, as it is the latest set value

############ To avoid the error, please set the job description value to None
sc.setJobDescription(None)
df.select('id').count()   #Default job description will come up
df.show()                 #Default job description will come up



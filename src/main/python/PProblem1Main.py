import sys
from pyspark import SparkContext
from pyspark import SQLContext

if __name__ == '__main__':
    sc = SparkContext(appName='PProblem1Main')
    # YOUR CODE GOES HERE 
    # HAPPY CODING 
    sqlContext = SQLContext(sc)
    df = sqlContext.read.csv(sys.argv[1], header=True)    
    print(df.count()) 


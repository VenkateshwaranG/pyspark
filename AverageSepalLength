#import required headers 
irisRDD = SpContext.textFile("iris.csv")
irisRDD.cache()
irisRDD.count()
irisRDD.take(5)

#Create a transformation function
def xformIris( irisStr) :
    
    if ( irisStr.find("Sepal") != -1):
        return irisStr
        
    attList=irisStr.split(",")
        
    attList[4] = attList[4].upper()
    for i in range(0,4):
        attList[i] = str(round(float(attList[i])))
    
    return ",".join(attList)
    
xformedIris = irisRDD.map(xformIris)
xformedIris.take(5)


versiData = irisRDD.filter(lambda x: "versicolor" in x)
versiData.count()



is_float = lambda x: x.replace('.','',1).isdigit() and "." in x


def getSepalLength( irisStr) :
    
    if isinstance(irisStr, float) :
        return irisStr
        
    attList=irisStr.split(",")
    
    if is_float(attList[0]) :
        return float(attList[0])
    else:
        return 0.0

#Do a reduce to find the sum and then divide by no. of records.        
SepLenAvg=irisRDD.reduce(lambda x,y : getSepalLength(x) + getSepalLength(y)) \
    / (irisRDD.count()-1)
    
print(SepLenAvg)


#Create KV RDD
flowerData = irisRDD.map( lambda x: ( x.split(",")[4], \
    x.split(",")[0]))
flowerData.take(5)
flowerData.keys().collect()

#Remove header row
header = flowerData.first()
flowerKV= flowerData.filter(lambda line: line != header)
flowerKV.collect()

#find maximum of Sepal.Length by Species
maxData = flowerKV.reduceByKey(lambda x, y: max(float(x),float(y)))
maxData.collect()


#Initialize accumulator
sepalHighCount = SpContext.accumulator(0)

#Setup Broadcast variable
avgSepalLen = SpContext.broadcast(SepLenAvg)

#Write a function to do the compare and count
def findHighLen(line) :
    global sepalHighCount
    
    attList=line.split(",")
    
    if is_float(attList[0]) :
        
        if float(attList[0]) > avgSepalLen.value :
            sepalHighCount += 1
    return
    
#map for running the count. Also do a action to force execution of map
irisRDD.map(findHighLen).count()

print(sepalHighCount)

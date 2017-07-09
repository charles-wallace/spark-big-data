def cleanTestFeature(feature: String): Array[String] = {
// Remove Double Quotes and split string by commas
val cleanf = feature.replaceAll("\"\"","").split(",")
// variables to hold features
var id = ""
var q1 = ""
var q2 = ""
// Check the size of the array to make sure no extra commas are affecting split
if(cleanf.size > 3 && cleanf(1).count(_ == '"') == 2){
id = cleanf(0)
q1 = cleanf(1)
for(remaining <- 2 to (cleanf.size -1)){
q2 = q2 + cleanf(remaining)
}
}else if(cleanf.size > 3 && cleanf(1).count(_ == '"') == 1){
id = cleanf(0)
q1 = cleanf(1)
var i = 1
do{
i += 1
q1 = q1 + cleanf(i)
} while(cleanf(i).count(_ == '"') != 1)
for(remaining <- (i+1) to (cleanf.size -1)){
q2 = q2 + cleanf(remaining)
}
} else if(cleanf.size == 2){
id = cleanf(0)
q1 = cleanf(1)
q2 = " "
} else {
id = cleanf(0)
q1 = cleanf(1)
q2 = cleanf(2)
}
Array(id,q1.filterNot( s => (s == '"' || s == '?')), q2.filterNot( s => (s == '"' || s == '?')))
}//end cleanString Function

def cleanTrainFeature(feature: String): Array[String] = {
// Remove Double Quotes and split string by commas
val cleanf = feature.replaceAll("\"\"","").split(",")
// variables to hold features
var id = ""
var qid1 = ""
var qid2 = ""
var q1 = ""
var q2 = ""
var dup = ""
// Check the size of the array to make sure no extra commas are affecting split
if(cleanf.size > 6 && cleanf(3).count(_ == '"') == 2){
id = cleanf(0)
qid1 = cleanf(1)
qid2 = cleanf(2)
q1 = cleanf(3)
for(remaining <- 4 to (cleanf.size -2)){
q2 = q2 + cleanf(remaining)
}
dup = cleanf(cleanf.size-1)
}else if(cleanf.size > 6 && cleanf(3).count(_ == '"') == 1){
id = cleanf(0)
qid1 = cleanf(1)
qid2 = cleanf(2)
q1 = cleanf(3)
var i = 3
do{
i += 1
q1 = q1 + cleanf(i)
} while(cleanf(i).count(_ == '"') != 1)
for(remaining <- (i+1) to (cleanf.size -2)){
q2 = q2 + cleanf(remaining)
}
dup = cleanf(cleanf.size-1)
}else {
id = cleanf(0)
qid1 = cleanf(1)
qid2 = cleanf(2)
q1 = cleanf(3)
q2 = cleanf(4)
dup = cleanf(5)
}
Array(id.filterNot( s => (s == '"')),qid1.filterNot( s => (s == '"')), qid2.filterNot( s => (s == '"')), q1.filterNot( s => (s == '"' || s == '?')), q2.filterNot( s => (s == '"' || s == '?')),dup.filterNot( s => (s == '"')))
}//end cleanString Function


/** Fixing Test */
val rawTest = sc.textFile("test.csv")
val cleanTest = rawTest.map(line => cleanTestFeature(line))

def findTestFragment(line: String) = {
if(line.charAt(line.size-1) != '"' && line.charAt(line.size-2) != '?')
true
else
false
}

val indexedRawTest = rawTest.zipWithIndex.map{case (key, value) => (value, key)}
val missing = indexedRawTest.filter({case (key, value) => findTestFragment(value)})
missing.foreach(println)

/** Fixing Train */
val rawTrain = sc.textFile("train.csv")
val cleanTrain = rawTrain.map(line => cleanTrainFeature(line))

/** Finding Fragmented Data */
def findTrainFragment(line: String) = {
if((line.charAt(line.size-3) != '"' && line.charAt(line.size-3) != '0' &&  line.charAt(line.size-2) != '"') && ((line.charAt(line.size-3) != '"' && line.charAt(line.size-3) != '1' &&  line.charAt(line.size-2) != '"')))
true
else
false
}

val indexedRawTrain = rawTrain.zipWithIndex.map{case (key, value) => (value, key)}
val missing = indexedRawTrain.filter({case (key, value) => findTrainFragment(value)})
missing.foreach(println)


/** Creating Final Clean Train and Test CSVs*/
val rawTest = sc.textFile("test.csv")
val rawTrain = sc.textFile("train.csv")
val cleanTest = rawTest.map(testFeature => cleanTestFeature(testFeature)).collect
val cleanTrain = rawTrain.map(trainFeature => cleanTrainFeature(trainFeature)).collect

import java.io._
val fw1 = new FileWriter("FinalCleanTest.txt", true)
cleanTest.map(feature => fw1.write(feature(0) + "," + feature(1) + "," + feature(2) + "\n"))
fw1.close()

val fw2 = new FileWriter("FinalCleanTrain.txt", true)
cleanTrain.map(feature => fw2.write(feature(0) + "," + feature(1) + "," + feature(2) + "," + feature(3) + "," + feature(4) + "," + feature(5) + "\n"))
fw1.close()

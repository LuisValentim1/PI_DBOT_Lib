from DB import  *
from Cache import Cache
from JsonParser import JsonParser
import time

c = Cache(5)

register('luis', 'passLuis')
register('marta', 'passMarta')
#register('carlos', 'passCarlos')
#register('ze', 'passZe')
#register('maria', 'passMaria')
#register('joao', 'passJoao')

sessionLuis = sessionLogin('luis', 'passLuis')
sessionMarta = sessionLogin('marta', 'passMarta')
#sessionCarlos = sessionLogin('carlos', 'passCarlos')
#sessionZe = sessionLogin('ze', 'passZe')
#sessionMaria = sessionLogin('maria', 'passMaria')
#sessionJoao = sessionLogin('joao', 'passJoao')

#c.add(sessionCarlos[0], sessionCarlos[1])
#c.add(sessionZe[0], sessionZe[1])
#c.add(sessionMaria[0], sessionMaria[1])
#c.add(sessionJoao[0], sessionJoao[1])
c.add(sessionMarta[0], sessionMarta[1])
c.add(sessionLuis[0], sessionLuis[1])

print(c.cachedElements.keys())

startTimeParser = time.time()
Parser_Obj = JsonParser()
finishTimeParser = time.time()
print("Parser start --- %s seconds ---" % (finishTimeParser - startTimeParser))

startTimeP = time.time()
flat_j1 = Parser_Obj.read_json('Json_Examples\Json1.json')
flat_j2 = Parser_Obj.read_json('Json_Examples\Json2.json')
flat_j3 = Parser_Obj.read_json('Json_Examples\Json3.json')
flat_j4 = Parser_Obj.read_json('Json_Examples\Json4.json')
flat_j5 = Parser_Obj.read_json('Json_Examples\Json5.json')
flat_j6 = Parser_Obj.read_json('Json_Examples\Json6.json')
flat_j7 = Parser_Obj.read_json('Json_Examples\Json7.json')
flat_j8 = Parser_Obj.read_json('Json_Examples\Json8.json')
flat_j9 = Parser_Obj.read_json('Json_Examples\Json9.json')
flat_j10 = Parser_Obj.read_json('Json_Examples\Json10.json')
flat_j11 = Parser_Obj.read_json('Json_Examples\Json11.json')
flat_j12 = Parser_Obj.read_json('Json_Examples\Json12.json')
flat_j13 = Parser_Obj.read_json('Json_Examples\Json13.json')
flat_j14 = Parser_Obj.read_json('Json_Examples\Json14.json')
flat_j15 = Parser_Obj.read_json('Json_Examples\Json15.json')
flat_j16 = Parser_Obj.read_json('Json_Examples\Json16.json')
flat_j17 = Parser_Obj.read_json('Json_Examples\Json17.json')
flat_j18 = Parser_Obj.read_json('Json_Examples\Json18.json')
flat_j19 = Parser_Obj.read_json('Json_Examples\Json19.json')
flat_j20 = Parser_Obj.read_json('Json_Examples\Json20.json')
flat_j21 = Parser_Obj.read_json('Json_Examples\Json21.json')
finishTimeP = time.time()
print("Parsing --- %s seconds ---" % (finishTimeP - startTimeP))

startTimeI = time.time()
insertIntoSensor(c.get("luis"), flat_j1, 2)
insertIntoSensor(c.get("luis"), flat_j2, 1)
insertIntoSensor(c.get("marta"), flat_j3, 2)
insertIntoSensor(c.get("luis"), flat_j4, 3)
insertIntoSensor(c.get("luis"), flat_j5, 1)
insertIntoSensor(c.get("luis"), flat_j6, 4)
insertIntoSensor(c.get("marta"), flat_j7, 2)
insertIntoSensor(c.get("luis"), flat_j8, 4)
insertIntoSensor(c.get("marta"), flat_j9, 2)
insertIntoSensor(c.get("luis"), flat_j10, 1)
insertIntoSensor(c.get("marta"), flat_j11, 2)
insertIntoSensor(c.get("luis"), flat_j12, 10)
insertIntoSensor(c.get("luis"), flat_j13, 1)
insertIntoSensor(c.get("luis"), flat_j14, 18)
insertIntoSensor(c.get("luis"), flat_j15, 21)
insertIntoSensor(c.get("luis"), flat_j16, 1)
insertIntoSensor(c.get("luis"), flat_j17, 21)
insertIntoSensor(c.get("luis"), flat_j18, 12)
insertIntoSensor(c.get("luis"), flat_j19, 18)
insertIntoSensor(c.get("luis"), flat_j20, 21)
insertIntoSensor(c.get("marta"), flat_j21, 2)
insertIntoSensor(c.get("marta"), flat_j20, 2)
insertIntoSensor(c.get("marta"), flat_j19, 2)
insertIntoSensor(c.get("marta"), flat_j18, 2)
insertIntoSensor(c.get("marta"), flat_j17, 2)
insertIntoSensor(c.get("marta"), flat_j16, 2)
insertIntoSensor(c.get("marta"), flat_j15, 2)
insertIntoSensor(c.get("marta"), flat_j14, 2)
insertIntoSensor(c.get("luis"), flat_j15, 1)
insertIntoSensor(c.get("luis"), flat_j14, 3)
finishTimeI = time.time()
print("Inserting --- %s seconds ---" % (finishTimeI - startTimeI))

startTime1 = time.time()
queryResult1 = rangeQueryPerUser(c.get("luis"), ['temperature'], [['temperature','<','13']], '2020-06-01 10:10:10', '2021-08-01 22:36:20.785976')
finishTime1 = time.time()

startTime2 = time.time()
queryResult2 = queryPerUser(c.get("luis"), ['temperature'], [['temperature','<','13']])
finishTime2 = time.time()

startTime3 = time.time()
queryResult3 = rangeQueryPerUser(c.get("luis"), ['temperature'], [], '2020-05-30 10:10:10', '2021-08-01 22:36:20.785976')
finishTime3 = time.time()

startTime4 = time.time()
queryResult4 = queryPerUser(c.get("luis"), ['temperature'], [['temperature','=','12']])
finishTime4 = time.time()

startTime5 = time.time()
queryResult5 = queryPerSensor(c.get("marta"), '2', ['sensorid'], [['temperature','=','12']])
finishTime5 = time.time()

startTime6 = time.time()
queryResult6 = queryPerUser(c.get("luis"), ['AVG:temperature'], [['sensorid','=','1']])
finishTime6 = time.time()

startTime7 = time.time()
queryResult7 = queryPerUser(c.get("luis"), ['MIN:temperature'], [['sensorid','=','1']])
finishTime7 = time.time()

startTime8 = time.time()
queryResult8 = queryPerUser(c.get("luis"), ['MAX:temperature'], [['sensorid','=','1']])
finishTime8 = time.time()

startTime9 = time.time()
queryResult9 = queryPerUser(c.get("luis"), ['SUM:temperature'], [['sensorid','=','1']])
finishTime9 = time.time()

startTime10 = time.time()
queryResult10 = queryPerUser(c.get("luis"), ['CNT:temperature'], [['sensorid','=','1']])
finishTime10 = time.time()

startTime11 = time.time()
queryResult11 = queryPerUser(c.get("luis"), ['CNT:temperature'], [['sensorid','=','1'], ['timestamp', '>', '2020-06-02 10:10:10']])
finishTime11 = time.time()

startTime12 = time.time()
queryResult12 = rangeQueryPerSensor(c.get("marta"), '2', ['sensorid'],[['temperature','>','10'], ['sensorid', '=', '2']], '2020-06-02 10:10:10', '2021-08-01 22:36:20.785976')
finishTime12 = time.time()

startTime13 = time.time()
queryResult13 = queryPerUser(c.get("luis"), ['temperature'], [['temperature','>=','10']])
finishTime13 = time.time()

print("\n")
printResults(queryResult1)
print("Range Query --- %s seconds ---" % (finishTime1 - startTime1))
print("\n")
printResults(queryResult2)
print("Regular Query with the same conditions --- %s seconds ---" % (finishTime2 - startTime2))
print("\n")
printResults(queryResult3)
print("Query without conditions --- %s seconds ---" % (finishTime3 - startTime3))
print("\n")
printResults(queryResult4)
print("Regular Query --- %s seconds ---" % (finishTime4 - startTime4))
print("\n")
printResults(queryResult5)
print("Query Per Sensor --- %s seconds ---" % (finishTime5 - startTime5))
print("\n")
printResults(queryResult6)
print("Query Average --- %s seconds ---" % (finishTime6 - startTime6))
print("\n")
printResults(queryResult7)
print("Query Minimum --- %s seconds ---" % (finishTime7 - startTime7))
print("\n")
printResults(queryResult8)
print("Query Maximum --- %s seconds ---" % (finishTime8 - startTime8))
print("\n")
printResults(queryResult9)
print("Query Sum --- %s seconds ---" % (finishTime9 - startTime9))
print("\n")
printResults(queryResult10)
print("Query Count --- %s seconds ---" % (finishTime10 - startTime10))
print("\n")
printResults(queryResult11)
print("Query Count2 --- %s seconds ---" % (finishTime11 - startTime11))
print("\n")
printResults(queryResult12)
print("Range Query Per Sensor --- %s seconds ---" % (finishTime12 - startTime12))
print("\n")
printResults(queryResult13)
print("Query Test >= --- %s seconds ---" % (finishTime13 - startTime13))
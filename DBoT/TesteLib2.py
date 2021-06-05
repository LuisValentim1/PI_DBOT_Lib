from DB import  *
from Cache import Cache
from JsonParser import JsonParser
import time

c = Cache(1)

register('luis', 'passLuis')
sessionLuis = sessionLogin('luis', 'passLuis')

c.add(sessionLuis[0], sessionLuis[1])

startTimeParser = time.time()
Parser_Obj = JsonParser()
finishTimeParser = time.time()
print("Parser start --- %s seconds ---" % (finishTimeParser - startTimeParser))

startTimeP = time.time()
flat_j1 = Parser_Obj.read_json('Json_Examples\Json1.json')
flat_j2 = Parser_Obj.read_json('Json_Examples\Json2.json')
flat_j4 = Parser_Obj.read_json('Json_Examples\Json4.json')
flat_j5 = Parser_Obj.read_json('Json_Examples\Json5.json')
flat_j6 = Parser_Obj.read_json('Json_Examples\Json6.json')
flat_j8 = Parser_Obj.read_json('Json_Examples\Json8.json')
flat_j10 = Parser_Obj.read_json('Json_Examples\Json10.json')
flat_j12 = Parser_Obj.read_json('Json_Examples\Json12.json')
flat_j13 = Parser_Obj.read_json('Json_Examples\Json13.json')
flat_j14 = Parser_Obj.read_json('Json_Examples\Json14.json')
flat_j15 = Parser_Obj.read_json('Json_Examples\Json15.json')
flat_j16 = Parser_Obj.read_json('Json_Examples\Json16.json')
flat_j17 = Parser_Obj.read_json('Json_Examples\Json17.json')
flat_j18 = Parser_Obj.read_json('Json_Examples\Json18.json')
flat_j19 = Parser_Obj.read_json('Json_Examples\Json19.json')
flat_j20 = Parser_Obj.read_json('Json_Examples\Json20.json')
finishTimeP = time.time()
print("Parsing --- %s seconds ---" % (finishTimeP - startTimeP))

startTimeI = time.time()
insertIntoSensor(c.get("luis"), flat_j1, 2)
insertIntoSensor(c.get("luis"), flat_j2, 1)
insertIntoSensor(c.get("luis"), flat_j4, 3)
insertIntoSensor(c.get("luis"), flat_j5, 1)
insertIntoSensor(c.get("luis"), flat_j6, 4)
insertIntoSensor(c.get("luis"), flat_j8, 4)
insertIntoSensor(c.get("luis"), flat_j10, 1)
insertIntoSensor(c.get("luis"), flat_j12, 10)
insertIntoSensor(c.get("luis"), flat_j13, 1)
insertIntoSensor(c.get("luis"), flat_j14, 18)
insertIntoSensor(c.get("luis"), flat_j15, 21)
insertIntoSensor(c.get("luis"), flat_j16, 1)
insertIntoSensor(c.get("luis"), flat_j17, 21)
insertIntoSensor(c.get("luis"), flat_j18, 12)
insertIntoSensor(c.get("luis"), flat_j19, 18)
insertIntoSensor(c.get("luis"), flat_j20, 21)
insertIntoSensor(c.get("luis"), flat_j15, 1)
insertIntoSensor(c.get("luis"), flat_j14, 3)
finishTimeI = time.time()
print("Inserting --- %s seconds ---" % (finishTimeI - startTimeI))

startTime1 = time.time()
queryResult1 = rangeQueryPerUser(c.get("luis"), ['temperature'], [['temperature','>=','12']], '2020-06-02 10:10:10', '2021-06-05 14:31:05')
finishTime1 = time.time()

startTime2 = time.time()
queryResult2 = queryPerUser(c.get("luis"), ['temperature'], [['temperature','>=','12'],['timestamp','>=','2020-06-02 10:10:10'],['timestamp','<','2021-06-05 14:31:05'] ])
finishTime2 = time.time()

printResults(queryResult1)
print("Range Query User --- %s seconds ---" % (finishTime1 - startTime1))
print("\n")

printResults(queryResult2)
print("Query User --- %s seconds ---" % (finishTime2 - startTime2))
print("\n")
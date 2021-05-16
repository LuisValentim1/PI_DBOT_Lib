from DB import DB
from JsonParser import JsonParser
import time

DB_Obj = DB()
Parser_Obj = JsonParser()

flat_j1 = Parser_Obj.read_json('Json_Examples\Json1.json')
flat_j2 = Parser_Obj.read_json('Json_Examples\Json2.json')
flat_j3 = Parser_Obj.read_json('Json_Examples\Json3.json')
flat_j4 = Parser_Obj.read_json('Json_Examples\Json4.json')

DB_Obj.insertIntoSensor(flat_j1, 2, "Marta")
DB_Obj.insertIntoSensor(flat_j2, 1, "Luis")
DB_Obj.insertIntoSensor(flat_j3, 2, "Marta")
DB_Obj.insertIntoSensor(flat_j4, 3, "Luis")

print(DB_Obj.getAllValuesOn("temperature"))
print("---------------")
print(DB_Obj.getSensors("Luis"))
print("---------------")
print(DB_Obj.getAttributes())
print("---------------")
print(DB_Obj.getUsers())
print("---------------")
print(DB_Obj.getAllSensorsAttributes())
print("---------------")
print(DB_Obj.getSensorAttributes("Marta", 2))
print("---------------")


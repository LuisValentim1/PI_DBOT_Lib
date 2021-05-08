from cassandra.cluster import Cluster
import uuid
from datetime import datetime

class DB:

    #Inicializador
    def __init__(self):
        self.cluster = Cluster()
        self.session = self.cluster.connect('db')
        try:
            self.session.execute("create table metadata(atribute text, pk text, PRIMARY KEY(atribute, pk) )")
            self.session.execute("create table sensors(sensor_id text, user text, pkList list<text>, PRIMARY KEY(user, sensor_id) )")
        except:
            pass

    # Função para verificar se já existe tabela para um dado atributo
    def checkTable(self, atribute):
        
        atributeQuery = self.session.execute("SELECT atribute FROM metadata")   # Verifiar os atributos existentes na tabela de metadados
        atributeList = [row[0] for row in atributeQuery]

        if atribute in atributeList:                                            # Se o atributo a testar está na tabela de metadados retornar True
            return True 
        return False                                                            # Caso contrário retornar False

    # Função para criar tabelas
    def createTable(self, atribute):

        self.session.execute("create table " + atribute + "_table(pk text, " + atribute + " text, PRIMARY KEY( pk, " + atribute + "))")

                
    # Função de inserção de um json
    def insertInto(self, flatJson, pk_id):           # Os parametros são o json e a pk que será passada pela API
        if "timeStamp" not in flatJson:
            flatJson["timeStamp"] = str(datetime.now())

        for key in flatJson:
            if not self.checkTable(key):
                self.createTable(key)
            self.session.execute("insert into " + key + "_table(pk, " + key + ") values('" + pk_id + "', '" + flatJson[key] + "')")
            self.session.execute("insert into metadata(atribute, pk) values('" + key + "', '" + pk_id + "')")

    #Função de inserção num sensor
    def insertIntoSensor(self, flatJson, sensor_id, user):

        sensor_id = str(sensor_id)
        pk_id = str(uuid.uuid1())

        self.insertInto(flatJson, pk_id)                                                        # Inserir o registo com a função principal de inserção
        
        sensor = self.session.execute("SELECT * FROM sensors where user = '" + user +"' and sensor_id = '" + sensor_id + "'")   # Procurar pelo sensor na tabela de sensores
        pkList = []

        if not sensor:                                                                                                          # Caso o sensor ainda não exista na tabela sensors adicionar                       
            pkList.append(pk_id)
            self.session.execute("insert into sensors (sensor_id, user, pkList) values ('" + sensor_id + "', '" + user + "', " + str(pkList) + ")")  
        else:
            row = sensor.one()                                                                                                  # Se o sensor já existe mas esta formatação não é uma das associadas, adicionar ao parametro tables e pks            tables = row[3]
            pkList = row[2]
            
            if not pk_id in pkList:
                pkList.append(pk_id)
                self.session.execute("update sensors set pkList = " + str(pkList) + " where user = '" + user + "' and sensor_id = '" + sensor_id + "'")

    #Subqueries de apoio a querying complexo // Queries nas tabelas secundárias que permitem encontrar os pks.
    def subQuery(self, pk, param, condition):
        retList = []                                                            # Lista de pks a retornar
        condition = condition[0] + "'" + condition[1:len(condition)] + "'"      # Alterar a formatação da condição para ser compativel com cql
        pk_ret = None

        try:
            pkRow = self.session.execute("Select pk from " + param + "_table where pk= '" + pk + "' and " + param + condition)   # Executar a query secundária
            #print("Select pk from " + table + "_" + param + " where tableName= '" + table + "' and " + param + condition)
            pk_ret = pkRow.one()[0]
        except:
            pass

        return pk_ret

    # Função de querying por utilizador
    def queryPerUser(self, user, projList, paramConditionDictionary):

        userPkQuery = self.session.execute("select pkList from sensors where user = '" + user + "'")    # Procurar para um utilizador as tabelas e pks associados
        
        possiblePkLists = []
        userPkLists = [row[0] for row in userPkQuery]
        userPks = []
        atributes = [key for key in paramConditionDictionary]
        atributePkDict = {}

        for userPkList in userPkLists:
            for userPk in userPkList:
                userPks.append(userPk)

        userPks = list(dict.fromkeys(userPks))

        for atribute in atributes:
            atributePkQuery = self.session.execute("select pk from metadata where atribute='" + atribute + "'")
            atributePkDict[atribute] = []
            for row in atributePkQuery:
                if row[0] in userPks:
                    atributePkDict[atribute].append(row[0])

        for key in paramConditionDictionary:
            keyPkList = []
            for pk in atributePkDict[key]:
                keyPkList.append(self.subQuery(pk, key, paramConditionDictionary[key]))
            possiblePkLists.append(keyPkList)

        possiblePkLists = set(possiblePkLists[0]).intersection(*possiblePkLists)
        possiblePkLists.remove(None)

        retList = []

        for pk in possiblePkLists:                                                      # Para cada pk possivel encontrado nas subqueries
            regDict = {}
            for par in projList:
                strCommand = "select "
                strCommand = strCommand + self.agrHandler(par) + " from " + self.agrHandler(par) + "_table where pk = '" + pk + "'"
                result = self.session.execute(strCommand)
                if not result == None:
                    regDict[self.agrHandler(par)] = result.one()[0]
            if len(regDict) == len(projList):
                retList.append(regDict)

        agrFlag = self.agrCheck(projList)                                               # Verificar se existem agregações e selecionar a correta 
        if agrFlag == "AVG:":
            retList = self.averageHandler(retList, self.agrHandler(projList[0]))
        elif agrFlag == "MIN:":
            retList = self.minimumHandler(retList, self.agrHandler(projList[0]))
        elif agrFlag == "MAX:":
            retList = self.maximumHandler(retList, self.agrHandler(projList[0]))
        elif agrFlag == "SUM:":
            retList = self.sumHandler(retList, self.agrHandler(projList[0]))
        elif agrFlag == "CNT:":
            retList = self.countHandler(retList, self.agrHandler(projList[0]))
        elif agrFlag == "ERROR":
            retList = [] 

        return retList

    # Função de querying por sensor
    def queryPerSensor(self, user, sensor, projList, paramConditionDictionary):

        sensorPkQuery = self.session.execute("select pkList from sensors where user = '" + user + "' and sensor_id = '" + sensor + "'")   # Procurar para um utilizador as tabelas e pks associados
                                                     
        possiblePkLists = []
        sensorPks = sensorPkQuery.one()[0]
        atributes = [key for key in paramConditionDictionary]
        atributePkDict = {}

        for atribute in atributes:
            atributePkQuery = self.session.execute("select pk from metadata where atribute='" + atribute + "'")
            atributePkDict[atribute] = []
            for row in atributePkQuery:
                if row[0] in sensorPks:
                    atributePkDict[atribute].append(row[0])

        for key in paramConditionDictionary:
            keyPkList = []
            for pk in atributePkDict[key]:
                keyPkList.append(self.subQuery(pk, key, paramConditionDictionary[key]))
            possiblePkLists.append(keyPkList)

        possiblePkLists = set(possiblePkLists[0]).intersection(*possiblePkLists)
        possiblePkLists.remove(None)

        retList = []

        for pk in possiblePkLists:                                                      # Para cada pk possivel encontrado nas subqueries
            if pk in sensorPks:                                                       # Se este é um dos pks do utilizador executar a query nessa tabela por esse pk
                regDict = {}
                for par in projList:
                    strCommand = "select "   
                    strCommand = strCommand + self.agrHandler(par) + " from " + self.agrHandler(par) + "_table where pk = '" + pk + "'"
                    result = self.session.execute(strCommand)
                    if not result == None:
                        regDict[self.agrHandler(par)] = result.one()[0]
                if len(regDict) == len(projList):
                    retList.append(regDict)

        agrFlag = self.agrCheck(projList)                                               # Verificar se existem agregações e selecionar a correta 
        if agrFlag == "AVG:":
            retList = self.averageHandler(retList, self.agrHandler(projList[0]))
        elif agrFlag == "MIN:":
            retList = self.minimumHandler(retList, self.agrHandler(projList[0]))
        elif agrFlag == "MAX:":
            retList = self.maximumHandler(retList, self.agrHandler(projList[0]))
        elif agrFlag == "SUM:":
            retList = self.sumHandler(retList, self.agrHandler(projList[0]))
        elif agrFlag == "CNT:":
            retList = self.countHandler(retList, self.agrHandler(projList[0]))
        elif agrFlag == "ERROR":
            retList = [] 

        return retList

    # Função para verificar se os parametros possuem agregações e se sim retirar do parametro 
    def agrHandler(self, param):
        agrList = ["AVG:", "MIN:", "MAX:", "SUM:", "CNT:"]
        if param[0:4] in agrList:
            return param[4:len(param)]
        return param

    # Função para verificar se existem agregações e retirar a agregação
    def agrCheck(self, projectionList):
        ret = None
        agrList = ["AVG:", "MIN:", "MAX:", "SUM:", "CNT:"]

        for atribute in projectionList:
            if atribute[0:4] in agrList:
                if len(projectionList) == 1:
                    ret = atribute[0:4]
                else:
                    print("ERRO! Numero de parametros de visualização impróprio para agregação")
                    ret = "ERROR"
        return ret

    # Função para retirar a média dos resultados
    def averageHandler(self, returnList, atribute):

        if not returnList[0][atribute].isnumeric():
            print("Invalid parameter for average")
            return {}
        
        valueList = [int(Dict[atribute]) for Dict in returnList]
        ret = sum(valueList) / len(valueList)

        return [{atribute: str(ret)}]

    # Função para retirar o minimo dos resultados
    def minimumHandler(self, returnList, atribute):
        if not returnList[0][atribute].isnumeric():
            print("Invalid parameter for average")
            return {}
        
        ret = int(returnList[0][atribute])
        for Dict in returnList[1:len(returnList)]:
            if  ret > int(Dict[atribute]):
                ret = int(Dict[atribute])

        return [{atribute: str(ret)}]

    # Função para retirar o maximo dos resultados
    def maximumHandler(self, returnList, atribute):
        if not returnList[0][atribute].isnumeric():
            print("Invalid parameter for average")
            return {}
                
        ret = int(returnList[0][atribute])
        for Dict in returnList[1:len(returnList)]:
            if  ret < int(Dict[atribute]):
                ret = int(Dict[atribute])

        return [{atribute: str(ret)}]

    # Função para retirar a soma dos resultados
    def sumHandler(self, returnList, atribute):
        if not returnList[0][atribute].isnumeric():
            print("Invalid parameter for average")
            return {}

        valueList = [int(Dict[atribute]) for Dict in returnList]
        
        ret = sum(valueList)

        return [{atribute: str(ret)}]
    
    # Função para retirar o numero de resultados
    def countHandler(self, returnList, atribute):
        return [{atribute: str(len(returnList)) }]

    def getAllValuesOn(self, atribute):
        valueRows = self.session.execute("Select " + atribute + " from " + atribute + "_table")

        values = [row[0] for row in valueRows]

        return values

    # Função para mostrar os utilizadores existentes na base de dados
    def getUsers(self):
        userRows = self.session.execute("Select * from sensors")
        users = []
        for row in userRows:
            users.append(row[0])
        
        users = list(dict.fromkeys(users))

        return users

    def getSensors(self, user):
        userRows = self.session.execute("Select * from sensors where user = '" + user + "'")

        sensors = [row[1] for row in userRows]

        return sensors
    
    # Função para printar resultados
    def printResults(resultList):
        for Dict in resultList:
            printStr = ""
            for key in Dict:
                printStr = printStr + key + " - " + Dict[key] + "   "
            print(printStr)

from cassandra.cluster import Cluster
import uuid
from datetime import datetime

class DB:

    #Inicializador
    def __init__(self):
        self.cluster = Cluster()
        self.session = self.cluster.connect('db')
        try:
            self.session.execute("create table metadata(atribute text, pk text, timestamp text, PRIMARY KEY(atribute, timestamp, pk) )")
            self.session.execute("create table metadata_reverse(atribute text, pk text, timestamp text, PRIMARY KEY(atribute, pk, timestamp) )")
            self.session.execute("create table sensors(sensor_id text, user text, pk text, PRIMARY KEY(user, sensor_id, pk) )")
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
    def createTable(self, atribute, flag):
        if flag==1:
            self.session.execute("create table " + atribute + "_table(pk text, " + atribute + " int, PRIMARY KEY( pk, " + atribute + "))")
        else:
            self.session.execute("create table " + atribute + "_table(pk text, " + atribute + " text, PRIMARY KEY( pk, " + atribute + "))")

                
    # Função de inserção de um json
    def insertInto(self, flatJson, pk_id):
        #Se existe um timestamp associar se não criar um 
        timestampNow = ""
        if "timeStamp" in flatJson:
            timestampNow = flatJson["timeStamp"]
        else:
            timestampNow = str(datetime.now())

        # Para cada parametro inserir na tabela do atributo e ambas as tabelas de metadados 
        for key in flatJson:
            flag = 0
            keyLower = key.lower()
            if flatJson[key].isdigit():
                    flag = 1
            if not self.checkTable(keyLower):
                self.createTable(key, flag)
            if flag == 1:
                self.session.execute("insert into " + keyLower + "_table(pk, " + keyLower + ") values('" + pk_id + "', " + flatJson[key] + ")")
            else:
                self.session.execute("insert into " + keyLower + "_table(pk, " + keyLower + ") values('" + pk_id + "', '" + flatJson[key] + "')")
            self.session.execute("insert into metadata(atribute, pk, timestamp) values('" + keyLower + "', '" + pk_id + "', '" + timestampNow +"')")
            self.session.execute("insert into metadata_reverse(atribute, pk, timestamp) values('" + keyLower + "', '" + pk_id + "', '" + timestampNow +"')")

    #Função de inserção num sensor
    def insertIntoSensor(self, flatJson, sensor_id, user):

        sensor_id = str(sensor_id)
        pk_id = str(uuid.uuid1())

        self.insertInto(flatJson, pk_id)                                    # Inserir o registo com a função principal de inserção

        self.session.execute("insert into sensors (sensor_id, user, pk) values('" + sensor_id + "', '" + user +"', '" + pk_id + "')")

    #Subqueries de apoio a querying complexo // Procuram os pks que satisfazem uma condição em especifico 
    def subQuery(self, pk, param, condition):

        retList = []                                                            # Lista de pks a retornar

        if not condition[1:len(condition)].isdigit():
            condition = condition[0] + "'" + condition[1:len(condition)] + "'"      # Alterar a formatação da condição para ser compativel com cql
        
        pk_ret = None

        try:
            pkRow = self.session.execute("Select pk from " + param + "_table where pk= '" + pk + "' and " + param + condition)   # Executar a query secundária
            #print("Select pk from " + param + "_table where pk= '" + pk + "' and " + param + condition)
            pk_ret = pkRow.one()[0]
        except:
            pass

        return pk_ret

    # Função de querying por utilizador
    def queryPerUser(self, user, projList, paramConditionDictionary):
        possiblePkLists = []                                            # Lista de pks que passaram todas as condições
        userPks = [row[0] for row in self.session.execute("select pk from sensors where user = '" + user + "'")]
        atributes = list(paramConditionDictionary.keys()) + projList    # Atributos associados à query
        atributes = list(dict.fromkeys(atributes))
        atributePkDict = {}                                             # Dicionário atributo : lista de pks

        # Para cada atributo criar um dicionário de pks associados a esse atributo 
        for atribute in atributes:
            atributePkQuery = self.session.execute("select pk from metadata where atribute='" + atribute + "'")
            atributePkDict[atribute] = [row[0] for row in atributePkQuery if row[0] in userPks]

        # Para cada atributo das condições efetuar subqueries para defnir os pks válidos para a query
        for key in paramConditionDictionary:
            keyPkList = [self.subQuery(pk, key, paramConditionDictionary[key]) for pk in atributePkDict[key]]
            possiblePkLists.append(keyPkList)

        possiblePkLists = set(possiblePkLists[0]).intersection(*possiblePkLists)
        
        if None in possiblePkLists:
            possiblePkLists.remove(None)
    	
        # Formar os resultados
        retList = []
        for pk in possiblePkLists:                                                     
            extra = 0
            regDict = {}
            if "timestamp" not in projList:
                timestamp = self.session.execute("select timestamp from metadata_reverse where atribute='" + self.agrHandler(projList[0]) + "' and pk='" + pk + "'")
                if timestamp.one() is not None:
                    regDict["timestamp"] = timestamp.one()[0]
                    extra = 1
            for par in projList:
                handledPar = self.agrHandler(par)
                strCommand = "select "
                strCommand = strCommand + self.agrHandler(par) + " from " + handledPar + "_table where pk = '" + pk + "'"
                result = self.session.execute(strCommand)
                if result.one() is not None:
                    regDict[handledPar] = result.one()[0]
            if len(regDict) == len(projList)+extra:
                retList.append(regDict)

        # Verificar se existem agregações e selecionar a correta
        agrFlag = self.agrCheck(projList)                                                
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

    # Função de querying por utilizador dentro de uma range de valores de tempo
    def rangeQueryPerUser(self, user, projList, paramConditionDictionary, dateStart, dateFinish):
        possiblePkLists = []                                                                                            # Lista de pks que passaram todas as condições
        userPks = [row[0] for row in self.session.execute("select pk from sensors where user = '" + user + "'")]  
        atributes = list(paramConditionDictionary.keys()) + projList                                                    # Atributos associados à query
        atributes = list(dict.fromkeys(atributes))
        atributePkDict = {}                                                                                             # Dicionário atributo : lista de pks

        # Para cada atributo criar um dicionário de pks associados a esse atributo 
        for atribute in atributes:
            atributePkQueryStart = self.session.execute("select pk from metadata where atribute='" + atribute + "' and timestamp > '" + dateStart +"'")
            atributePkQueryFinish = self.session.execute("select pk from metadata where atribute='" + atribute + "' and timestamp < '" + dateFinish +"'")
            atributePkQuery = set([row[0] for row in atributePkQueryStart]).intersection([row[0] for row in atributePkQueryFinish])
            atributePkDict[atribute] = [pk for pk in atributePkQuery if pk in userPks]

        # Para cada atributo das condições efetuar subqueries para defnir os pks válidos para a query
        for key in paramConditionDictionary:
            keyPkList = [self.subQuery(pk, key, paramConditionDictionary[key]) for pk in atributePkDict[key]]
            possiblePkLists.append(keyPkList)

        possiblePkLists = set(possiblePkLists[0]).intersection(*possiblePkLists)
        
        if None in possiblePkLists:
            possiblePkLists.remove(None)

        # Formar os resultados
        retList = []
        for pk in possiblePkLists:                                                     
            extra = 0
            regDict = {}
            if "timestamp" not in projList:
                timestamp = self.session.execute("select timestamp from metadata_reverse where atribute='" + self.agrHandler(projList[0]) + "' and pk='" + pk + "'")
                if timestamp.one() is not None:
                    regDict["timestamp"] = timestamp.one()[0]
                    extra = 1
            for par in projList:
                handledPar = self.agrHandler(par)
                strCommand = "select "
                strCommand = strCommand + self.agrHandler(par) + " from " + handledPar + "_table where pk = '" + pk + "'"
                result = self.session.execute(strCommand)
                if result.one() is not None:
                    regDict[handledPar] = result.one()[0]
            if len(regDict) == len(projList)+extra:
                retList.append(regDict)

        # Verificar se existem agregações e selecionar a correta 
        agrFlag = self.agrCheck(projList)                                               
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
        possiblePkLists = []                                                                                                                            # Lista de pks que passaram todas as condições
        sensorPks = [row[0] for row in self.session.execute("select pk from sensors where user = '" + user + "' and sensor_id = '" + sensor + "'")]  
        atributes = list(paramConditionDictionary.keys()) + projList                                                                                    # Atributos associados à query
        atributes = list(dict.fromkeys(atributes))
        atributePkDict = {}                                                                                                                             # Dicionário atributo : lista de pks

        # Para cada atributo criar um dicionário de pks associados a esse atributo 
        for atribute in atributes:
            atributePkQuery = self.session.execute("select pk from metadata where atribute='" + atribute + "'")
            atributePkDict[atribute] = [row[0] for row in atributePkQuery if row[0] in sensorPks]

        # Para cada atributo das condições efetuar subqueries para defnir os pks válidos para a query
        for key in paramConditionDictionary:
            keyPkList = [self.subQuery(pk, key, paramConditionDictionary[key]) for pk in atributePkDict[key]]
            possiblePkLists.append(keyPkList)

        possiblePkLists = set(possiblePkLists[0]).intersection(*possiblePkLists)
        
        if None in possiblePkLists:
            possiblePkLists.remove(None)

        # Formar os resultados
        retList = []
        for pk in possiblePkLists:                                                     
            extra = 0
            regDict = {}
            if "timestamp" not in projList:
                timestamp = self.session.execute("select timestamp from metadata_reverse where atribute='" + self.agrHandler(projList[0]) + "' and pk='" + pk + "'")
                if timestamp.one() is not None:
                    regDict["timestamp"] = timestamp.one()[0]
                    extra = 1
            for par in projList:
                handledPar = self.agrHandler(par)
                strCommand = "select "
                strCommand = strCommand + self.agrHandler(par) + " from " + handledPar + "_table where pk = '" + pk + "'"
                result = self.session.execute(strCommand)
                if result.one() is not None:
                    regDict[handledPar] = result.one()[0]
            if len(regDict) == len(projList)+extra:
                retList.append(regDict)

        # Verificar se existem agregações e selecionar a correta 
        agrFlag = self.agrCheck(projList)                                            
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

    # Função de querying por sensor dentro de uma range de valores de tempo
    def rangeQueryPerSensor(self, user, sensor, projList, paramConditionDictionary, dateStart, dateFinish):
        possiblePkLists = []                                                                                                                            # Lista de pks que passaram todas as condições
        sensorPks = [row[0] for row in self.session.execute("select pk from sensors where user = '" + user + "' and sensor_id = '" + sensor + "'")]
        atributes = list(paramConditionDictionary.keys()) + projList                                                                                    # Atributos associados à query
        atributes = list(dict.fromkeys(atributes))
        atributePkDict = {}                                                                                                                             # Dicionário atributo : lista de pks

        # Para cada atributo criar um dicionário de pks associados a esse atributo 
        for atribute in atributes:
            atributePkQueryStart = self.session.execute("select pk from metadata where atribute='" + atribute + "' and timestamp > '" + dateStart +"'")
            atributePkQueryFinish = self.session.execute("select pk from metadata where atribute='" + atribute + "' and timestamp < '" + dateFinish +"'")
            atributePkQuery = set([row[0] for row in atributePkQueryStart]).intersection([row[0] for row in atributePkQueryFinish])
            atributePkDict[atribute] = [pk for pk in atributePkQuery if pk in sensorPks]

        # Para cada atributo das condições efetuar subqueries para defnir os pks válidos para a query
        for key in paramConditionDictionary:
            keyPkList = [self.subQuery(pk, key, paramConditionDictionary[key]) for pk in atributePkDict[key]]
            possiblePkLists.append(keyPkList)

        possiblePkLists = set(possiblePkLists[0]).intersection(*possiblePkLists)
        
        if None in possiblePkLists:
            possiblePkLists.remove(None)

        # Formar os resultados
        retList = []
        for pk in possiblePkLists:                                                     
            extra = 0
            regDict = {}
            if "timestamp" not in projList:
                timestamp = self.session.execute("select timestamp from metadata_reverse where atribute='" + self.agrHandler(projList[0]) + "' and pk='" + pk + "'")
                if timestamp.one() is not None:
                    regDict["timestamp"] = timestamp.one()[0]
                    extra = 1
            for par in projList:
                handledPar = self.agrHandler(par)
                strCommand = "select "
                strCommand = strCommand + self.agrHandler(par) + " from " + handledPar + "_table where pk = '" + pk + "'"
                result = self.session.execute(strCommand)
                if result.one() is not None:
                    regDict[handledPar] = result.one()[0]
            if len(regDict) == len(projList)+extra:
                retList.append(regDict)

        # Verificar se existem agregações e selecionar a correta 
        agrFlag = self.agrCheck(projList)                                               
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
        agr = projectionList[0][0:4]
        agrList = ["AVG:", "MIN:", "MAX:", "SUM:", "CNT:"]

        if agr in agrList:
            if len(projectionList) == 1:
                ret = agr
            else:
                print("ERRO! Numero de parametros de visualização impróprio para agregação")
                ret = "ERROR"

        return ret

    # Função para retirar a média dos resultados
    def averageHandler(self, returnList, atribute):
        valueList = [int(Dict[atribute]) for Dict in returnList]
        ret = sum(valueList) / len(valueList)

        return [{atribute: str(ret)}]

    # Função para retirar o minimo dos resultados
    def minimumHandler(self, returnList, atribute):
        ret = int(returnList[0][atribute])
        for Dict in returnList[1:len(returnList)]:
            if  ret > int(Dict[atribute]):
                ret = int(Dict[atribute])

        return [{atribute: str(ret)}]

    # Função para retirar o maximo dos resultados
    def maximumHandler(self, returnList, atribute):
        ret = int(returnList[0][atribute])
        for Dict in returnList[1:len(returnList)]:
            if  ret < int(Dict[atribute]):
                ret = int(Dict[atribute])

        return [{atribute: str(ret)}]

    # Função para retirar a soma dos resultados
    def sumHandler(self, returnList, atribute):
        valueList = [int(Dict[atribute]) for Dict in returnList]
        
        ret = sum(valueList)

        return [{atribute: str(ret)}]
    
    # Função para retirar o numero de resultados
    def countHandler(self, returnList, atribute):
        return [{atribute: str(len(returnList)) }]

    # Função para retirar todos os resultados de um certo atributo
    def getAllValuesOn(self, atribute):
        valueRows = self.session.execute("Select " + atribute + " from " + atribute + "_table")
        values = [row[0] for row in valueRows]

        return values

    # Função para mostrar os utilizadores existentes na base de dados
    def getUsers(self):
        userRows = self.session.execute("Select * from sensors")
        users = [row[0] for row in userRows]
        users = list(dict.fromkeys(users))

        return users

    # Função para retirar todos os sensores de um utilizador
    def getSensors(self, user):
        userRows = self.session.execute("Select * from sensors where user = '" + user + "'")
        sensors = list(dict.fromkeys([row[1] for row in userRows]))

        return sensors

    # Função para retirar todos os atributos existentes
    def getAtributes(self):
        atList = []
        atributeRows = self.session.execute("Select atribute from metadata")
        atList = list(dict.fromkeys([row[0] for row in atributeRows if row[0] not in atList]))
        return atList
    
    # Função para printar resultados
    def printResults(self, resultList):
        for Dict in resultList:
            printStr = ""
            for key in Dict:
                printStr = printStr + key + " - " + str(Dict[key]) + "   "
            print(printStr)

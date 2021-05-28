from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
from datetime import datetime

def register(user, passW):
    auth_provider = PlainTextAuthProvider(
        username='cassandra', password='cassandra')
    cluster = Cluster(auth_provider=auth_provider)

    session = cluster.connect('db')

    session.execute("create role " + user + " with password='" + passW + "' and login=true")
    session.execute("create keyspace db_" + user + " with replication = {'class':'SimpleStrategy', 'replication_factor' : 1}")
    session.execute("grant all on keyspace db_" + user + " to " + user)
    initializa(user,passW)
    

def initializa(user, passW):

    auth_provider = PlainTextAuthProvider(
        username=user, password=passW)
    cluster = Cluster(auth_provider=auth_provider)

    session = cluster.connect('db_'+user)

    session.execute("create table metadata(attribute text, pk text, timestamp text, PRIMARY KEY(attribute, timestamp, pk) )")
    session.execute("create table metadata_reverse(attribute text, pk text, timestamp text, PRIMARY KEY(attribute, pk, timestamp) )")
    session.execute("create table sensors(sensor_id text, user text, pk text, attributes list<text>, PRIMARY KEY(user, sensor_id, pk) )")

def sessionLogin(user, passW):
    auth_provider = PlainTextAuthProvider(
        username=user, password=passW)
    cluster = Cluster(auth_provider=auth_provider)

    session = cluster.connect('db_'+user)
    ret = [user, session]
    return ret

# Função para verificar se já existe tabela para um dado atributo
def checkTable(session, attribute):

    attributeQuery = session.execute("SELECT attribute FROM metadata")   # Verifiar os atributos existentes na tabela de metadados
    attributeList = [row[0] for row in attributeQuery]

    if attribute in attributeList:                                            # Se o atributo a testar está na tabela de metadados retornar True
        return True 
    return False                                                            # Caso contrário retornar False

# Função para criar tabelas
def createTable(session, attribute, flag):

    if flag==1:
        session.execute("create table " + attribute + "_table(pk text, " + attribute + " int, PRIMARY KEY( pk, " + attribute + "))")
    else:
        session.execute("create table " + attribute + "_table(pk text, " + attribute + " text, PRIMARY KEY( pk, " + attribute + "))")

            
# Função de inserção de um json
def insertInto(session, flatJson, pk_id):

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
        if not checkTable(session, keyLower):
            createTable(session, key, flag)
        if flag == 1:
            session.execute("insert into " + keyLower + "_table(pk, " + keyLower + ") values('" + pk_id + "', " + flatJson[key] + ")")
        else:
            session.execute("insert into " + keyLower + "_table(pk, " + keyLower + ") values('" + pk_id + "', '" + flatJson[key] + "')")
        session.execute("insert into metadata(attribute, pk, timestamp) values('" + keyLower + "', '" + pk_id + "', '" + timestampNow +"')")
        session.execute("insert into metadata_reverse(attribute, pk, timestamp) values('" + keyLower + "', '" + pk_id + "', '" + timestampNow +"')")

#Função de inserção num sensor
def insertIntoSensor(sessCache, flatJson, sensor_id):

    session = sessCache[1]
    user = sessCache[0]

    sensor_id = str(sensor_id)
    pk_id = str(uuid.uuid1())

    insertInto(session, flatJson, pk_id)                                    # Inserir o registo com a função principal de inserção

    keys = [key.lower() for key in flatJson.keys()]

    session.execute("insert into sensors (sensor_id, user, pk, attributes) values('" + sensor_id + "', '" + user +"', '" + pk_id + "', " + str(keys) + ")")

#Subqueries de apoio a querying complexo // Procuram os pks que satisfazem uma condição em especifico 
def subQuery(session, pk, param, condition):

    retList = []                                                            # Lista de pks a retornar

    if not condition[1:len(condition)].isdigit():
        condition = condition[0] + "'" + condition[1:len(condition)] + "'"      # Alterar a formatação da condição para ser compativel com cql
    
    pk_ret = None

    try:
        pkRow = session.execute("Select pk from " + param + "_table where pk= '" + pk + "' and " + param + condition)   # Executar a query secundária
        #print("Select pk from " + param + "_table where pk= '" + pk + "' and " + param + condition)
        pk_ret = pkRow.one()[0]
    except:
        pass

    return pk_ret

# Função de querying por utilizador
def queryPerUser(sessCache, projList, paramConditionDictionary):

    user = sessCache[0]
    session = sessCache[1]

    possiblePkLists = []                                            # Lista de pks que passaram todas as condições
    userPks = [row[0] for row in session.execute("select pk from sensors where user = '" + user + "'")]
    attributes = list(paramConditionDictionary.keys()) + projList    # Atributos associados à query
    attributes = list(dict.fromkeys(attributes))
    attributePkDict = {}                                             # Dicionário atributo : lista de pks

    # Para cada atributo criar um dicionário de pks associados a esse atributo 
    for attribute in attributes:
        attribute = agrHandler(attribute)
        attributePkQuery = session.execute("select pk from metadata where attribute='" + attribute + "'")
        attributePkDict[attribute] = [row[0] for row in attributePkQuery if row[0] in userPks]

    # Para cada atributo das condições efetuar subqueries para defnir os pks válidos para a query
    for key in paramConditionDictionary:
        keyPkList = [subQuery(session, pk, key, paramConditionDictionary[key]) for pk in attributePkDict[key]]
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
            timestamp = session.execute("select timestamp from metadata_reverse where attribute='" + agrHandler(projList[0]) + "' and pk='" + pk + "'")
            if timestamp.one() is not None:
                regDict["timestamp"] = timestamp.one()[0]
                extra = 1
        for par in projList:
            handledPar = agrHandler(par)
            strCommand = "select "
            strCommand = strCommand + handledPar + " from " + handledPar + "_table where pk = '" + pk + "'"
            result = session.execute(strCommand)
            if result.one() is not None:
                regDict[handledPar] = result.one()[0]
        if len(regDict) == len(projList)+extra:
            retList.append(regDict)

    # Verificar se existem agregações e selecionar a correta
    agrFlag = agrCheck(projList)                                                
    if agrFlag == "AVG:":
        retList = averageHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "MIN:":
        retList = minimumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "MAX:":
        retList = maximumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "SUM:":
        retList = sumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "CNT:":
        retList = countHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "ERROR":
        retList = [] 

    return retList

# Função de querying por utilizador dentro de uma range de valores de tempo
def rangeQueryPerUser(sessCache, projList, paramConditionDictionary, dateStart, dateFinish):

    user = sessCache[0]
    session = sessCache[1]

    possiblePkLists = []                                                                                            # Lista de pks que passaram todas as condições
    userPks = [row[0] for row in session.execute("select pk from sensors where user = '" + user + "'")]  
    attributes = list(paramConditionDictionary.keys()) + projList                                                    # Atributos associados à query
    attributes = list(dict.fromkeys(attributes))
    attributePkDict = {}                                                                                             # Dicionário atributo : lista de pks

    # Para cada atributo criar um dicionário de pks associados a esse atributo 
    for attribute in attributes:
        attribute = agrHandler(attribute)
        attributePkQueryStart = session.execute("select pk from metadata where attribute='" + attribute + "' and timestamp > '" + dateStart +"'")
        attributePkQueryFinish = session.execute("select pk from metadata where attribute='" + attribute + "' and timestamp < '" + dateFinish +"'")
        attributePkQuery = set([row[0] for row in attributePkQueryStart]).intersection([row[0] for row in attributePkQueryFinish])
        attributePkDict[attribute] = [pk for pk in attributePkQuery if pk in userPks]

    # Para cada atributo das condições efetuar subqueries para defnir os pks válidos para a query
    for key in paramConditionDictionary:
        keyPkList = [subQuery(session, pk, key, paramConditionDictionary[key]) for pk in attributePkDict[key]]
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
            timestamp = session.execute("select timestamp from metadata_reverse where attribute='" + agrHandler(projList[0]) + "' and pk='" + pk + "'")
            if timestamp.one() is not None:
                regDict["timestamp"] = timestamp.one()[0]
                extra = 1
        for par in projList:
            handledPar = agrHandler(par)
            strCommand = "select "
            strCommand = strCommand + handledPar + " from " + handledPar + "_table where pk = '" + pk + "'"
            result = session.execute(strCommand)
            if result.one() is not None:
                regDict[handledPar] = result.one()[0]
        if len(regDict) == len(projList)+extra:
            retList.append(regDict)

    # Verificar se existem agregações e selecionar a correta 
    agrFlag = agrCheck(projList)                                               
    if agrFlag == "AVG:":
        retList = averageHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "MIN:":
        retList = minimumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "MAX:":
        retList = maximumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "SUM:":
        retList = sumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "CNT:":
        retList = countHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "ERROR":
        retList = [] 

    return retList

# Função de querying por sensor
def queryPerSensor(sessCache, sensor, projList, paramConditionDictionary):

    user = sessCache[0]
    session = sessCache[1]

    possiblePkLists = []                                                                                                                            # Lista de pks que passaram todas as condições
    sensorPks = [row[0] for row in session.execute("select pk from sensors where user = '" + user + "' and sensor_id = '" + sensor + "'")]  
    attributes = list(paramConditionDictionary.keys()) + projList                                                                                    # Atributos associados à query
    attributes = list(dict.fromkeys(attributes))
    attributePkDict = {}                                                                                                                             # Dicionário atributo : lista de pks

    # Para cada atributo criar um dicionário de pks associados a esse atributo 
    for attribute in attributes:
        attribute = agrHandler(attribute)
        attributePkQuery = session.execute("select pk from metadata where attribute='" + attribute + "'")
        attributePkDict[attribute] = [row[0] for row in attributePkQuery if row[0] in sensorPks]

    # Para cada atributo das condições efetuar subqueries para defnir os pks válidos para a query
    for key in paramConditionDictionary:
        keyPkList = [subQuery(session, pk, key, paramConditionDictionary[key]) for pk in attributePkDict[key]]
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
            timestamp = session.execute("select timestamp from metadata_reverse where attribute='" + agrHandler(projList[0]) + "' and pk='" + pk + "'")
            if timestamp.one() is not None:
                regDict["timestamp"] = timestamp.one()[0]
                extra = 1
        for par in projList:
            handledPar = agrHandler(par)
            strCommand = "select "
            strCommand = strCommand + handledPar + " from " + handledPar + "_table where pk = '" + pk + "'"
            result = session.execute(strCommand)
            if result.one() is not None:
                regDict[handledPar] = result.one()[0]
        if len(regDict) == len(projList)+extra:
            retList.append(regDict)

    # Verificar se existem agregações e selecionar a correta 
    agrFlag = agrCheck(projList)                                            
    if agrFlag == "AVG:":
        retList = averageHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "MIN:":
        retList = minimumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "MAX:":
        retList = maximumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "SUM:":
        retList = sumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "CNT:":
        retList = countHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "ERROR":
        retList = [] 

    return retList

# Função de querying por sensor dentro de uma range de valores de tempo
def rangeQueryPerSensor(sessCache, sensor, projList, paramConditionDictionary, dateStart, dateFinish):
    
    user = sessCache[0]
    session = sessCache[1]

    possiblePkLists = []                                                                                                                            # Lista de pks que passaram todas as condições
    sensorPks = [row[0] for row in session.execute("select pk from sensors where user = '" + user + "' and sensor_id = '" + sensor + "'")]
    attributes = list(paramConditionDictionary.keys()) + projList                                                                                    # Atributos associados à query
    attributes = list(dict.fromkeys(attributes))
    attributePkDict = {}                                                                                                                             # Dicionário atributo : lista de pks

    # Para cada atributo criar um dicionário de pks associados a esse atributo 
    for attribute in attributes:
        attribute = agrHandler(attribute)
        attributePkQueryStart = session.execute("select pk from metadata where attribute='" + attribute + "' and timestamp > '" + dateStart +"'")
        attributePkQueryFinish = session.execute("select pk from metadata where attribute='" + attribute + "' and timestamp < '" + dateFinish +"'")
        attributePkQuery = set([row[0] for row in attributePkQueryStart]).intersection([row[0] for row in attributePkQueryFinish])
        attributePkDict[attribute] = [pk for pk in attributePkQuery if pk in sensorPks]

    # Para cada atributo das condições efetuar subqueries para defnir os pks válidos para a query
    for key in paramConditionDictionary:
        keyPkList = [subQuery(session, pk, key, paramConditionDictionary[key]) for pk in attributePkDict[key]]
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
            timestamp = session.execute("select timestamp from metadata_reverse where attribute='" + agrHandler(projList[0]) + "' and pk='" + pk + "'")
            if timestamp.one() is not None:
                regDict["timestamp"] = timestamp.one()[0]
                extra = 1
        for par in projList:
            handledPar = agrHandler(par)
            strCommand = "select "
            strCommand = strCommand + handledPar + " from " + handledPar + "_table where pk = '" + pk + "'"
            result = session.execute(strCommand)
            if result.one() is not None:
                regDict[handledPar] = result.one()[0]
        if len(regDict) == len(projList)+extra:
            retList.append(regDict)

    # Verificar se existem agregações e selecionar a correta 
    agrFlag = agrCheck(projList)                                               
    if agrFlag == "AVG:":
        retList = averageHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "MIN:":
        retList = minimumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "MAX:":
        retList = maximumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "SUM:":
        retList = sumHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "CNT:":
        retList = countHandler(retList, agrHandler(projList[0]))
    elif agrFlag == "ERROR":
        retList = [] 

    return retList

# Função para verificar se os parametros possuem agregações e se sim retirar do parametro 
def agrHandler(param):
    agrList = ["AVG:", "MIN:", "MAX:", "SUM:", "CNT:"]
    if param[0:4] in agrList:
        return param[4:len(param)]
    return param

# Função para verificar se existem agregações e retirar a agregação
def agrCheck(projectionList):
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
def averageHandler( returnList, attribute):
    valueList = [int(Dict[attribute]) for Dict in returnList]
    ret = sum(valueList) / len(valueList)

    return [{attribute: str(ret)}]

# Função para retirar o minimo dos resultados
def minimumHandler( returnList, attribute):
    ret = int(returnList[0][attribute])
    for Dict in returnList[1:len(returnList)]:
        if  ret > int(Dict[attribute]):
            ret = int(Dict[attribute])

    return [{attribute: str(ret)}]

# Função para retirar o maximo dos resultados
def maximumHandler( returnList, attribute):
    ret = int(returnList[0][attribute])
    for Dict in returnList[1:len(returnList)]:
        if  ret < int(Dict[attribute]):
            ret = int(Dict[attribute])

    return [{attribute: str(ret)}]

# Função para retirar a soma dos resultados
def sumHandler( returnList, attribute):
    valueList = [int(Dict[attribute]) for Dict in returnList]
    
    ret = sum(valueList)

    return [{attribute: str(ret)}]

# Função para retirar o numero de resultados
def countHandler( returnList, attribute):
    return [{attribute: str(len(returnList)) }]

# Função para retirar todos os resultados de um certo atributo
def getAllValuesOn(sessCache, attribute):

    session = sessCache[1]

    valueRows = session.execute("Select " + attribute + " from " + attribute + "_table")
    values = [row[0] for row in valueRows]

    return values

# Função para mostrar os utilizadores existentes na base de dados
def getUsers(sessCache):

    session = sessCache[1]

    userRows = session.execute("Select * from sensors")
    users = [row[0] for row in userRows]
    users = list(dict.fromkeys(users))

    return users

# Função para retirar todos os sensores de um utilizador
def getSensors(sessCache):
    
    user = sessCache[0]
    session = sessCache[1]

    userRows = session.execute("Select * from sensors where user = '" + user + "'")
    sensors = list(dict.fromkeys([row[1] for row in userRows]))

    return sensors

def getAllSensorsAttributes(sessCache):
    session = sessCache[1]

    sensorAttributes = [[row[0], row[1], row[2]] for row in session.execute("select user, sensor_id, attributes from sensors")]
    sensorAttributesUnique = []
    for sensor in sensorAttributes:
        if not sensor in sensorAttributesUnique:
            sensorAttributesUnique.append(sensor)
    return sensorAttributesUnique

def getSensorAttributes(sessCache, sensor_id):
    
    user = sessCache[0]
    session = sessCache[1]

    sensorAttributes = [[row[0], row[1], row[2]] for row in session.execute("select user, sensor_id, attributes from sensors where user = '" + user + "' and sensor_id = '" + str(sensor_id) + "'")]
    sensorAttributesUnique = []
    for sensor in sensorAttributes:
        if not sensor in sensorAttributesUnique:
            sensorAttributesUnique.append(sensor)    
    return sensorAttributesUnique

# Função para retirar todos os atributos existentes
def getAttributes(sessCache):

    session = sessCache[1]

    atList = []
    atributeRows = session.execute("Select attribute from metadata")
    atList = list(dict.fromkeys([row[0] for row in atributeRows if row[0] not in atList]))
    return atList

# Função para printar resultados
def printResults(resultList):
    for Dict in resultList:
        printStr = ""
        for key in Dict:
            printStr = printStr + key + " - " + str(Dict[key]) + "   "
        print(printStr)

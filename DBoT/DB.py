from cassandra.cluster import Cluster
from datetime import datetime

class DB:

    #Inicializador
    def __init__(self):
        self.cluster = Cluster()
        self.session = self.cluster.connect('db')
        try:
            self.session.execute("create table metadata (tableName text, tableAtributes list<text>, PRIMARY KEY(tableName) )")
            self.session.execute("create table tableNum (pk int, num int, PRIMARY KEY(pk) )")
            self.session.execute("create table sensors (sensor_id text,user text ,tables list<text>, pks list<text>, PRIMARY KEY(user, sensor_id) )")
        except:
            pass
        if self.session.execute("select * from tableNum").one() == None:
            self.session.execute("insert into tableNum(pk, num) values(1, 1)") 

    # Função para verificar se já existe uma tabela na qual o json possa ser inserido
    def checkTable(self, flatJson):
        tableName = None
        lowerParList = []                                           # Criar uma lista com todos os parametros do flat json em minisculas => Porque o cassandra vai ter as colunas em miniscula
        for par in flatJson.keys():
            lowerParList.append(par.lower())

        lowerParList.append('pk')                                   # Adicionar pk porque estará em todas as tabelas

        tableAts = self.session.execute("SELECT * FROM metadata")   # Verifiar os dados da tabela de metadados

        for row in tableAts:
            if set(row[1]) == set(lowerParList):                    # Se existir alguma tabela que já possua a mesma formatação retornar o seu nome
                tableName = row[0]
        return tableName                                            # Caso contrário retorna None

    # Função para criar tabelas
    def createTables(self, flatJson):
        tableNumber = self.session.execute("select num from tableNum").one()[0]
        table_name = "table" + str(tableNumber)              # Gerar nome da tabela principal // Não sei se este será a forma como vamos gerar nomes // Solução provisória
        self.session.execute("update tableNum set num =" + str(tableNumber+1) + " where pk=1")

        strCommand = "create table " + table_name + "(pk text"      # Começar a string de comando que cria a tabela principal
        lowerParList = []                                           # Lista de parametros / Para colocar na tabela de metadados

        for key in flatJson.keys():                                 # Para cada chave do flatJson
            key = key.lower()
            atrTables = self.session.execute("SELECT table_name from system_schema.tables where keyspace_name='db'")
            atrList = [atr[0] for atr in atrTables]                 #Lista das tabelas secundárias que já possuem tabela criada
            if not key + '_table' in atrList :
                self.session.execute("create table " + key + "_table(tableName text, pk text," + key + " text, PRIMARY KEY( tableName, " + key + ", pk))")
            lowerParList.append(key)                                # Recolher os parametros para a tabela de metadados
            strCommand = strCommand + ", " + key + " text"          # Adicionar à string do comando que cria a tabela principal
            
        lowerParList.append('pk')
        strCommand = strCommand + ", PRIMARY KEY(pk)) "             # Acabar a string de comando após todos os parametros serem adicionados e correr o comando
        self.session.execute(strCommand)

        self.session.execute("insert into metadata(tableName, tableAtributes) values ('" + table_name + "', " + str(lowerParList) + ")")                # Adicionar a a tabela principal à tabela de metadados
        return table_name                                           # Retorna o table_name da tabela principal criada
        
    # Função de inserção de um json
    def insertInto(self, flatJson, pk_id, sensor_id):           # Os parametros são o json e a pk que será passada pela API
        table_name = self.checkTable(flatJson)                  # Verificar se existe uma tabela em que os dados possam ser inseridos
        
        if not table_name:
            table_name = self.createTables(flatJson)            # Caso não exista chamar a função que cria que retorna o seu nome (da principal)

        new_table_name = table_name

        strInsert = "insert into " + table_name + "(pK"         # strInsert é a string de instrução / O que escreveriamos se estivessemos a inserir por terminal

        for key in flatJson.keys():
            strInsert = strInsert + ", " + key

        strInsert = strInsert + ") values('" + pk_id + "'"      # Acabar os campos da tabela e começar a inserir os valores na string

        for key in flatJson.keys():                             # Ciclo para adicionar os valores à string de instrução
            strInsert = strInsert + ", '" + flatJson[key] + "'"

        strInsert = strInsert + ")"                             # Fechar a string

        print(strInsert)                                        # Print e executar o comando
        self.session.execute(strInsert)

        self.insertSecondaryTables(table_name, flatJson, pk_id) # Inserir os dados nas tabelas adicionais para se poder fazer querying complexo
        return new_table_name

    # Função de inserção nas tabelas secundárias
    def insertSecondaryTables(self, table, flatJson, pk):
        for key in flatJson.keys():                 # Para cada parametro do flatJson adicionar a informação às tabelas adicionais e atualizar a metadata_atributes
            insertStr = "insert into " + key + "_table (tableName, pK, " + key + ") values('" + table + "','" + pk + "', '" + flatJson[key] + "')"
            self.session.execute(insertStr)         # Inserção de dados nas tabelas secundárias

    #Função de inserção num sensor
    def insertIntoSensor(self, flatJson, sensor_id, user):
        sensor_id = str(sensor_id)
        pk_id = sensor_id + user + str(datetime.now())
        new_table_name = self.insertInto(flatJson, pk_id, sensor_id)                                                            # Inserir o registo com a função principal de inserção
        sensor = self.session.execute("SELECT * FROM sensors where user = '" + user +"' and sensor_id = '" + sensor_id + "'")   # Procurar pelo sensor na tabela de sensores
        tables = []
        pks = []

        if not sensor:                                                                                                          # Caso o sensor ainda não exista na tabela sensors adicionar                       
            tables.append(new_table_name)
            pks.append(pk_id)
            self.session.execute("insert into sensors (sensor_id, user, tables, pks) values ('" + sensor_id + "', '" + user + "', " + str(tables) + ", " + str(pks) + ") ")  
        else:
            row = sensor.one()                                                                                                  # Se o sensor já existe mas esta formatação não é uma das associadas, adicionar ao parametro tables e pks            tables = row[3]
            pks = row[2]
            tables = row[3]
            
            if not pk_id in pks:
                pks.append(pk_id)
                self.session.execute("update sensors set pks = " + str(pks) + " where user = '" + user + "' and sensor_id = '" + sensor_id + "'")

            if not new_table_name in tables:
                tables.append(new_table_name)
                self.session.execute("update sensors set tables = " + str(tables) + " where user = '" + user + "' and sensor_id = '" + sensor_id + "'")

    #Subqueries de apoio a querying complexo // Queries nas tabelas secundárias que permitem encontrar os pks.
    def subQuery(self, table, param, condition):
        retList = []                                                            # Lista de pks a retornar
        condition = condition[0] + "'" + condition[1:len(condition)] + "'"      # Alterar a formatação da condição para ser compativel com cql

        try:
            pkRows = self.session.execute("Select pk from " + param + "_table where tableName= '" + table + "' and " + param + condition)   # Executar a query secundária
            #print("Select pk from " + table + "_" + param + " where tableName= '" + table + "' and " + param + condition)

            for row in pkRows:                                                  # Para cada pk retornado adicionar a lista de retorno
                retList.append(row[0])
        except:
            pass

        return retList

    # Função de querying geral
    def query(self, table, projList, paramConditionDictionary):
        pkLists = []                                                # Lista de listas de pks validadados pelas subqueries

        strCommand = "select "                                      # Adicionar ao comando os parametros que desejamos procurar
        for par in projList:
            strCommand = strCommand + self.agrHandler(par)
            if not projList[len(projList) - 1] == par:
                strCommand = strCommand + ','

        strCommand = strCommand + ' from ' + table + ' where '      # Adicionar ao comando a tabela principal em que desejamos procurar
        # print(strCommand)

        for key in paramConditionDictionary:                        # Para cada conjunto de parametro e condição pelos quais queremos filtrar fazer uma subquery nas tabelas secundárias
            value = paramConditionDictionary[key]
            pkLists.append(
                self.subQuery(table, key, value))                   # Adicionar a lista de resultados dessas subqueries à lista de listas das pks

        # print(pkLists)

        pks = set(pkLists[0]).intersection(*pkLists)                # Fazer a interceção de todas as listas para verificar que parametros correspondem a todas as filtragens
        # print(pks)

        retList = []

        for pk in pks:                                              # Pesquisar na tabela principal e apresentar os resultados
            finalCommand = strCommand + "pk='" + pk + "'"
            # print(finalCommand)
            result = self.session.execute(finalCommand)
            retList.append(result.one())

        agrFlag = self.agrCheck(projList)                           # Verificar se existem agregações e selecionar a correta 
        if agrFlag == "AVG:":
            retList = self.averageHandler(retList)
        elif agrFlag == "MIN:":
            retList = self.minimumHandler(retList)
        elif agrFlag == "MAX:":
            retList = self.maximumHandler(retList)
        elif agrFlag == "SUM:":
            retList = self.sumHandler(retList)
        elif agrFlag == "CNT:":
            retList = self.countHandler(retList)
        elif agrFlag == "ERROR":
            retList = [] 
        
        return retList

    # Função de querying por utilizador
    def queryPerUser(self, user, projList, paramConditionDictionary):
        pkDict = {}                                                                                         # Lista de listas de pks validadados pelas subqueries

        tableQuery = self.session.execute("select tables, pks from sensors where user = '" + user + "'")    # Procurar para um utilizador as tabelas e pks associados
                
        tableLists = []
        userPkLists = []

        atributes = []                                                                  # Recolher todos os atributos necessários nesta query
        for key in paramConditionDictionary:
            atributes.append(key)
        if not projList[0] == '*':
            for atr in projList:
                atributes.append(self.agrHandler(atr))

        possibleTables = []                                                             # Através da tabela metadata_atributes perceber que tabelas possuem todos os atributos necessários ou seja que tabelas poderiam satisfazer a query
        for atr in atributes:
            atributeQuery = self.session.execute("select tablename from " + atr + "_table")
            atrTables = [table[0] for table in atributeQuery]
            possibleTables.append(atrTables)

        for row in tableQuery:                                              
            tableLists.append(row[0])
            userPkLists.append(row[1])

        tables = []
        userPks = []

        for tableList in tableLists:
            for table in tableList:
                tables.append(table)

        for userPkList in userPkLists:
            for userPk in userPkList:
                userPks.append(userPk)

        tables = list(dict.fromkeys(tables))                                            # Criar um set com valores unicos para as tabelas do utilizador e outro para os pks
        userPks = list(dict.fromkeys(userPks))

        if len(possibleTables) == 0:                                                    # Lidar com o caso especifico em que projList = ['*'] e não existem condições de filtragem
            possibleTables = tables
        else:
            possibleTables = set(possibleTables[0]).intersection(*possibleTables)       #Interseção das tabelas possiveis para a lista conter apenas tabelas que possuem todos os atributos

        for table in possibleTables:                                                    # Para cada tabela possivel se for uma tabela do utilizador efetuar as subqueries para obter os pks possiveis
            if table in tables:
                pkLists = []
                for key in paramConditionDictionary:                                    # Para cada conjunto de parametro e condição pelos quais queremos filtrar fazer uma subquery nas tabelas secundárias
                    value = paramConditionDictionary[key]
                    pkLists.append(self.subQuery(table, key, value))                    # Adicionar a lista de resultados dessas subqueries à lista de listas das pks
                pkLists = set(pkLists[0]).intersection(*pkLists)                        # Criar um set de pks que passaram todos os requisitos associados a dada tabela
                pkDict[table] = pkLists

        retList = []

        for key in pkDict:                                                              # Para cada tabela do utilizador
            for pk in pkDict[key]:                                                      # Para cada pk possivel encontrado nas subqueries
                if pk in userPks:                                                       # Se este é um dos pks do utilizador executar a query nessa tabela por esse pk
                    strCommand = "select "                                              # Adicionar ao comando os parametros que desejamos procurar
                    for par in projList:
                        strCommand = strCommand + self.agrHandler(par)
                        if not projList[len(projList) - 1] == par:
                            strCommand = strCommand + ','
                    strCommand = strCommand + " from " + key + " where pk='" + pk + "'" # Adicionar ao comando a tabela principal em que desejamos procurar
                    result = self.session.execute(strCommand)
                    retList.append(result.one())

        agrFlag = self.agrCheck(projList)                                               # Verificar se existem agregações e selecionar a correta 
        if agrFlag == "AVG:":
            retList = self.averageHandler(retList)
        elif agrFlag == "MIN:":
            retList = self.minimumHandler(retList)
        elif agrFlag == "MAX:":
            retList = self.maximumHandler(retList)
        elif agrFlag == "SUM:":
            retList = self.sumHandler(retList)
        elif agrFlag == "CNT:":
            retList = self.countHandler(retList)
        elif agrFlag == "ERROR":
            retList = [] 

        return retList

    # Função de querying por sensor
    def queryPerSensor(self, user, sensor, projList, paramConditionDictionary):
        pkDict = {}                                                                     # Lista de listas de pks validadados pelas subqueries

        tableQuery = self.session.execute("select tables, pks from sensors where user = '" + user + "' and sensor_id = '" + sensor + "'")   # Procurar para um utilizador as tabelas e pks associados
                                                     
        tables = tableQuery.one()[0]
        userPks = tableQuery.one()[1]

        atributes = []                                                                  # Recolher todos os atributos necessários nesta query
        for key in paramConditionDictionary:
            atributes.append(key)
        if not projList[0] == '*':
            for atr in projList:
                atributes.append(self.agrHandler(atr))

        possibleTables = []                                                             # Através da tabela metadata_atributes perceber que tabelas possuem todos os atributos necessários ou seja que tabelas poderiam satisfazer a query
        for atr in atributes:
            atributeQuery = self.session.execute("select tablename from " + atr + "_table")
            atrTables = [table[0] for table in atributeQuery]
            possibleTables.append(atrTables)

        if len(possibleTables) == 0:                                                    # Lidar com o caso especifico em que projList = ['*'] e não existem condições de filtragem
            possibleTables = tables
        else:
            possibleTables = set(possibleTables[0]).intersection(*possibleTables)       # Interseção das tabelas possiveis para a lista conter apenas tabelas que possuem todos os atributos

        for table in possibleTables:                                                    # Para cada tabela possivel se for uma tabela do utilizador efetuar as subqueries para obter os pks possiveis
            if table in tables:
                pkLists = []
                for key in paramConditionDictionary:                                    # Para cada conjunto de parametro e condição pelos quais queremos filtrar fazer uma subquery nas tabelas secundárias
                    value = paramConditionDictionary[key]
                    pkLists.append(self.subQuery(table, key, value))                    # Adicionar a lista de resultados dessas subqueries à lista de listas das pks
                pkLists = set(pkLists[0]).intersection(*pkLists)                        # Criar um set de pks que passaram todos os requisitos associados a dada tabela
                pkDict[table] = pkLists

        retList = []

        for key in pkDict:                                                              # Para cada tabela do utilizador
            for pk in pkDict[key]:                                                      # Para cada pk possivel encontrado nas subqueries
                if pk in userPks:                                                       # Se este é um dos pks do utilizador executar a query nessa tabela por esse pk
                    strCommand = "select "                                              # Adicionar ao comando os parametros que desejamos procurar
                    for par in projList:
                        strCommand = strCommand + self.agrHandler(par)
                        if not projList[len(projList) - 1] == par:
                            strCommand = strCommand + ','
                    strCommand = strCommand + " from " + key + " where pk='" + pk + "'" # Adicionar ao comando a tabela principal em que desejamos procurar
                    result = self.session.execute(strCommand)
                    retList.append(result.one())
            
        agrFlag = self.agrCheck(projList)                                               # Verificar se existem agregações e selecionar a correta 
        if agrFlag == "AVG:":
            retList = self.averageHandler(retList)
        elif agrFlag == "MIN:":
            retList = self.minimumHandler(retList)
        elif agrFlag == "MAX:":
            retList = self.maximumHandler(retList)
        elif agrFlag == "SUM:":
            retList = self.sumHandler(retList)
        elif agrFlag == "CNT:":
            retList = self.countHandler(retList)
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
    def averageHandler(self, returnList):

        if not returnList[0][0].isnumeric():
            print("Invalid parameter for average")
            return []
        
        valueList = [int(val[0]) for val in returnList]
        avg = sum(valueList) / len(valueList)

        return ["row[" + str(avg) + "]"]

    # Função para retirar o minimo dos resultados
    def minimumHandler(self, returnList):
        if not returnList[0][0].isnumeric():
            print("Invalid parameter for average")
            return []
        
        ret = int(returnList[0][0])
        for val in returnList[1:len(returnList)]:
            if  ret > int(val[0]):
                ret = int(val[0])

        return ["row[" + str(ret) + "]"]

    # Função para retirar o maximo dos resultados
    def maximumHandler(self, returnList):
        if not returnList[0][0].isnumeric():
            print("Invalid parameter for average")
            return []
                
        ret = int(returnList[0][0])
        for val in returnList[1:len(returnList)]:
            if  ret < int(val[0]):
                ret = int(val[0])

        return ["row[" + str(ret) + "]"]

    # Função para retirar a soma dos resultados
    def sumHandler(self, returnList):
        if not returnList[0][0].isnumeric():
            print("Invalid parameter for average")
            return []
        
        valueList = [int(val[0]) for val in returnList]
        ret = sum(valueList)

        return ["row[" + str(ret) + "]"]
    
    # Função para retirar o numero de resultados
    def countHandler(self, returnList):
        return ["row[" + str(len(returnList)) + "]"]

    def getAllValuesOn(self, atribute):
        valueRows = self.session.execute("Select " + atribute + " from " + atribute + "_table")

        values = [row[0] for row in valueRows]

        return values

    # Função para apresentar as tabelas de registos existentes e que atributos possui cada tabela
    def getTables(self):
        tableRows = self.session.execute("Select * from metadata")

        tables = [row[0] for row in tableRows]

        return tables

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

    # Função que mostra as tabelas e atributos respetivos às mesmas mas apenas aquelas nas quais o utilizador tem registos
    def getMyTables(self, user):
        ret = []
        userRows = self.session.execute("Select * from sensors where user = '" + user + "'" )
        tables = []

        for row in userRows:
            for table in row[3]:
                tables.append(table)
        
        tables = list(dict.fromkeys(tables))

        for table in tables:
            result = self.session.execute("Select * from metadata where tableName = '" + table + "'")
            tableInfo = result.one()
            print(tableInfo[0] + " - " + str(tableInfo[1]))
    
    # Função para printar resultados
    def printResults(resultList):
        rs = ""

        for result in resultList:
            rs = rs + (str(result)[4:len(str(result)) - 1])
            rs = rs + "\n"
            rs = rs + ("..........................................\n")

        print(rs)

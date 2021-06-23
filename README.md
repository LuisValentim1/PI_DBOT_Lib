# PI_DBOT_Lib
## Database of Things supporting library.
Contents: 
    1. Data Model
    2. DB - Metodos de Interação com Cassandra 
    3. JSON Parser
    4. Cache
## Data Model 

###### O modelo de dados utilizado para possibilitar a organização eficiente e posterior extensão das capacidades de querying de CQL.

* metadata   PRIMARY KEY(attribute, timestamp, pk)

[]()|[]()|[]()
---     |    ---     |    ---
'attribute' | 'timestamp' | 'pk'
[]()|[]()


* metadata_reverse   PRIMARY KEY(attribute, pk, timestamp)

[]()|[]()|[]()
---     |    ---     |    ---
'attribute' | 'pk' | 'timestamp'
[]()|[]()|[]()

* sensors   PRIMARY KEY(user, sensor_id, pk)

[]()|[]()|[]()|[]()
---     |    ---     |    ---     |    ---
'user' | 'sensor_id' | 'pk' | 'attributes'
[]()|[]()|[]()|[]()

* attribute_table   PRIMARY KEY(pk,attribute_value)

[]()|[]()
---     |    ---
'pk' | 'attribute_value'
[]()|[]()|[]()|[]()

## DB - Metodos de Interação com Cassandra

### Login and Authentication

Metodos associados à gestão de roles e keyspaces necessários para garantir a privacidade do sistema através de keyspaces individuais.

* REGISTER(user, password) - Criar keyspace e role individuais para o utilizador
    
* INITIALIZE(user, password) - Criar as tabelas base metadata, metadata_reverse e sensors no keyspace do utilizador

* SESSION_LOGIN(user, password) - Abrir uma sessão com os dados de autenticação passados nos argumentos

### Insertion Functions

Metodos que retiram os dados do flat json e os organizam no nosso data model, apenas a função insert_into sensor é chamada, todas as outras são chamadas posteriormente e estão separadas para facilitar a visinilidade e modularidade do processo.

* CHECK_TABLE(session, attribute) - Verificar se já existe tabela criada para guardar dados de um determinado atributo

* CREATE_TABLE(session, attribute, flag) - Criar tabela de um atributo. A flag indica se é um atributo numérico ou alfanumérico.

* INSERT_INTO(session, flat json, pk id) - Criar timestamp caso seja necessário e inserir os dados nas tabelas de metadados e atributos

* INSERT_INTO_SENSOR(cache session, flat json, sensor id) - Criar pk e criar um registo na tabela sensors de forma a identificar o utilizador e sensor a que os dados estão associados. Cache session possui a sessão do utilizador e o seu username.

### Querying Functions 

As queries obtem o seu resultado cruzando os pks associados ao utilizador ou sensor com todos os pks de todos os atributos presentes na query, organiza num dicionário atributo:pk e utiliza as subqueries de forma a identificar quais destes pks passam as condições de filtragem, os pks que estiverem simultaneamente em todas as condições são os registos amostrados.

* SUB_QUERY(session, pk, attribute, condition, value) - Querying nas tabelas de atributos de forma a encontrar os pks dos registos que satisfazem a condição a ser verificada.

* QUERY_PER_USER(cache session, projection list, condition list) - Sistema de querying apenas nos registos de um dado utilizador. Cache session possui a sessão e username do utilizador, projection list indica os atributos que queremos visualizar, condition list é uma lista de listas, cada uma das sublistas deve possuir 3 elementos e representa uma condição por exemplo se quisermos filtrar por temperaturas inferiores a 20 graus mas não menores que 10 graus a lista de condições poderia ser a seguinte, [['temperature','<','20'],['temperature','>=','10']].

* QUERY_PER_SENSOR(cache session, sensor id, projection list, condition list) - Sistema de querying apenas nos registos de um dado sensor. Cache session possui a sessão e username do utilizador, projection list indica os atributos que queremos visualizar, condition list é uma lista de listas, cada uma das sublistas deve possuir 3 elementos e representa uma condição por exemplo se quisermos filtrar por temperaturas inferiores a 20 graus mas não menores que 10 graus a lista de condições poderia ser a seguinte, [['temperature','<','20'],['temperature','>=','10']].

* RANGE_QUERY_PER_USER(cache session, projection list, condition list, start date, finish date) - Query per user num intervalo temporal definido. Cache session possui a sessão e username do utilizador, projection list indica os atributos que queremos visualizar, condition list é uma lista de listas, cada uma das sublistas deve possuir 3 elementos e representa uma condição por exemplo se quisermos filtrar por temperaturas inferiores a 20 graus mas não menores que 10 graus a lista de condições poderia ser a seguinte, [['temperature','<','20'],['temperature','>=','10']].

* RANGE_QUERY_PER_SENSOR(cache session, sensor id, projection list, condition list, start date, finish date) - Query per sensor num intervalo temporal definido. Cache session possui a sessão e username do utilizador, projection list indica os atributos que queremos visualizar, condition list é uma lista de listas, cada uma das sublistas deve possuir 3 elementos e representa uma condição por exemplo se quisermos filtrar por temperaturas inferiores a 20 graus mas não menores que 10 graus a lista de condições poderia ser a seguinte, [['temperature','<','20'],['temperature','>=','10']].

### Aggregation Handling 
Existem 5 tipos de agregação disponiveis:
Count - CNT()
Average - AVG()
Maximum value - MAX()
Minimum value - MIN()
Sum of every value - SUM()
Utilização de agregações: A agregação deve ser passada através dos atributos de projeção, por exemplo se pretendemos obter a média da temperatura conforme x filtros invocamos o metodo de query relativo à amostra que queremos e alteramos a projeção de ['temperatura'] para ['AVG(temperatura)'] .
Quando pretendemos fazer agregações só pode ser selecionado um atributo para projeção.

* AGR_HANDLER(attribute) - Verificar se o atributo contem uma agregação e retorna o parametro sem agregação.

* AGR_CHECK(projection list) - Verificar se a lista de projeções é válida para agregações e se foi utilizado uma agregação no atributo, retorna qual a agregação encontrada caso encontre, caso contrário retorna "Erro" ou None

* COUNT_HANDLER(query results, attribute) - Retorna o numero de registos que a query encontrou.

* AVERAGE_HANDLER(query results, attribute) - Retorna a média dos valores obtidos pela query.

* MAXIMUM_HANDLER(query results, attribute) - Retorna o maior dos valores obtidos pela query.

* MINIMUM_HANDLER(query results, attribute) - Retorna o menor dos valores obtidos pela query.

* SUM_HANDLER(query results, attribute) - Retorna a soma de todos os valores obtidos pela query.

### API support Functions

Funções que não são estritamente necessárias para os processos de inserção, querying e proteção de dados, no entanto suportam a executar mais facilmente operações dentro da propria bilioteca e/ou na API, particularmente uteis para possibilitar a integração de grafana e da interface gráfica da API.
Cache session possui a sessão do utilizador e o seu username.

* CHECK_FLOAT(value) - Verificar se é possivel transformar o valor num float. Utilizado para perceber quais os atributos alfanuméricos e quais os estritamente numéricos 

* GET_ALL_VALUES_ON(cache session, attribute) - Obter todos os valores relativos a um dado atributo

* GET_USERS(cache session) - Obter todos os utilizadores da base de dados. (Metodo desenvolvido para base de dados comum sem capacidades de segurança)

* GET_SENSORS(cache session) - Obter todos os sensores associados a um utilizador

* GET_ALL_SENSORS_ATTRIBUTES(cache session) - Obter todos os atributos presentes em todos os sensores no formato [['utilizador','sensor',[atributos]]]

* GET_SENSOR_ATTRIBUTES(cache session, sensor id) - Obter todos os atributos presentes num dado sensor no formato [['utilizador','sensor',[atributos]]]

* GET_ATTRIBUTES(cache session) - Retorna uma lista com todos os atributos existentes na base de dados

* PRINT_RESULTS(query results) - Printa o resultado de uma query, utilizado em debugging e testes funcionais ao longo do desenvolvimento da biblioteca. Relevante apenas em situações de teste/debugging

## JSON Parser 
O JSON Parser transforma os registos JSON enviamos pelo utilizador em flattened jsons, dicionários com todos os atributos do JSON e sem recursividade, de forma a que estes possam ser utilizados pelos metodos da DB.
A maioria dos metodos que estão na origem deste Parser foram nos fornecidos pelo orientador. 

* IS_LIST_NUMERIC(self, list) - Verifica se uma lista contem apenas elementos numérico

* IS_LIST_STR(self, list) - Verifica se uma lista contem apenas elementos alfanuméricos/string

* IS_LIST_DICT(self, list) - Verifica se uma lista contem apenas elementos do tipo dicionário

* NEW_PREFIX(self, prefixo, key) - Cria um prefixo unico de quando por exemplo estão a ser desfeitas listas de elementos dentro do json


* FLAT_DICT(self, data, prefixo) - Processo de transformação da estrutura dicionário com recursividade num dicionário flat/sem recursividade

* FLAT_JSON(self, json) - Carregar o json como dicionário e invocar o método flat_dict 

* READ_JSON(self, file name) - Ler um ficheiro no diretório atual. Utilizado para testes de funcionais e de pequena escala

## Cache
Cache simples para gestão de sessões de Cassandra ativa, limita o número de utilizadores que podem estar a interagir ao mesmo tempo. Tamanho fixo configurável na inicialização, timeout de elemento após 50 minutos.

* ADD(self, user, session) - Adicionar um elemento à cache, caso esteja na capacidade maxima o elemento mais antigo é eliminado. Se um elemento que já existe for adicionado o timeout timer é reiniciado

* GET(self, user) - Retorna o elemento, sessão neste caso, relativo a um utilizador especifico

* VALID(self, user) - Verifica se um elemento na Cache ainda é válido e se não o for elimina-o 
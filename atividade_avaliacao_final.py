
import pymongo as pm
import pprint as pp

host = "mongodb://localhost:27017"
conexao = pm.MongoClient(host)


'''Nota :Eu não fiz os try-except para economizar tempo ja que não era o foco, então levei em consideração 
só problemas que poderiam gerar problemas de fato,não problemas que fariam o programa só parar de funcionar'''



# Funções

#-------------------------------------------------------------------------------------------------

def bd_vincular(conexao):                               # Função quem pede o input de um nome de um banco de dados e cria uma conexao
    nome_bd = input('Não esta vinculado a nenhum banco de dados, digite o nome de um banco de dados para proseguir:\n')
    bd = conexao[nome_bd]
    
    return nome_bd,bd

#-------------------------------------------------------------------------------------------------

def bd_vinculado(nome_bd,conexao,chave_protecao):       # Função que se voce ja tiver conectado a um banco de dados ele pergunta se quer manter ou trocar
    resposta = input(f'você esta vinculado ao banco de dados: "{nome_bd}" ,deseja mudar?\n1- Sim\n2- Não\n')

    if resposta == '1':
        nome_bd = input('digite o nome do banco de dados que deseja vincular como padrão:\n')
        bd = conexao[nome_bd]
        chave_protecao = True                           # chave_protecao serve para o caso de troca o banco de dados e nao trocar a colecao

    elif resposta == '2':
        bd = conexao[nome_bd]
    
    return nome_bd,bd,chave_protecao
   
#-------------------------------------------------------------------------------------------------

def clc_vincular(bd):                                   # Função quem pede o input de um nome de uma coleção e cria uma conexao
    nome_clc = input('Não esta vinculado a nenhuma coleção, digite o nome de uma coleção para proseguir:\n')
    clc = bd[nome_clc]

    return nome_clc,clc

#-------------------------------------------------------------------------------------------------

def clc_vinculado(bd,nome_clc):                         # Função que se voce ja tiver conectado a uma coleção ele pergunta se quer manter ou trocar
    resposta = input(f'você esta vinculado a coleção: "{nome_clc}" ,deseja mudar?\n1- Sim\n2- Não\n')

    if resposta == '1':
        nome_clc = input('digite o nome da coleção que deseja vincular como padrão:\n')
        clc = bd[nome_clc]

    elif resposta == '2':
        clc = bd[nome_clc]

    return nome_clc,clc

#-------------------------------------------------------------------------------------------------

def revisor_bd(nome_bd,bd,conexao,chave_protecao):      # Função que revisa se esta conectado a um banco de dados ou não e direciona as funções adequadas anteriores
    databases_list = conexao.list_database_names()
    print(f'Seus Bancos de Dados:\n\n{databases_list}\n')

    if bd == None:
        nome_bd,bd = bd_vincular(conexao)
    else:
        nome_bd,bd,chave_protecao = bd_vinculado(nome_bd,conexao,chave_protecao)
    return nome_bd,bd,chave_protecao

#-------------------------------------------------------------------------------------------------

def revisor_clc(bd,clc,nome_clc,chave_protecao):        # Função que revisa se esta conectado a uma coleção ou não e direciona as funções adequadas anteriores

    colecao_list = bd.list_collection_names()
    print(f'Suas Coleções:\n\n{colecao_list}\n')
    
    if chave_protecao == True:

        nome_clc = None
        clc = None

    if clc == None:
        nome_clc,clc = clc_vincular(bd)
    else:
        nome_clc,clc = clc_vinculado(bd,nome_clc) 

    chave_protecao = False 

    return nome_clc,clc,chave_protecao

#-------------------------------------------------------------------------------------------------

def formatacao_insert_many(documento,clc):              # Função insert varios documentos/csv

                lista_documento = documento.split('/')        # [documento,documento]
                
                for doc in lista_documento:                   # [documento]

                    lista_registros = doc.split(';')            # [registro,registro]                
                    lista_valor = []
                    lista_chave = []
                    listas_chave_valor = []

                    for reg in lista_registros:                     #[registro]
                        listas_chave_valor += [reg.split(',')]        #[[chave,valor],[chave,valor]...]
                        
                    quantidade_chave_valor = len(listas_chave_valor)

                    for i in range(quantidade_chave_valor):                                   # separar chave_valor
                               
                        lista_chave += [listas_chave_valor[i][0]]

                        if listas_chave_valor[i][1].isdigit():
                            lista_valor += [int(listas_chave_valor[i][1])]                     # se for tudo digito tranforma em int
                        else:   
                            lista_valor += [listas_chave_valor[i][1]]
                    
                    clc.insert_one(                                                           # insert 
                        {
                        lista_chave[c]:lista_valor[c] for c in range(quantidade_chave_valor)
                        }
                    )

#-------------------------------------------------------------------------------------------------

# variaveis ---------------------------------------------

x = 0                   # Usei para o programa se reiniciar
nome_bd = None          # Nome Banco de Dados
bd = None               # Banco de Dados
nome_clc = None         # Coleção 
clc = None              # Nome Coleção
chave_protecao = False  # Chave_protecao serve para o caso de troca o banco de dados e nao trocar a colecao

# -------------------------------------------------------

# Programa ----------------------------------------------

while x == 0:
  
    print('''\nDigite o número dentre umas das opções a baixo:
    1- Criar ou se Conectar a um Banco de dados
    2- Criar ou se Conectar a uma Coleção
    3- Inserir Documento
    4- Busca de Documento
    5- Deletar Documentos
    6- Remover Coleção
    7- Listar Banco de Dados
    8- Listar Coleções de um Banco de Dados
    9- Sair\n''')

    codigo = input()
    
#-------------------------------------------------------------------------------------------------

    if codigo == '1':     # Criar ou se Conectar a um Banco de dados
        
        nome_bd,bd,chave_protecao = revisor_bd(nome_bd,bd,conexao,chave_protecao)
        
#-------------------------------------------------------------------------------------------------

    elif codigo == '2':   # Criar ou se Conectar a uma Coleção
        
        nome_bd,bd,chave_protecao = revisor_bd(nome_bd,bd,conexao,chave_protecao)
        nome_clc,clc,chave_protecao = revisor_clc(bd,clc,nome_clc,chave_protecao)
            
#-------------------------------------------------------------------------------------------------

    elif codigo == '3':   # Inserir Documento

        nome_bd,bd,chave_protecao = revisor_bd(nome_bd,bd,conexao,chave_protecao)

        nome_clc,clc,chave_protecao = revisor_clc(bd,clc,nome_clc,chave_protecao)
        
        resposta = input('Deseja inserir um ou mais Documentos?\n1- Um Documento\n2- Mais que um Documento\n')
        
        documento = input('Digite a chave e o valor separados por ",",\nChave de chave saparado por ";",\nDocumentos separados por / caso seja mais de um Documento\n'
        '\nExemplo: nome,daniel;endereco,rua 10/nome,lara;endereco,rua 12\n')
        

        
        if resposta == '1':   
            
            formatacao_insert_many(documento,clc)

        elif resposta == '2':

            formatacao_insert_many(documento,clc)

#-------------------------------------------------------------------------------------------------

    elif codigo == '4':   # Busca de Documento

        nome_bd,bd,chave_protecao = revisor_bd(nome_bd,bd,conexao,chave_protecao)
        nome_clc,clc,chave_protecao = revisor_clc(bd,clc,nome_clc,chave_protecao)

        resposta = input('Escolha entre as opções de busca a baixo:\n\n1- Todos os Documentos\n2- Todos os Documentos com base em igualdade\n3- Alguns Documento com base em igualdade\n')

        
        if resposta == '1':   

            busca = clc.find()

            for i in busca:
                print(i)

        elif resposta == '2':

            chave = input('Digite a chave:\n')

            valor = input('Digite o valor:\n')

            if valor.isdigit():
                valor = int(valor)

            busca = clc.find(
                {
                    chave:{'$eq':valor}
                }
            )

            for i in busca:
                print(i)

        elif resposta == '3':

            chave = input('Digite a chave:\n')

            valor = input('Digite o valor:\n')

            if valor.isdigit():
                valor = int(valor)

            limite = int(input('Digite quantos esta buscando:\n'))

            busca = clc.find(
                {
                    chave:{'$eq':valor}
                }
            ).limit(limite)

            for i in busca:
                print(i)

#-------------------------------------------------------------------------------------------------

    elif codigo == '5':   # Deletar Documentos

        nome_bd,bd,chave_protecao = revisor_bd(nome_bd,bd,conexao,chave_protecao)
        nome_clc,clc,chave_protecao = revisor_clc(bd,clc,nome_clc,chave_protecao)

        resposta = input('Escolha entre as opções de delete a baixo:\n1- Um Documento\n2- Mais que um Documento\n')
        print('Esse delete sera feito na condição de igualdade chave-valor:\n')

        chave = input('Digite a chave:\n')

        valor = input('Digite o valor:\n')

        if valor.isdigit():
            valor = int(valor)

        if resposta == '1':   

            clc.delete_one(
                {
                    chave:{'$eq':valor}
                }
            )

        elif resposta == '2':

             clc.delete_many(
                {
                    chave:{'$eq':valor}
                }
            )
        
#-------------------------------------------------------------------------------------------------

    elif codigo == '6':   # Remover Coleção

        nome_bd,bd,chave_protecao = revisor_bd(nome_bd,bd,conexao,chave_protecao)

        colecao_list = bd.list_collection_names()
        print(f'Suas Coleções:\n\n{colecao_list}\n')

        nome_colecao_drop = input('Digite qual coleção deseja deletar:\n')

        chave_protecao = True

        bd[nome_colecao_drop].drop()

#-------------------------------------------------------------------------------------------------

    elif codigo == '7':   # Listar Banco de Dados
        
        databases_list = conexao.list_database_names()

        print(databases_list)

#-------------------------------------------------------------------------------------------------

    elif codigo == '8':   # Listar Coleções de um Banco de Dados
        
        nome_bd,bd,chave_protecao = revisor_bd(nome_bd,bd,conexao,chave_protecao)

        colecao_list = conexao[nome_bd].list_collection_names()

        print(colecao_list)

#-------------------------------------------------------------------------------------------------

    elif codigo == '9':   # Sair
        x = 1
        
#-------------------------------------------------------------------------------------------------

        
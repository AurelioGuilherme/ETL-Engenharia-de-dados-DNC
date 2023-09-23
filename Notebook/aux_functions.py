"""
Funções auxiliares criadas para a análise e ETL dos dados do case técnico para o cargo de Engenheiro de Dados Jr da Escola DNC

"""
# import libs
import pandas as pd
import os
import unicodedata


def carregar_dados_xlsx():
    """ Função criada para carregar os dados da Lotofacil em um DataFrame pandas

    args: path_file (str) -> caminho do arquivo Lotofacil.xlsx

    returns: df -> DataFrame pandas    
    
    """
    # Caminho completo para o arquivo XLSX
    arquivo_xlsx = "../Data/Lotofacil.xlsx"
    
    # Verifica se o arquivo XLSX existe no caminho especificado
    if os.path.exists(arquivo_xlsx):
        # Lê o arquivo XLSX
        df = pd.read_excel(arquivo_xlsx, engine="openpyxl")
        print("Leitura do arquivo 'Lotofacil.xlsx' concluída com sucesso")
        return df
    else:
        print("Arquivo 'Lotofacil.xlsx' não encontrado no diretório 'Data/'")
        return None


def check_null_values(df, columns):
    """
    Função criada para verificar e contar a quantidade de valores nulos 
    

    args:
        df -> DataFrame: Conjunto de dados para ser verificado
        columns -> Str ou lista de Str contendo os nomes das colunas para serem verificadas
    returns:
        imprmi se existem valores nulos mostrando a quantidade e a coluna
        Caso não houver valor nulo imprimi que não possui valores nulos
    """
    # Variável para armazenar se existem valores nulos ou não 
    possui_nulos = False
    
    # itera sobre as colunas fornecidas verificando se existem dados nulos e armazena a quantidade de nulos
    for col in columns:
        null_count = df[col].isnull().sum()

        # Verifica se a quantidade de nulos é maior que 0 caso for imprimi que possui valores nulos nas colunas fornecidas
        if null_count > 0:
            possui_nulos = True
            print(f"\nA coluna '{col}' possui {null_count} valores nulos.")

    # Se não possuir nenhum valor nulo no conjunto de dados imprimi 
    if not possui_nulos:
        print("\nNenhuma das colunas fornecidas possui valores nulos.")


def count_duplicates(df, columns):
    """
    Função criada para verificar se existem dados duplicados

    args:
        df -> DataFrame: Conjunto de dados para ser verificado
        columns -> Str ou lista de Str contendo os nomes das colunas para serem verificadas

    returns:
        Imprimi se existem valores duplicados a quantidade e qual coluna, casão não houver imprimi que não existem
    
    """

    # Variável para armazenar se existem valores duplicados ou não 
    possui_duplicados = False  

    # intera sobre as colunas fornecidas verificando se existem dados duplicados e armazena no objeto possui_duplicados
    for col in columns:
        duplicados = df[df.duplicated(subset=col, keep=False)]

        # Verifica se possui dados salvos como duplicados, caso tenha muda o estado de 'possui_duplicados' para verdadeiro  
        # Imprimi qual coluna tem dado duplicado e quantos dados duplicados
        if not duplicados.empty:
            possui_duplicados = True
            print(f"\nA coluna '{col}' possui {len(duplicados)} dados duplicados:")
            print(duplicados[col].value_counts())

    # Caso não houver dados duplicados imprimi que esta tudo ok 
    if not possui_duplicados:
        print("\nNão há dados duplicados em nenhuma das colunas fornecidas.")


def verificar_tipos_de_dados(df):
    """
    Função criada  para verificar se os dados estão dentro do padrão desejado

    args: df -> DataFrame pandas: Conjunto de dados para ser verificado

    return: imprimi quais colunas estão fora do padrão especificado
    
    
    """

    # Dicionário com os tipos desejados para cada coluna
    tipos_de_dados_desejados = {
        "Concurso": "int64",
        "Data Sorteio": "<M8[ns]",
        "Bola1": "int64",
        "Bola2": "int64",
        "Bola3": "int64",
        "Bola4": "int64",
        "Bola5": "int64",
        "Bola6": "int64",
        "Bola7": "int64",
        "Bola8": "int64",
        "Bola9": "int64",
        "Bola10": "int64",
        "Bola11": "int64",
        "Bola12": "int64",
        "Bola13": "int64",
        "Bola14": "int64",
        "Bola15": "int64",
        "Ganhadores 15 acertos":"int64",
        "Cidade / UF": "O",
        "Rateio 15 acertos": "float64",
        "Ganhadores 14 acertos": "int64",
        "Rateio 14 acertos" : "float64",
        "Ganhadores 13 acertos": "int64",
        "Rateio 13 acertos" : "float64",
        "Ganhadores 12 acertos" : "int64",
        "Rateio 12 acertos" : "float64",
        "Ganhadores 11 acertos" : "int64",
        "Rateio 11 acertos" : "float64",
        "Acumulado 15 acertos" : "float64",
        "Arrecadacao Total" : "float64",
        "Estimativa Prêmio" : "float64",
        "Acumulado sorteio especial Lotofácil da Independência": "float64",
        "Observação": "O"   
    }

    # Itera sobre chave e valor do dicionario criado
    for coluna, tipo in tipos_de_dados_desejados.items():
        if coluna in df.columns:
            # Verifica se a coluna está presente no DataFrame
            if df[coluna].dtype != tipo:
                # Se o tipo atual da coluna for diferente do tipo desejado
                print(f"A coluna '{coluna}' não está no tipo de dado desejado ({tipo}).")


def verifica_data(df, columns):
    """
    Função criada para verificar a coluna de datas se esta no padrão dd/mm/yyyy

    args:
        df -> DataFrame: Conjunto de dados para ser verificado
        col -> Str: Nome da coluna contendo as datas

    Returns: 
        Print da mensagem de erro contendo o index que esta fora do padrão.
        Caso os dados estiverem dentro do padrão printa a mensagem:
        'Formato da data esta dentro padrão: dd/mm/yyyy'

    """
    # itera sobre o conjunto de dados
    for i in range(0,len(df)):

        # Armazena o valor correspondente ao dia
        condicao_dia = int(str(df[columns][i])[0:2])

        # Armazena o valor correspondente ao mês
        condicao_mes = int(str(df[columns][i])[3:5])

        # Armazena o valor correspondente a quantidade de caracteres do ano
        condicao_ano = len(str(df[columns][0])[6:10])

        # Armazena valores correspondete aos separadores
        separador_1 = str(df["Data Sorteio"][0])[2]
        separador_2 = str(df["Data Sorteio"][0])[5]

        # Verifica se o valor correspondente ao dia é maior que 31 ou menor 1
        if (condicao_dia > 31) or (condicao_dia < 1):
            print(f'\nindex: {i} fora do padrão dia')

        # Verifica se o valor correspondente ao mes é maior que 12 ou menor 1    
        elif (condicao_mes > 12) or (condicao_mes < 1):
            print(f'\nindex: {i} fora do padrão mês')

        # Verifica se o valor correspondente a quantidade de caracteres do ano é diferente de 4    
        elif condicao_ano != 4:
            print(f'\nindex: {i} fora do padrão ano')

        # Verifica separador é diferente de '/'    
        elif (separador_1 != '/') or (separador_2 != '/'):
            print(f"\nSeparador incorreto - 1: {separador_1} 2: {separador_2}")
            
    else:
        # Imprimi que os dados estão dentro do padrão
        print("\nFormato da data esta dentro padrão: dd/mm/yyyy")


def transform_data_type(df, col_data_sorteio="Data Sorteio"):
    """
    A Função foi criada para transformar os dados Data Sorteio em um formato e tipo datetime.
    Caso a função não consiga efetuar a transformação exibira um erro sobre a falha.

    Args: 
        df -> DataFrame pandas: Conjunto de dados
        col_data_sorteio -> str: Nome da coluna correspondente (default = 'Data Sorteio')
    
    returns:
        DataFrame com a coluna correspondente a data de sorteio convertida  para um formato data
    
    """
    # Nome do tipo do dado desejado
    condicao_type = "datetime64[ns]"
    
    try:
        # Tenta transformar a coluna em um formato de data
        data_transformed = pd.to_datetime(df[col_data_sorteio], dayfirst=True)
        
        # Verifica se o tipo de dado é datetime64[ns] (desejado), caso sim transforma o dado original e exibi que a transformação foi bem sucedida 
        if data_transformed.dtypes == condicao_type:
            df[col_data_sorteio] = data_transformed
            print("A transformação para data foi bem-sucedida.")
        else:
            print("A transformação não resultou em datetime64[ns].")
    except Exception as e:
        # Captura qualquer exceção que possa ocorrer durante a transformação
        print(f"Erro durante a transformação: {str(e)}")


def remover_acentos_e_especiais(texto):
    """
    Função para remoção de caracteres especiais
    args:
        texto -> string: 
    return:
        texto sem caracteres especiais    
    """
    # Verifica cada caracter do texto se possui caracteres especiais
    texto_sem_acentos = ''.join((c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn'))
    return ''.join(e for e in texto_sem_acentos if e.isalnum() or e.isspace()).upper()

def dividir_cidade_uf(valor):
    """
    Função para dividir o conjunto de dados em cidades e UF únicos.
    args:
        valor -> string: contendo informações de uf_cidade separados por;
    
    retorns:
        DataFrame contendo valores únicos de cidades e UF
        Caso não houver valores adiona o valor não especificado
    
    """
    # Verifica se o valor não é nulo
    if pd.notna(valor):
        # Separa string por ;  
        partes = valor.split(";")
        # Armazena valores únicos de cidades
        cidades = set()  
        # Armazena valores únicos de UFs
        ufs = set()    
        
        # Itera por cada string separa por ';' anteriormente
        for parte in partes:
            cidade_uf = parte.strip()
            # Verifica se existe '/' na string, caso houver separa em cidade e UF
            if "/" in cidade_uf:
                try:
                    cidade, uf = cidade_uf.split("/")
                    # Garante que os valores estejam em maiúsculas e sem caracteres especiais
                    cidade = remover_acentos_e_especiais(cidade)
                    uf = remover_acentos_e_especiais(uf)
                    # Adiciona os valores em cidades
                    cidades.add(cidade)
                    # adiciona os valores em ufs
                    ufs.add(uf)
                except ValueError:
                    # IMprimi mensagem de erro caso houver mostrando a linha/index
                    print(f"Falha na conversão na linha: {valor}")
            else:
                # Caso não houver valores adiciona o valor NÃO ESPECIFICADA
                cidades.add("CIDADE NAO ESPECIFICADA")
                ufs.add(cidade_uf)
        # Retorna valores separados de cidade e valores
        return pd.Series({'Cidade': ", ".join(cidades), 'UF': ", ".join(ufs)})
    else:
        # Caso o valor for nulo retorna Nâo especificado
        return pd.Series({'Cidade': "CIDADE NAO ESPECIFICADA", 'UF': "UF NAO ESPECIFICADA"})

        
    

    
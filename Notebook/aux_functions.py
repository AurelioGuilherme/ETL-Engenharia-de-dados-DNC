"""
Funções auxiliares criadas para a análise e ETL dos dados do case técnico para o cargo de Engenheiro de Dados Jr da Escola DNC

"""


import pandas as pd


def verifica_data(df, col):
    """
    Função criada para verificar a coluna de datas se esta no padrão dd/mm/yyyy

    args: df -> DataFrame: Conjunto de dados
          col -> Str: Nome da coluna contendo as datas

    Returns: Print da mensagem de erro contendo o index que esta fora do padrão.
             Caso os dados estiverem dentro do padrão printa a mensagem:
             'Formato da data esta dentro padrão: dd/mm/yyyy'

    """
    # itera sobre o conjunto de dados
    for i in range(0,len(df)):

        # Armazena o valor correspondente ao dia
        condicao_dia = int(str(df[col][i])[0:2])

        # Armazena o valor correspondente ao mês
        condicao_mes = int(str(df[col][i])[3:5])

        # Armazena o valor correspondente a quantidade de caracteres do ano
        condicao_ano = len(str(df[col][0])[6:10])

        # Verifica se o valor correspondente ao dia é maior que 31 ou menor 1
        if (condicao_dia > 31) or (condicao_dia < 1):
            print(f'index: {i} fora do padrão dia')

        # Verifica se o valor correspondente ao mes é maior que 12 ou menor 1    
        elif (condicao_mes > 12) or (condicao_mes < 1):
            print(f'index: {i} fora do padrão mês')

        # Verifica se o valor correspondente a quantidade de caracteres do ano é diferente de 4    
        elif condicao_ano != 4:
            print(f'index: {i} fora do padrão ano')
            
    else:
        print("Formato da data esta dentro padrão: dd/mm/yyyy")
        
    

    
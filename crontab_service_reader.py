import dateutil.parser
import pandas
import csv
import datetime


import email_wraper_sender


def return_f(value):
    """
    Função somente ecoa o valor enviado para a função
    Usado na da função pd.apply()
    """
    return value


directories = ['/home/mauriciolongato/scraping_praticagem_ES_atracado/log/process_summary.log',
                '/home/mauriciolongato/scraping_praticagem_ES_executado/log/process_summary.log',
                '/home/mauriciolongato/scraping_praticagem_ES_fundeado/log/process_summary.log',
                '/home/mauriciolongato/scraping_praticagem_ES_previstos/log/process_summary.log',
                '/home/mauriciolongato/scraping_praticagem_Maceio/log/process_summary.log',
                '/home/mauriciolongato/scraping_praticagem_Maranhao_atracado/log/process_summary.log',
                '/home/mauriciolongato/scraping_praticagem_Maranhao_fundeado/log/process_summary.log',
                '/home/mauriciolongato/scraping_praticagem_Maranhao_manobras/log/process_summary.log',
                '/home/mauriciolongato/scraping_praticagem_Norte/log/process_summary.log']

# Abre arquivo crontab_service
lista_erros = []
for address in directories:
    
    try:
        # Abre e trata o log e obtem o ultimo status
        header_from_to = {0:"data", 1:"internet_conn_status", 2:"debug_level", 3:"parser_status", 4:"mysql_conn_status", 5:"erro_upload"}
        dataframe = pandas.read_csv(filepath_or_buffer=address, sep =';', header=None)
        dataframe = dataframe.rename(columns=header_from_to)
        dataframe['data'] = dataframe['data'].apply(dateutil.parser.parse)

        # Mudar para obter as linhas das ultimas 36 horas de processamento
        #last_status = dataframe.loc[dataframe['data'].idxmax()]
        t_inicial = datetime.datetime.now() - datetime.timedelta(days=1, hours=12)
        dataframe = dataframe[(dataframe['data'] > t_inicial)]
        # Filtra somente as linhas que contem False (FALSE) em pelo menos uma das colunas
        mensagem = dataframe[dataframe.apply(lambda x: True in x.values, axis=1)]
        if not mensagem.empty:
            
            mensagem["address"] = mensagem.apply(lambda row: return_f(address), axis=1)        
            lista_erros.append(mensagem)

    except:
        # Reportar erro de estruturação do log
        erro_report = { "data": datetime.datetime.now(), 
                        "internet_conn_status": "ERRO", 
                        "debug_level": "ERRO", 
                        "parser_status": "ERRO", 
                        "mysql_conn_status": "ERRO", 
                        "erro_upload": "ERRO",
                        "address": address}
        mensagem = pandas.DataFrame(data=erro_report)
        lista_erros.append(mensagem)


if lista_erros:
    # Caso existam linhas com erros, vamos concatenar a tabela e enviar por email
    tabela_mensagem = pandas.concat(lista_erros)
    #print(tabela_mensagem)
    header = ["address", "data", "internet_conn_status", "debug_level", "parser_status", "mysql_conn_status", "erro_upload"]
    tabela_mensagem = tabela_mensagem[header]
    #print(tabela_mensagem)
    pandas.set_option('display.max_colwidth', -1)
    mensagem = tabela_mensagem.to_html()
    #print(mensagem)
    #mensagem = "ERRO: ", last_status
    email_wraper_sender.main(mensagem, "mauricio.longato@terraf.com.br")
    email_wraper_sender.main(mensagem, "caio.rufato@terraf.com.br")
    #print("ERRO: ", last_status)

# Preparação e envio do email
"""
print(lista_erros)
lista_erros = [{'parser_status': True, 
                'internet_conn_status': 'INFO', 
                'data': "Timestamp('2017-01-19 13:16:41')", 
                'mysql_conn_status': 'na', 
                'debug_level': True, 
                'address': '/home/mauriciolongato/scraping_praticagem_ES_atracado/log/process_summary.log', 
                'erro_upload': 'na'}]

print('\n\n')
if lista_erros:
    header = ["address", "data", "internet_conn_status", "debug_level", "parser_status", "mysql_conn_status", "erro_upload"]
    tabela_mensagem = pandas.DataFrame(data=lista_erros, columns=header)
    tabela_mensagem['data'] = tabela_mensagem['data'].apply(dateutil.parser.parse)
    print(tabela_mensagem)
    #mensagem = "ERRO: ", last_status
    #email_wraper_sender.main(mensagem)
    #print("ERRO: ", last_status)
"""
#mensagem = last_status.to_frame().T.to_html()
#mensagem = last_status.to_frame().T.reindex()
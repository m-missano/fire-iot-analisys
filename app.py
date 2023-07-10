from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import pprint
import numpy as np
import arrow
import requests
import json
import pandas as pd
import matplotlib.pyplot as plt
from pandas.io.json._normalize import json_normalize
from threading import Timer
import schedule
import time as t
from datetime import datetime, time
import bot

# Definicao do envio de mensagens de alerta
angstrom_send = False
nesterov_send = False
fire_send = False

# Definicao das categoria de risco de angstron
angstron_categorias = {
    'improvável': (4, float('inf')),
    'desfavorável': (2.5, 4),
    'favorável': (2, 2.5),
    'provável': (1, 2),
    'muito provável': (-float('inf'), 1)
}


# Executa a chamada na API do clima tempo
def call_api(iTIPOCONSULTA=4):

  iTOKEN = "iTOKEN" # alterar token do clima tempo

  if iTIPOCONSULTA == 1:
      iURL= "http://apiadvisor.climatempo.com.br/api/v1/locale/city?name=Rio Claro&state=SP&token=" + str(iTOKEN)
      iRESPONSE = requests.request("GET",iURL)
      iRETORNO_REQ = json.loads(iRESPONSE.text)
      print(iRETORNO_REQ)
      iURL = "http://apiadvisor.climatempo.com.br/api-manager/user-token/"+str(iTOKEN) + "/locales"
      payload = "localeId[]=" +str(3623)
      headers = {'Content-Type':'application/x-www-form-urlencoded'}
      iRESPONSE = requests.request("PUT",iURL, headers=headers, data=payload)
      print(iRESPONSE.text)

  # Retorna a previsão atual do clima
  if iTIPOCONSULTA == 2:
      iURL= "http://apiadvisor.climatempo.com.br/api/v1/weather/locale/3623/current?token=" + str(iTOKEN)
      iRESPONSE = requests.request("GET",iURL)
      iRETORNO_REQ = json.loads(iRESPONSE.text)
      #pprint(iRETORNO_REQ)
      return iRETORNO_REQ

  if iTIPOCONSULTA == 3:
      iURL= "http://apiadvisor.climatempo.com.br/api/v1/fire-focus?token="+str(iTOKEN) + "&from=2023-07-02T00:00:00&to=2023-07-02T23:59:59&localeId=3623"
      iRESPONSE = requests.request("GET",iURL)
      iRETORNO_REQ = json.loads(iRESPONSE.text)
      #pprint(iRETORNO_REQ)
      return iRETORNO_REQ

  # Retorna a previsão do tempo para as próximas 72 horas
  if iTIPOCONSULTA == 4:
      iURL= "http://apiadvisor.climatempo.com.br/api/v1/forecast/locale/3623/hours/72?token=" + str(iTOKEN)
      iRESPONSE = requests.request("GET",iURL)
      iRETORNO_REQ = json.loads(iRESPONSE.text)
      #pprint(iRETORNO_REQ)
      return iRETORNO_REQ

# Calcula a precipitacao do dia com base na API do clima tempo
def calculate_precipitation():
    current_date = datetime.now().strftime("%d/%m/%Y")

    total_precipitation = 0
    for item in call_api(4)["data"]:
        # Soma os valores de precipitacao por hora do dia atual
        if item["date_br"].startswith(current_date):
            rain_value = item["rain"]["precipitation"]
            precipitation = item["rain"]["precipitation"]
            total_precipitation += precipitation
    # Obtem a precipitacao total do dia
    print("Total precipitation: ", total_precipitation)
    return total_precipitation


# Recupera os dados da plataforma da konker
def get_data():
    global fire_send
    # Configuracao inicial para recebimento de dados da konker
    #Url de publicacao dos dados
    pub_url = 'https://data.demo.konkerlabs.net/pub/'
    #Url da API
    base_api = 'https://api.demo.konkerlabs.net'
    #Application padrão
    application = 'default'

    username = 'username' # Alterar username
    password = 'password' # Alterar password

    client = BackendApplicationClient(client_id=username)
    oauth = OAuth2Session(client=client)
    token = oauth.fetch_token(token_url='{}/v1/oauth/token'.format(base_api),
                                        client_id=username,
                                        client_secret=password)

    devices = oauth.get("https://api.demo.konkerlabs.net/v1/{}/devices/".format(application)).json()['result']
    #for dev in devices:
        #print(dev)

    guid_term=""
    for dev in devices:
        if dev['name'] == "nodemcu":
            guid_term = dev['guid']

    # Verifica se ha dados para o dia atual, caso contrario, pega o dia anterior
    current_time = arrow.utcnow().to('America/Sao_Paulo').time()
    if current_time.hour < 1:
        dt_start = arrow.utcnow().to('America/Sao_Paulo').floor('day').shift(days=-1)
    else:
        dt_start = arrow.utcnow().to('America/Sao_Paulo').floor('day')

    # Carrega temperatura
    canal = 'bme280_temperature'
    stats = oauth.get("https://api.demo.konkerlabs.net/v1/{}/incomingEvents?q=device:{} channel:{} timestamp:>{}&sort=oldest&limit=50000".format(application, guid_term, canal, dt_start)).json()['result']
    temp_df = json_normalize(stats).set_index('timestamp')
    temp_df = temp_df[3:]
    #print(temp_df)

    # Carrega umidade
    canal = 'bme280_humidity'
    stats = oauth.get("https://api.demo.konkerlabs.net/v1/{}/incomingEvents?q=device:{} channel:{} timestamp:>{}&sort=oldest&limit=50000".format(application,guid_term,canal,dt_start)).json()['result']
    hum_df = json_normalize(stats).set_index('timestamp')
    hum_df = hum_df[3:]
    #print(hum_df)

    # Carrega pressao
    canal = 'bme280_pressure'
    stats = oauth.get("https://api.demo.konkerlabs.net/v1/{}/incomingEvents?q=device:{} channel:{} timestamp:>{}&sort=oldest&limit=50000".format(application,guid_term,canal,dt_start)).json()['result']
    press_df = json_normalize(stats).set_index('timestamp')
    press_df = press_df[3:]
    #print(press_df)

    # Carrega luminosidade
    canal = 'bh1750_lux'
    stats = oauth.get("https://api.demo.konkerlabs.net/v1/{}/incomingEvents?q=device:{} channel:{} timestamp:>{}&sort=oldest&limit=50000".format(application,guid_term,canal,dt_start)).json()['result']
    lux_df = json_normalize(stats).set_index('timestamp')
    lux_df = lux_df[3:]
    #print(lux_df)
    
    # Verifica se deve enviar alerta de provavel incendio
    if(temp_df["payload.value"][-1] > 35 and hum_df["payload.value"][-1] < 20 and fire_send == False):
        bot.aviso("Provável incêndio!")

    return (temp_df, hum_df, press_df, lux_df)


# Realiza a categoriacao do indice de angstron
def categorizar_risco(valor):
    for categoria, (limite_inferior, limite_superior) in angstron_categorias.items():
        if limite_inferior < valor <= limite_superior:
            return categoria
    return 'desconhecido'

# Angston Index
def angstron_index():
    global angstrom_send
    print("Update Angstron")

    # Recupera os dados da konker
    temp_df, hum_df, press_df, lux_df = get_data()

    temp_df2 = temp_df.copy()
    hum_df2 = hum_df.copy()

    # Converte ingestedTimestamp para o formato ISO8601
    temp_df2['ingestedTimestamp'] = pd.to_datetime(temp_df2['ingestedTimestamp'], format='ISO8601')
    hum_df2['ingestedTimestamp'] = pd.to_datetime(temp_df2['ingestedTimestamp'], format='ISO8601')
    temp_df2.set_index('ingestedTimestamp', inplace=True)
    hum_df2.set_index('ingestedTimestamp', inplace=True)

    #print(temp_df2)
    # Agrupa dataframe por minuto
    temp_hourly = temp_df2['payload.value'].resample('T').mean().astype(float)
    hum_hourly = hum_df2['payload.value'].resample('T').mean().astype(float)
    hourly_average_df = pd.DataFrame({'timestamp': temp_hourly.index, 'temperature_avg': temp_hourly, 'humidity_avg': hum_hourly})

    #print("HORA:", hourly_average_df)

    # Calcula o valor de Angstron para cada minuto
    angstrom_values = 0.05 * hourly_average_df['humidity_avg'] - 0.10 * (hourly_average_df['temperature_avg'] - 27)

    # Adiciona o indice de angstron ao dataframe
    hourly_average_df['angstrom'] = angstrom_values
    hourly_average_df['risco'] = hourly_average_df['angstrom'].apply(categorizar_risco)

    # Verifica se necessita enviar mensagem de alerta para o bot
    if(hourly_average_df['risco'][-1] == 'provável' and angstrom_send == False):
        angstrom_send = True
        bot.aviso("Angstrom: É provável que haja um foco de incêndio")
    if(hourly_average_df['risco'][-1] == 'muito provável' and angstrom_send == False):
        angstrom_send = True
        bot.aviso("Angstrom: É muito provável que haja um foco de incêndio")

    # Plot/atualiza o indice de angstrom 
    plt.figure(1)
    plt.plot(hourly_average_df.index, hourly_average_df['angstrom'])
    plt.xlabel('Minuto')
    plt.ylabel('Angstrom')
    plt.title('Valores de Angstrom por Minuto')
    plt.grid(True)

    # Adicao dos limites de risco ao plot
    plt.axhspan(min(min(hourly_average_df['angstrom']), 0), 1, color='red', alpha=0.4, label='muito provável')
    plt.axhspan(1, 2, color='red', alpha=0.2, label='provável')
    plt.axhspan(2, 2.5, color='orange', alpha=0.2, label='favorável')
    plt.axhspan(2.5, 4, color='blue', alpha=0.2, label='desfavorável')
    plt.axhspan(4, max(max(hourly_average_df['angstrom']), 4+1), color='green', alpha=0.2, label='improvável')

    plt.legend()
    plt.tight_layout()
    plt.pause(0.001)
    plt.clf()

# Realiza o calculo de Nesterov
def calc_nesterov(data, keys=None):
    global nesterov_send
    #print("DATA:", data)

    # Verifica as keys a serem utilizadas em data
    if keys is None:
        keys = {'hora':'Hora (UTC)', 'data':'Data', 'total_chuva':'Total Chuva (mm)', 'temp_ins':'Temp. Ins. (C)', 'umid_ins':'Umi. Ins. (%)'}

    indices_nesterov = pd.DataFrame(columns=['Data', 'Hora (UTC)', 'Temp. Ins. (C)', 'E', 'e', 'Deficit de Saturacao', 'Umi. Ins. (%)', 'Total Chuva (mm)', 'Indice Nesterov'])

    arquivo = open('nesterov.json', 'r')
    nesterov_history = json.loads(arquivo.read())
    arquivo.close()
    # Verifica se ha historico de nesterov
    if len(nesterov_history['nesterov_index']) == 0:
        indice_anterior = 0;
    else:
        indice_anterior = nesterov_history['nesterov_index'][-1]

    for i in range(len(data)):
        hora_utc = data[keys['hora']][i]
        chuva_dia = data[keys['total_chuva']][i]

        #print("HORA:", hora_utc)
        #print("COMPARACAO", hora_utc == time(16,00,00))
        # Verifica a hora de acordo com o UTC
        if hora_utc == time(16,00,00):
            #print("ENTROU")
            if(pd.isna(data[keys['temp_ins']][i])):
                #print("AS")
                data[keys['temp_ins']][i] = 0
            if(pd.isna(data[keys['umid_ins']][i])):
                #print("BS")
                data[keys['umid_ins']][i] = 0
            E = 6.1078 * np.power(10, (7.5 * data[keys['temp_ins']][i]) / (237.3 + data[keys['temp_ins']][i]))
            Td = data[keys['temp_ins']][i] - ((100 - data[keys['umid_ins']][i]) / 5)
            e = 6.1078 * np.power(10, (7.5 * Td) / (237.3 + Td))

            deficit_saturacao = E - e
            if chuva_dia <= 2:
                # Não há modificação no cálculo, mantenha o valor anterior de G
                indice_nesterov = indice_anterior + (deficit_saturacao * data[keys['temp_ins']][i])
            elif 2.1 <= chuva_dia <= 5.0:
                # Abater 25% no valor de G calculado na véspera e somar (d.t) do dia
                indice_nesterov = 0.75 * indice_anterior + (deficit_saturacao * data[keys['temp_ins']][i])
            elif 5.1 <= chuva_dia <= 8.0:
                # Abater 50% no valor de G calculado na véspera e somar (d.t) do dia
                indice_nesterov = 0.5 * indice_anterior + (deficit_saturacao * data[keys['temp_ins']][i])
            elif 8.1 <= chuva_dia <= 10.0:
                # Abandonar somatória anterior e recomeçar novo cálculo, isto é, G = (d.t) do dia
                indice_nesterov = deficit_saturacao * data[keys['temp_ins']][i]
            elif chuva_dia > 10:
                # Interromper o cálculo (G = 0), recomeçando a somatória do dia seguinte ou quando a chuva cessar
                indice_nesterov = 0

                # Verifica se necessita enviar mensagem de alerta para o bot
                if(indice_nesterov < 4000 and indice_nesterov >= 1000 and nesterov_send == False):
                    nesterov_send = True
                    bot.aviso("Nesterov: Risco de incêndio: Grande")
                if(indice_nesterov >= 4000 and nesterov_send == False):
                    nesterov_send = True
                    bot.aviso("Nesterov: Risco de incêndio: Perigosíssimo")

            # Salva indice atualizado no historico
            indices_nesterov = pd.concat([indices_nesterov, pd.DataFrame({'Data': [data[keys['data']][i]], 'Hora (UTC)': [hora_utc],
                                                                        'Temp. Ins. (C)': [data[keys['temp_ins']][i]],
                                                                        'Umi. Ins. (%)': [data[keys['umid_ins']][i]],
                                                                        'Total Chuva (mm)': [chuva_dia],
                                                                        'Deficit de Saturacao': [deficit_saturacao],
                                                                        'E': [E], 'e': [e],
                                                                        'Indice Nesterov': [indice_nesterov]})], ignore_index=True)
            nesterov_history['nesterov_index'].append(indice_nesterov)
            nesterov_history['data'].append(data[keys['data']][i].strftime('%Y-%m-%d'))
            arquivo = open('nesterov.json', 'w')
            arquivo.write(json.dumps(nesterov_history))
            arquivo.close()

    # Definicao dos limites de risco
    limite_nenhum = 300
    limite_fraco = 500
    limite_medio = 1000
    limite_grande = 4000
    limite_perigosissimo = 4000

    # Atualiza o indice de nesterov 
    plt.figure(2, figsize=(10, 6))
    plt.plot(nesterov_history['data'], nesterov_history['nesterov_index'], label='Índice de Nesterov')
    plt.xlabel('Data')
    plt.ylabel('Índice de Nesterov')
    plt.title('Variação do Índice de Nesterov ao longo do tempo')
    plt.xticks(rotation=45)
    

    # Adicao dos limites de risco ao plot
    plt.axhspan(0, limite_nenhum, color='green', alpha=0.2, label='Nenhum')
    plt.axhspan(limite_nenhum, limite_fraco, color='blue', alpha=0.2, label='Fraco')
    plt.axhspan(limite_fraco, limite_medio, color='orange', alpha=0.2, label='Médio')
    plt.axhspan(limite_medio, limite_grande, color='red', alpha=0.2, label='Grande')
    plt.axhspan(limite_perigosissimo, indices_nesterov['Indice Nesterov'].max(), color='red', alpha=0.4, label='Perigosíssimo')

    plt.legend()
    plt.tight_layout()
    plt.pause(0.001)
    plt.clf()


# Roda o calculo do indice de nesterov
def run_nesterov():

    print("Update Nesterov")

    # Recupera os dados da konker
    temp_df, hum_df, press_df, lux_df = get_data()

    # Calcula precipipitacao do dia
    total_precipitation = calculate_precipitation()

    temp_df2 = temp_df.copy()
    hum_df2 = hum_df.copy()

    # Converte ingestedTimestamp para o formato ISO8601
    temp_df2['ingestedTimestamp'] = pd.to_datetime(temp_df2['ingestedTimestamp'], format='ISO8601')
    hum_df2['ingestedTimestamp'] = pd.to_datetime(temp_df2['ingestedTimestamp'], format='ISO8601')
    temp_df2.set_index('ingestedTimestamp', inplace=True)
    hum_df2.set_index('ingestedTimestamp', inplace=True)

    # Agrupa dataframe por hora
    temp_hourly = temp_df2['payload.value'].resample('H').mean().astype(float)
    hum_hourly = hum_df2['payload.value'].resample('H').mean().astype(float)
    hourly_average_df = pd.DataFrame({'timestamp': temp_hourly.index, 'temperature_avg': temp_hourly, 'humidity_avg': hum_hourly})

    # Converte timestamp para o formato de data e hora
    hourly_average_df['timestamp'] = pd.to_datetime(hourly_average_df['timestamp'])
    df_utc = hourly_average_df.copy()

    # Separa timestamp em data e hora
    df_utc['data'] = df_utc['timestamp'].dt.date
    df_utc['hora'] = df_utc['timestamp'].dt.time
    df_utc.drop('timestamp', axis=1, inplace=True)

    # Adiciona ao dataframe a precipitacao do dia
    df_utc['rain'] = [total_precipitation] * len(df_utc)

    # Declaracao dos campos do dataframe data_utc
    dictio = {'hora':'hora', 'data':'data', 'total_chuva':'rain','temp_ins':'temperature_avg', 'umid_ins':'humidity_avg'}

    # Chama a funcao para o calculo de Nesterov
    calc_nesterov(data=df_utc, keys=dictio)

def load_nesterov():
    # Carrega o arquivo contendo o historico do indice de nesterov
    arquivo = open('nesterov.json', 'r')
    nesterov_history = json.loads(arquivo.read())
    arquivo.close()
    try:
        # Pega o ultimo indice de nesterov
        indice_nesterov = nesterov_history['nesterov_index'][-1]

        # Verifica se necessita enviar mensagem de alerta para o bot
        if(indice_nesterov < 4000 and indice_nesterov >= 1000):
            nesterov_send = True
            bot.aviso("Nesterov: Risco de incêndio: Grande")
        if(indice_nesterov >= 4000):
            nesterov_send = True
            bot.aviso("Nesterov: Risco de incêndio: Perigosíssimo")


        # Definicao dos limites de risco
        limite_nenhum = 300
        limite_fraco = 500
        limite_medio = 1000
        limite_grande = 4000
        limite_perigosissimo = 4000


        # Plota indice de nesterov
        plt.figure(2)
        plt.plot(nesterov_history['data'], nesterov_history['nesterov_index'], label='Índice de Nesterov')
        plt.xlabel('Data')
        plt.ylabel('Índice de Nesterov')
        plt.title('Variação do Índice de Nesterov ao longo do tempo')
        plt.xticks(rotation=45)
        
        # Adicao dos limites de risco ao plot
        plt.axhspan(0, limite_nenhum, color='green', alpha=0.2, label='Nenhum')
        plt.axhspan(limite_nenhum, limite_fraco, color='blue', alpha=0.2, label='Fraco')
        plt.axhspan(limite_fraco, limite_medio, color='orange', alpha=0.2, label='Médio')
        plt.axhspan(limite_medio, limite_grande, color='red', alpha=0.2, label='Grande')
        plt.axhspan(limite_perigosissimo, max(max(nesterov_history['nesterov_index']), limite_perigosissimo+1), color='red', alpha=0.4, label='Perigosíssimo')

        plt.legend()
        plt.tight_layout()
        plt.pause(0.001)
        plt.clf()

    except:
        return -1


# Libera o envio de mensagens de grau de risco de incêndio
def update_send_messages():
    angstrom_send = False
    nesterov_send = False

# Libera o envio de mensagens de alerta de possível incêndio
def update_fire_message():
    fire_send = False

angstron_index()
load_nesterov()
#run_nesterov()
# Agenda a execução das funções de calculo de angstron, nesterov e alerta de mensagens
schedule.every(60).seconds.do(angstron_index)
schedule.every().day.at("23:09").do(run_nesterov)
schedule.every().day.at("00:00").do(update_send_messages)
schedule.every(30).minutes.do(update_fire_message)

print("Running")
# Executa programa infinitamente e verifica por agendamentos pendentes
while True:
    schedule.run_pending()
    t.sleep(1)
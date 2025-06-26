import pandas as pd

class DataTransformer():
    def __init__(self, df):
        self.df = df

    def clear_and_filter_data(self):
        print('Iniciando limpeza e filtro do DataFrame')

        df_list_chunks = []  # Lista vazia para guardar os pedaços do dataset filtrado e limpo

        for chunk in self.df:
            df_temp = chunk[['RecordedAtTime', 'DirectionRef', 'PublishedLineName', 'VehicleRef',
                            'ArrivalProximityText', 'ScheduledArrivalTime']]  # Colunas que serão utilizadas
            df_temp = df_temp[(df_temp['ArrivalProximityText'] == 'at stop')] # Filtro de registros no ponto
            df_temp.dropna(subset=['ScheduledArrivalTime'], inplace=True)  # Remoção de valores null
            df_temp.drop(['ArrivalProximityText'], axis=1, inplace=True) # Remove a coluna do ponto pois não será usada
            df_list_chunks.append(df_temp)
            
        self.df = pd.concat(df_list_chunks, ignore_index=True) # Concatena todos os pedaços em um único DataFrame   
        print(f'Dados do DataFrame foram filtrados filtrados.') 

    def __fix_ScheduledArrivalTime(self, hora_str):
        hora = int(hora_str[:2])
        if hora > 23:
            return "00:" + hora_str[3:]
        return hora_str

    def __fix_ScheduledArrivalTime_column(self):
        self.df['ScheduledArrivalTime'] = self.df['ScheduledArrivalTime'].apply(self.__fix_ScheduledArrivalTime)

        self.df['ScheduledArrivalTime'] = pd.to_datetime(self.df['ScheduledArrivalTime'], format='%H:%M:%S') # Converte a coluna de data e hora para datetime
        self.df['ScheduledArrivalTime'] = pd.to_timedelta(self.df['ScheduledArrivalTime'].dt.strftime('%H:%M:%S')) # Extrai apenas o tempo da coluna de data e hora

    def __fix_RecordedAtTime(self, each_row):
        real = each_row['RecordedAtTime']
        prog = each_row['ScheduledArrivalTime']

        # Se real for menor que programado e a diferença for maior que 12h, então virou o dia
        if real < prog and (prog - real) > pd.Timedelta(hours=12):
            return real + pd.Timedelta(days=1)
        else:
            return real

    def __fix_RecordedAtTime_column(self):
        self.df['RecordedAtTime'] = self.df.apply(self.__fix_RecordedAtTime, axis=1)
        
        print('Intervalo de dias em horários realizados após a meia-noite ajustado para o dia seguinte com base no horário programado.')

    def add_RecordedAtDate_and_Time_columns(self):
        self.df['RecordedAtDate'] = pd.to_datetime(self.df['RecordedAtTime']).dt.date # Cria uma nova coluna com a data
        self.df['RecordedAtTime'] = pd.to_datetime(self.df['RecordedAtTime']).dt.strftime('%H:%M:%S') # Transforma a coluna de timestamp em hora

        self.df['RecordedAtDate'] = pd.to_datetime(self.df['RecordedAtDate'], format='%Y-%m-%d') # Converte a coluna de data para datetime
        self.df['RecordedAtTime'] = pd.to_datetime(self.df['RecordedAtTime'], format='%H:%M:%S') # Converte a coluna de data e hora para datetime
        self.df['RecordedAtTime'] = pd.to_timedelta(self.df['RecordedAtTime'].dt.strftime('%H:%M:%S')) # Extrai apenas o tempo da coluna de data e hora

        print('Coluna de data e hora realizadas adicionadas.')
        self.__fix_ScheduledArrivalTime_column()
        self.__fix_RecordedAtTime_column()


    def add_DiffArrivalMins_column(self): # Função para adicionar a coluna de diferença entre a chegada programada e a chegada real
        self.df['DiffArrivalMins'] = (self.df['RecordedAtTime'] - self.df['ScheduledArrivalTime']).dt.total_seconds()/60 # Converte a diferença de tempo para minutos
        self.df['DiffArrivalMins'] = self.df['DiffArrivalMins'].astype('int64')  # Converte a diferença de tempo para minutos
        
        print('Coluna de entre a chegada programada e a chegada real adicionada.')

    def add_Recorded_and_ScheduledTimeRange_columns(self): # Função para adicionar colunas de faixa horária do horário registrado e programado
        self.df['RecordedTimeRange'] = self.df['RecordedAtTime'].dt.components['hours'].astype('int64') # Extrai apenas a hora da coluna de horário registrado
        self.df['ScheduledTimeRange'] = self.df['ScheduledArrivalTime'].dt.components['hours'].astype('int64') # Extrai apenas a hora da coluna de horário programado

        print("Coluna de faixa horária do registrado e programado adicionadas.")

    def get_df(self):
        return self.df
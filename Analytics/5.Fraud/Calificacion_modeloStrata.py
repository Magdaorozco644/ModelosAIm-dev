import pyodbc
import joblib
import numpy as np
import pandas as pd
import datetime as dt



def conectarse():
    '''
    Se conecta a la base para leer y escribir
    '''
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};\
            SERVER=VIADB_PROD;\
                DATABASE=Envio;\
                    UID=ViaFraudEngine;\
                        PWD=v(REf1!Y=AX6e^7w=EZk')
    print('Conectado a la base!')
    cnxn.autocommit = True
    cursor = cnxn.cursor()
    return cursor

def calificar(x_list:list)->dict:
    '''
    La funcion crea el diccionario del vector a calificar. Crea dos listas;
    La primera con las llaves y variables adicionales que no entran a la calificación del modelo.
    La segunda con las variables de entrada del modelo que se usan como filtro del diccionario creado,
    para posteriormente ser calificado.
    La calificacion incluye una regla dura, en donde las personas que han cometido alguna transaccion 
    fraudulenta es calificada como Fraude con 100% de probabilidad. 
    La fila recibida por la función se convierte a un vector de numpy, y se pasa al modelo cargado para 
    conseguir el resultado y la probabilidad asociada que depende del umbral definido.
    
    Parameters:
    ---
    x_list:list
        Fila de una tabla SQL convertida a una lista y organizada previamente
    
    Returns:
    ---
    x:dict
        Diccionario con las variables de x_list, y adicional, la predicción y la probabilidad.
    '''
    
    nombres = ['ID_BRANCH', 'ID_RECEIVER', 'TRANSACTION_UNIQUE', 
                  'date_receiver',
                  'RECEIVER_FRAUD','SENDER_FRAUD',
                  'branch_minutes_since_last_transaction',
                    'branch_trans_3m',  'branch_has_fraud',  
                    'branch_trans_40min', 'branch_trans_10min', 'cash_pick_up_40min', 
                    'location_nro_fraud', 'sender_trans_3m', 
                    'sender_nro_fraud','id_country_receiver_claim', 
                    'sender_state','id_state', 'net_amount_receiver', 'range_hist'
                    'branch_has_fraud', 'location_nro_fraud',
                    'id_payout', 
                    'sender_days_to_last_transaction', 
                    'receiver_transaction_count','01_sender_sending_days',
                    'branch_working_days', '01_net_amount_receiver', 
                    'sender_minutes_since_last_transaction',
                    'hour_receiver']

    cols_modelo = ['ID_BRANCH', 'ID_RECEIVER', 'TRANSACTION_UNIQUE', 
                    'date_receiver',
                    'RECEIVER_FRAUD','SENDER_FRAUD',
                    'branch_minutes_since_last_transaction',
                    'branch_trans_3m',  'branch_has_fraud',  
                    'branch_trans_40min', 'branch_trans_10min', 'cash_pick_up_40min', 
                    'location_nro_fraud', 'sender_trans_3m', 
                    'sender_nro_fraud','01_isMexico', 
                    '01_sender_branch_state', '01_var_range_hist', 
                    '01_branch_fraud', '01_location_fraud',
                    '01_isCashPick', '01_isBankDep', 
                    'sender_days_to_last_transaction_more7m', 
                    'receiver_transaction_count','01_sender_sending_days',
                    'branch_working_days', '01_net_amount_receiver', 
                    'sender_minutes_since_last_transaction_2days', 
                    'sender_days_to_last_transaction_365', 
                    'sender_days_to_last_transaction_7m', '01_hour_receiver']

    s = joblib.load('StrataModel.pkl')
    Buckets = pd.read_csv('./BucketsStrataModel.csv')
    pd.options.display.float_format = "{:,.15f}".format

    x = {}
    for num,i in enumerate(x_list):
        x[nombres[num]] = x_list[num]
        # print(nombresnum, i)
    # print(x)

    #Recode some variables
    x['01_isMexico'] = (x['id_country_receiver_claim'].str.strip() =='MEX')
    x['01_sender_branch_state'] = (x['sender_state'].str.strip() == x['id_state'].str.strip())
    x['01_var_range_hist'] =  (x['net_amount_receiver'].astype(float) / x['range_hist'])
    x['01_branch_fraud'] =  (x['branch_has_fraud']>0)
    x['01_location_fraud'] =  (x['location_nro_fraud']>0)
    
    cash_payout = ['M','P','S'] #id_payout for Cash Pick-up
    bankdp_payout =['C','N','X','T'] #id_payout for Bank Deposit
    x['01_isCashPick'] = x.id_payout.isin(cash_payout)
    x['01_isBankDep'] = x.id_payout.isin(bankdp_payout)
    
    x['sender_days_to_last_transaction_more7m'] = (x['sender_days_to_last_transaction']> 7*30)
    x['sender_days_to_last_transaction_7m'] = x.loc[x['sender_days_to_last_transaction']< 7*30, 'sender_days_to_last_transaction']
    x['sender_days_to_last_transaction_365'] = x.loc[x['sender_days_to_last_transaction']< 365, 'sender_days_to_last_transaction']
    x['sender_minutes_since_last_transaction_2days'] = x.loc[x['sender_minutes_since_last_transaction']> 2*24*60,'sender_minutes_since_last_transaction']
    
    
    if (x['RECEIVER_FRAUD'] == 1) or (x['SENDER_FRAUD'] == 1):
        x['WAS_FRAUD'] = 1
        x['PROBABILIDAD'] = 1
        return x

    filtered_d = dict((k, x[k]) for k in cols_modelo)
    giro_a_calificar = np.array(list(filtered_d.values())).reshape(1,-1)
    print('vector a calificar:{}'.format(giro_a_calificar))
    resultado = s.predict_proba(giro_a_calificar)
    print(resultado)
    
    #Recode score to standarize
    Buckets['min'] = pd.to_numeric(Buckets['min'])
    Buckets['max'] = pd.to_numeric(Buckets['max'])

    #bins = np.sort(np.append(Buckets['min'], Buckets['max'].iloc[-1]))
    bins = np.sort(np.append(Buckets['min'], np.inf))  

    labels = np.sort(Buckets['final_score'])
    
    # resultado_def = s.predict(giro_a_calificar)[0]
    umbral = 0.9
    x['PROBABILIDAD'] = resultado[0][1]
    # Recode scores using pd.cut()
    x['PROBABILIDAD'] = pd.cut(x['PROBABILIDAD'], bins=bins, labels=labels)
    if resultado[0][1] >= umbral:
        x['WAS_FRAUD'] = 1
    else:
        x['WAS_FRAUD'] = 0
    return x
    

def get_data(cursor=None):
    '''
    La función trae todas las filas de la tabla, y las ordena según row_v.
    las califica con la funciona extra. se conecta a la tabla de escritura.
    Guarda la calificacion y toma la siguiente fila. 
    '''
    print('entra')
    query1 = 'EXECUTE [Fraud].[Get_Fraud_Score_Vectors_V2]'
    cursor_q = cursor.execute(query1)
    print('se queda')
    i = 0
    
    
    row = cursor_q.fetchone()
    columns = [column[0] for column in cursor_q.description]
    # print('Column names: ',columns)
    print(row is None)
    while row is not None:
        row_v = [row.ID_BRANCH, row.ID_RECEIVER, row.TRANSACTION_UNIQUE, row.RECEIVER_FRAUD, row.SENDER_FRAUD, 
                    row.branch_minutes_since_last_transaction
                    row.branch_trans_3m,
                    row.branch_has_fraud,
                    row.branch_trans_40min,
                    row.branch_trans_10min,
                    row.cash_pick_up_40min,
                    row.location_nro_fraud,
                    row.sender_trans_3m,
                    row.sender_nro_fraud,
                    row.01_isMexico,
                    row.01_sender_branch_state,
                    row.01_var_range_hist,
                    row.01_branch_fraud,
                    row.01_location_fraud,
                    row.01_isCashPick,
                    row.01_isBankDep,
                    row.sender_days_to_last_transaction_more7m,
                    row.receiver_transaction_count,
                    row.01_sender_sending_days,
                    row.branch_working_days,
                    row.01_net_amount_receiver,
                    row.sender_minutes_since_last_transaction_2days,
                    row.sender_days_to_last_transaction_365,
                    row.sender_days_to_last_transaction_7m,
                    row.01_hour_receiver]
    
        # row_v = [i for i in row]
        # print(row_v)
        giros_calificados = calificar(row_v)
        # print(giros_calificados)
        print(giros_calificados['WAS_FRAUD'], giros_calificados['PROBABILIDAD'])
        print ("Giro calificado exitosamente!")
        cursor = conectarse()
        guardar_score(giros_calificados,cursor)
        i += 1 
        print('Filas guardadas: {}'.format(i))
        row = cursor_q.fetchone()
    cursor_q.close()
    return 

def guardar_score (row:dict,cursor):
    
    query2 = '''EXECUTE [Fraud].[Set_Fraud_Score_V2] @ID_BRANCH = ?, @ID_RECEIVER = ?, @SCORE = ?, @WAS_FRAUD =?'''
    cursor.execute(query2, (row['ID_BRANCH'],row['ID_RECEIVER'],row['PROBABILIDAD'],int(row['WAS_FRAUD'])))
    print ("Giro guardado exitosamente!")

    return  

def main():
    cursor=conectarse()
    get_data(cursor)
    
    return

main()

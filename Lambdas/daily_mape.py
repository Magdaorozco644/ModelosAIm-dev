import awswrangler as wr
import boto3
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# Config client S3
s3 = boto3.client('s3')

def calculate_mape(merged_df):
    # Calculate mean absolute error
    merged_df['pred'] = merged_df['pred'].astype(float)
    merged_df['amount'] = merged_df['amount'].astype(float)
    merged_df['abs_error'] = np.abs(merged_df['pred'] - merged_df['amount'])

    # ABS error
    merged_df['MAPE'] = (merged_df['abs_error'] / merged_df['amount'])

    merged_df[merged_df['amount'] == 0]
    # Select columns
    mape_df = merged_df[['processing_date', 'pred_date', 'pred', 'payer_country',
            'id_main_branch', 'id_country', 'amount', 'abs_error', 'MAPE']]

    return mape_df


def lambda_handler(event, context):
    s3_bucket = os.environ['BUCKET'] # 'viamericas-datalake-dev-us-east-1-283731589572-analytics'
    inference_prefix = os.environ['PREFIX'] # 'inference_prediction'
    save_prefix = os.environ['SAVE_PREFIX']

    # Setting processing_date
    processing_date = datetime.today().strftime('%Y-%m-%d')
    previous_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f'Processing date: {processing_date}')

    # Read Daily Check
    database_name = os.environ['DATABASE'] # "analytics"
    table_name = os.environ['TABLE_NAME'] # "daily_check_gp"
    df_daily = wr.athena.read_sql_table(
        table=table_name,
        database=database_name,
    )

    # Cast to date
    df_daily['date'] = pd.to_datetime(df_daily['date'])
    print(f'Shape daily check: {df_daily.shape}')

    # Read predictions
    # TODO: solo para 2d?
    predict_path = f's3://{s3_bucket}/{inference_prefix}/day={previous_date}/predictions_2d/'
    df_predict = wr.s3.read_parquet(predict_path)
    print(f'Shape predict: {df_predict.shape}')

    # Merge data frames
    merged_df = pd.merge(df_predict, df_daily, left_on=['pred_date','id_main_branch','id_country', 'payer', 'country'], 
                        right_on=['date','id_main_branch','id_country', 'payer', 'country'])
    print(f'Shape merge: {merged_df.shape}')

    mape_df = calculate_mape(merged_df=merged_df)
    print(f'Shape mape: {mape_df.shape}')

    # TODO: solo a s3? Enviar a redshift?
    to_save = f's3://{s3_bucket}/{save_prefix}/day={previous_date}/mape_2d/'
    print(f'To save: {to_save}')
    wr.s3.to_parquet(df=mape_df,path=to_save)

    return {
        'statusCode': 200
    }

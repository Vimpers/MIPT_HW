import pandas as pd
from pathlib import Path
import logging
import pendulum
from datetime import date, timedelta
from airflow.decorators import dag, task

"извлекаем данные таблицы прибыли из CSV."
ProfitTable = pd.DataFrame
@task(task_id="extract", execution_timeout=timedelta(minutes=5))
def extract_profit_table(csv_source: Path = Path("profit_table.csv")) -> ProfitTable:
    return pd.read_csv(csv_source)

"затем трансформируем"
@task(task_id="transform", execution_timeout=timedelta(minutes=5), retries=5)
def transform_profit_table(profit_table: ProfitTable, current_date: date) -> ProfitTable:
    logger = logging.getLogger("airflow.task")
    current_date = pd.to_datetime(current_date)
    start_date = current_date - pd.DateOffset(months=2)
    end_date = current_date + pd.DateOffset(months=1)
    date_list = pd.date_range(
        start=start_date, end=end_date, freq='M'
    ).strftime('%Y-%m-01')

    df_tmp = (
        profit_table[profit_table['date'].isin(date_list)]
        .drop('date', axis=1)
        .groupby('id')
        .sum()
    )

    product_list = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j')
    for product in product_list:
        df_tmp[f'flag_{product}'] = (
            df_tmp.apply(
                lambda x: x[f'sum_{product}'] != 0 and x[f'count_{product}'] != 0,
                axis=1
            ).astype(int)
        )

    df_tmp = df_tmp.filter(regex='flag').reset_index()
    logger.info(f"Таблица трансформирована для {current_date}")
    return df_tmp

@task(task_id="load", execution_timeout=timedelta(minutes=5))
def load_activity_table(
    activity_table: ProfitTable,
    csv_target: Path = Path("activity_table.csv")
) -> None:
    
    "теперь загружаем данные таблицы активности."
    if csv_target.exists():
        activity_table.to_csv(csv_target, mode='a', header=False, index=False, line_terminator='\n')
    else:
        activity_table.to_csv(csv_target, index=False)

@dag(dag_display_name="ETL2024", start_date=pendulum.now(), schedule_interval="0 0 5 * *", tags=["MIPT"], dag_id="etl_2024")
def etl_2024():
    
    "Определяем DAG для ETL-процесса."
    extract = extract_profit_table()
    transform = transform_profit_table(extract, date.today())
    load = load_activity_table(transform)
    extract >> transform >> load
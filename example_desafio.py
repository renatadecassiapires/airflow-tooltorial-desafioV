from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import pandas as pd
import sqlite3

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##


def read_orders_and_save_to_csv():
    # Conectar ao banco de dados
    con = sqlite3.connect('data/Northwind_small.sqlite')

    # Ler a tabela 'Order' do banco de dados
    orders = pd.read_sql_query("SELECT * FROM 'Order'", con)

    # Salvar os dados em um arquivo CSV
    orders.to_csv('output_orders.csv', index=False)

    # Fechar a conexão com o banco de dados
    con.close()





def join_order_detail_with_csv():
    # Conectar ao banco de dados
    con = sqlite3.connect('data/Northwind_small.sqlite')

    # Ler a tabela 'OrderDetail' do banco de dados
    order_detail = pd.read_sql_query("SELECT * FROM OrderDetail", con)

    # Ler o arquivo 'output_orders.csv' gerado na tarefa anterior
    orders = pd.read_csv('output_orders.csv')

    # Fazer o JOIN das tabelas
    merged_data = pd.merge(orders, order_detail, how="inner", left_on="Id", right_on="OrderId")

    # Filtrar por Rio de Janeiro 
    filtered_df = merged_data.query('ShipCity == "Rio de Janeiro"')
    
    # Somar as quantidades de vendas para o Rio de Janeiro
    count = str(filtered_df['Quantity'].sum())

    # Salvar o resultado em um novo arquivo CSV
    with open("count.txt", 'w') as f:
        f.write(count)

    # Fechar a conexão com o banco de dados
    con.close()




with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
   
    orders_and_save_to_csv = PythonOperator(
        task_id='orders_and_save_to_csv',
        python_callable=read_orders_and_save_to_csv,
        provide_context=True
   )

    join_order_csv = PythonOperator(
        task_id='join_order_csv',
        python_callable=join_order_detail_with_csv,
        provide_context=True
    )
    
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )
    
    
    orders_and_save_to_csv >> join_order_csv >> export_final_output
#Pip install apache-airflow-providers-postgres
import time 
import json 
from airflow import DAG 
from airflow.operators.postgres_operator import PostgresOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 1, 
    'retry_delay': timedelta(minutes=5), 
}

#Write a DAG which creates an employe table - each row represents a new person and contains info about their name and age
#then insert 3 people (with the correct metadata)
#finally execute a query which calculates the average age of all employees
create_query = """ 
CREATE TABLE employee (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INT NOT NULL
);
"""

#create a logic that populates the table with some data
insert_data_query = """ 
INSERT INTO employee (id, name, age) VALUES (1, 'Enis Mert', 24);
INSERT INTO employee (id, name, age) VALUES (2, 'Aslı Özcan', 25);
INSERT INTO employee (id, name, age) VALUES (3, 'Margot Robbie', 35);
"""

calculating_averag_age = """ 
SELECT AVG(age) as average_age FROM employee;
"""

dag_postgres = DAG(dag_id = "postgres_dag_connection", default_args = default_args, schedule_interval = None, start_date = days_ago(1))

#here you will define the tasks by calling the operator
create_table = PostgresOperator(task_id = "creation_of_table", sql = create_query, dag = dag_postgres, postgres_conn_id = "postgres_pedro_local")

insert_data = PostgresOperator(task_id = "insertion_of_data", sql = insert_data_query, dag = dag_postgres, postgres_conn_id = "postgres_pedro_local")

group_data = PostgresOperator(task_id = "calculating_averag_age", sql = calculating_averag_age, dag = dag_postgres, postgres_conn_id = "postgres_pedro_local")

create_table >> insert_data >> group_data
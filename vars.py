from sqlalchemy import create_engine

engine_de = create_engine('postgresql://postgres:@host.docker.internal:5432/de_course')
path = '/opt/airflow/dags/de_course'

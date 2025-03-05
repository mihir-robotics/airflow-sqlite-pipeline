# Commands to run airflow server

#1. Change directory to airflow-env
cd airflow-dir/

#2. Source the environment
source airflow-env/bin/activate

#3. Start airflow webserver
airflow webserver --port 8080

#4. Start airflow scheduler in another terminal after sourcing the environment
airflow scheduler
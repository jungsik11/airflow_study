# airflow_study
1. install airflow 
```
# Airflow needs a home. `~/airflow` is the default, but you can put it
# somewhere else if you prefer (optional)
export AIRFLOW_HOME=/Users/yangjungsik/Desktop/git/airflow_study

# Install Airflow using the constraints file
AIRFLOW_VERSION=2.2.4
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.6
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.2.4/constraints-3.6.txt
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Visit localhost:8080 in the browser and use the admin account details
# shown on the terminal to login.
# Enable the example_bash_operator dag in the home page
```

2. Change config file
```
# Whether to load the DAG examples that ship with Airflow. It's good to
# get started, but you probably want to set this to ``False`` in a production
# environment
load_examples = False

# Whether to enable pickling for xcom (note that this is insecure and allows for
# RCE exploits).
enable_xcom_pickling = True

# Expose the configuration file in the web server
expose_config = True

```
3. Create User
```
airflow users create --username airflow --password airflow \
    --firstname Peter --lastname Parker --role Admin --email example@test.org
```

4. Stard airflow
export AIRFLOW_HOME=/Users/yangjungsik/Desktop/git/airflow_study로 환경변수를 터미널 실행시마다 변경해줘야함
   
```
export AIRFLOW_HOME=/Users/yangjungsik/Desktop/git/airflow_study

airflow webserver --port 8080

airflow scheduler
```
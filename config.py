import pymysql
from sshtunnel import SSHTunnelForwarder

# Remote Config
SSH_HOST = '3.125.139.40'
SSH_PORT = 18822
SSH_USER = 'devuser_tunnels'
SSH_PASS = 'LnfuhKrrVWACKDTkPUIS'

RDS_HOST = 'osaio-eu-bicenter.cbpim8frjuhu.eu-central-1.rds.amazonaws.com'
RDS_PORT = 3306
DB_USER = 'readonly'
DB_PASS = 'WHFOWEIF##$#$...'
DB_NAME = 'bi_center'

# Mapping local port to avoid conflicts
LOCAL_BIND_PORT = 13309

VALID_PRODUCTS = [
    '14-day history event recording monthly',
    '30-day video history event recording pro monthly',
    '7-day video history CVR recording monthly',
    '30-day video history event recording monthly',
    '14-day video history CVR recording pro monthly',
    '30-day video history event recording AI monthly',
    '14-day video history event recording annually',
    '14-day video history CVR recording monthly',
    '30-day video history event recording pro annually',
    '7-day video history CVR recording annually',
    '14-day video history CVR recording pro annually',
    '30-day video history event recording annually',
    '30-day video history event recording AI annually',
    '14-day video history CVR recording AI monthly',
    '14-day video history CVR recording AI annually',
    '14-day video history CVR recording annually'
]

PRODUCT_CODE_MAPPING = {
    "04ddc7037a5c1643e4b70b8e7511e446": "14-day video history CVR recording monthly",
    "25ef29e69266a18e4e62a9abfb24906e": "7-day video history CVR recording monthly",
    "2d45e3725a5340568d134b65d0c7caa2": "14-day history event recording monthly",
    "39ead1f70dc714c3b77f59cff2f9b6a3": "14-day video history CVR recording annually",
    "3d0ac7655d04b572480900cc01fb598b": "30-day video history event recording AI annually",
    "675dab39bc46e7c9f19976c6df9cab83": "30-day video history event recording annually",
    "689860a9db2aac4e0375d2edbaf73faf": "14-day video history event recording annually",
    "71d7bf50084b13308df8baa5d0ab67b4": "30-day video history event recording monthly",
    "72ae5ef6671ee995ce33e0f66b46c7f9": "14-day video history CVR recording AI annually",
    "72ea10ef0bae90d823529d12ead51a96": "14-day video history CVR recording pro annually",
    "8590980d14c303bb7ef93bb30432901d": "14-day video history CVR recording AI monthly",
    "a4a1ccf545b9f7e1c9dede93d2aae78f": "30-day video history event recording pro annually",
    "c9f452e0f08e80c1093a82616cc946a4": "30-day video history event recording AI monthly",
    "d90d40f341f05593daf9d4c4702f8bee": "14-day video history CVR recording pro monthly",
    "eb34f5845642ed10ab2d91703baf28e3": "30-day video history event recording pro monthly",
    "ef8d5c18b133ebf0c514f7c9188ca6a3": "7-day video history CVR recording annually"
}

def get_plan_type(product_name):
    if 'annually' in product_name.lower():
        return 'Yearly'
    return 'Monthly'

def get_db_connection(local_port=LOCAL_BIND_PORT):
    return pymysql.connect(host='127.0.0.1', port=local_port, user=DB_USER, password=DB_PASS, database=DB_NAME)

def create_ssh_tunnel(local_port=LOCAL_BIND_PORT):
    return SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASS,
        remote_bind_address=(RDS_HOST, RDS_PORT),
        local_bind_address=('127.0.0.1', local_port),
        allow_agent=False
    )

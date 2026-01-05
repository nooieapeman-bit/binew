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
    '30-day video history event recording pro monthly ',
    '7-day video history CVR recording monthly ',
    '30-day video history event recording monthly',
    '14-day video history CVR recording pro monthly',
    '30-day video history event recording AI monthly',
    '14-day video history event recording annually  ',
    '14-day video history CVR recording monthly  ',
    '30-day video history event recording pro annually',
    '7-day video history CVR recording annually',
    '14-day video history CVR recording pro annually   ',
    '30-day video history event recording annually',
    '30-day video history event recording AI annually',
    '14-day video history CVR recording AI monthly',
    '14-day video history CVR recording AI annually',
    '14-day video history CVR recording annually '
]

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

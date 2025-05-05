import pymongo as mongo

dataBase = "congo_nokia_project_telco360_topology"
from_db = "huawei_mpbn_alarms"
to_db = "sam_ipran_alarms"

to_save_db = "Corel_test"
to_save_collection = "congoTesting_corels_without_clear"



def getConnection():
    authSrc = "admin"
    port = "27017"
    db_ip = "192.168.100.100"
    db_pwd = "Xplor12"
    db_user = "msRoot"
    database_ip = '145.123.145.2'
    database_port = 27017
    connection_string = f"mongodb://{db_user}:{db_pwd}@{db_ip}:{port}/{authSrc}?authSource={authSrc}"
    # Add connection pooling options
    connection = mongo.MongoClient(
        connection_string
    )
    return connection
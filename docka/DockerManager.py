import subprocess
import time
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values 
# loading variables from .env file
load_dotenv() 

def getDockerID(name):
    id = str(subprocess.check_output('docker ps -aqf "name=elastic"', shell=True))
    id = id.replace("b'","").replace("'","").replace("\\n","")
    if '' != id:
        return id
    return None

def stop(dockerId):
    id = str(subprocess.check_output('docker stop ' + str(dockerId), shell=True))
    return id

def remove(dockerId):
    id = str(subprocess.check_output('docker rm ' + dockerId, shell=True))
    return id
def start(cmd):
    id = str(subprocess.call(cmd, shell=True))
    return id

# 8.8.0
# 7.17.26

# docker run --rm --name elasticsearch -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" -e "xpack.security.enabled=false" elasticsearch:7.17.26
# docker run --restart=always -d --name elasticsearch -p  9200:9200 -e "http.host=0.0.0.0" -e "transport.host=127.0.0.1" docker.elastic.co/elasticsearch/elasticsearch:7.17.26
def setUpESDcokerServer():
    lastID = getDockerID('elastic')
    if lastID != None:
        print("Stoping ", lastID)
        stop(lastID)
        remove(lastID)
    print("-",start('docker run --restart=always -d --name elasticsearch -p  9200:9200 -e "http.host=0.0.0.0" -e "transport.host=127.0.0.1" docker.elastic.co/elasticsearch/elasticsearch:7.17.26'), "-")
    time.sleep(10)

def composeUp():
    return start("docker-compose up")

def composeDown():
    return start("docker-compose down")
    

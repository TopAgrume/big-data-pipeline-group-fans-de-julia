# ***Instructions***

## ***/hdfs***
### ***Build/Stop***
```bash
# Build and run the hdfs (namenode/datanode instances) :
docker-compose up -d

# To stop the docker instances: (optionnal, keep them to keep the data) :
docker-compose-stop
```

### ***Set up you local machine***
```bash
# We need to find on which ip adress the namenode docker runs :
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' namenode

# We write this into our hosts file to link namenode with the ip address :
sudo vim /etc/hosts
<ip_adress> namenode
example: 0.0.0.0 namenode 
```

### ***If this is the first time running the docker instance***
```bash
# Create the necessary directories :
docker exec -it namenode hdfs dfs -mkdir /user/
docker exec -it namenode hdfs dfs -mkdir /user/clovinux
docker exec -it namenode hdfs dfs -chown clovinux /user/clovinux

# Give full rights otherwise sbt doesn't load the data :
docker exec -it namenode hdfs dfs -chmod 777 /user
docker exec -it namenode hdfs dfs -chmod 777 /user/clovinux
```

## ***/data_ingestion***
### ***Compile/Run to load the data !***
```bash
#  
sbt run

# To check if the data was loaded in the namenode (hdfs) docker instance :
docker exec -it namenode hdfs dfs -ls /user/hadoop/ecommerce_data

# And we're done !
# too easy..
```

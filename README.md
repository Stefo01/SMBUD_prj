# SMBUD Project Setup

To set up the project, you'll need to install Docker and Docker Compose. The instructions below assume you're using a Linux system like Ubuntu.
First of all, run the ```pre_queries.py``` file

## 1. Install Docker and Docker Compose for Kibana and elastic search

If you haven't already installed Docker and Docker Compose, follow these commands:

```bash
sudo apt-get update
sudo apt-get install docker-compose-plugin
```

Check the version with: 

```bash
docker-compose --version
```

## 2. Build docker for elastic search and kibana
After that to build the project is sufficient to run in the main folder:

```bash
docker-compose build
```

Note: The initial build take some time, have a think about that. Use this cmd only one time to build everything. It is not necessary to run all the times.

After that you can run all using: 

```bash
docker-compose up
```

And when you want to stop all, you can use: 

```bash
docker-compose down
```
Or ```ctrl+c``` to stop the execution from same terminal. 

Note: use ```docker-compose build``` cmd only one time to build everything. It is not necessary to run all the times.

## 3. Run MongoDB docker service

Go into ```MongoDB``` repo and run the following cmds:

```bash
docker build -t my-mongodb .
docker run -d -p 27017:27017 --name mongodb my-mongodb
docker cp archive/filtered_file.csv mongodb:/filtered_file.csv
docker exec -i mongodb mongoimport   --host 0.0.0.0   --port 27017   --db crime_data   --collection crime_records   --type csv   --file filtered_file.csv   --headerline
```
To run queries, see the ```queries.py```

## 4. Access services

Finally, you will find all services running at:

- MongoDB: http://localhost:27017
- Elasticsearch: https://localhost:9200 
- Kibana: http://localhost:5601

NOTE: For kibana UI use:
- UserName: elastic
- pw: SecureP4ssword

Or go into ```elasticsearch.py``` file and run whatever query you want.
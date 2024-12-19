# SMBUD Project Setup

To set up the project, you'll need to install Docker and Docker Compose. The instructions below assume you're using a Linux system like Ubuntu or WSL.

## 1. Install Docker and Docker Compose

If you haven't already installed Docker and Docker Compose, follow these commands:

```bash
sudo apt-get update
sudo apt-get install docker-compose-plugin
```

Check the version with: 

```bash
docker-compose --version
```

## 2. Install npm Dependencies

You'll also need to have the npm package manager installed. If you haven't installed it yet, please do so.

Once npm is installed, navigate to the ```web_ui``` directory and run the following command to install all necessary dependencies:

```bash
cd /web_ui
npm install
```

To verify the installation, check the subdomain after installation.

## 3. Build and run the project

After that to build all the project is sufficient to run in the main folder:

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

## 4. Access services

Finally, you will find all services running at:

- MongoDB: http://localhost:27017
- Elasticsearch: https://localhost:9200 
- Kibana: http://localhost:5601 to access the interface
- Web UI: http://localhost:8080 to see the unified management interface
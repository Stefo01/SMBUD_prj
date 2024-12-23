# Use the official MongoDB image from Docker Hub
FROM mongo:latest

# Set the working directory inside the container
WORKDIR /data/db

COPY archive/filtered_file.csv /filtered_file.csv

# Expose the MongoDB default port
EXPOSE 27017

# Start MongoDB server when the container launches
CMD ["sh", "-c", "mongod --fork --logpath /var/log/mongodb.log && mongoimport --db your_database_name --collection your_collection_name --type csv --file /your_file.csv --headerline && tail -f /var/log/mongodb.log"]

# To Run
# docker build -t my-mongodb .
# docker run -d -p 27017:27017 --name mongodb my-mongodb

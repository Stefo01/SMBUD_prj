# Use the official MongoDB image from Docker Hub
FROM mongo:latest

# Set the working directory inside the container
WORKDIR /data/db

# Expose the MongoDB default port
EXPOSE 27017

# Start MongoDB server when the container launches
CMD ["mongod"]

# To Run
# docker build -t my-mongodb .
# docker run -d -p 27017:27017 --name mongodb my-mongodb

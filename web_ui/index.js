const express = require('express');
const { MongoClient } = require('mongodb');
const { Client } = require('@elastic/elasticsearch');

const app = express();
const PORT = 8080;

// MongoDB Configuration
const mongoUrl = "mongodb://admin:admin123@mongodb:27017";
const mongoClient = new MongoClient(mongoUrl);

// Elasticsearch Configuration
const esClient = new Client({ node: 'http://elasticsearch:9200' });

app.get('/', (req, res) => {
    res.send('Welcome to the Unified Management Interface');
});

// Example endpoint for MongoDB
app.get('/mongodb-status', async(req, res) => {
    try {
        await mongoClient.connect();
        res.json({ status: 'MongoDB is connected' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Example endpoint for Elasticsearch
app.get('/elasticsearch-status', async(req, res) => {
    try {
        const health = await esClient.cluster.health();
        res.json(health);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.listen(PORT, () => {
    console.log(`Web UI is running on http://localhost:${PORT}`);
});
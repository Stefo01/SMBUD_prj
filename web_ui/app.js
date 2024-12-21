async function checkMongoDBStatus() {
    const resultElement = document.getElementById('mongodb-result');
    resultElement.textContent = 'Checking MongoDB status...';

    try {
        const response = await fetch('/mongodb-status');
        const data = await response.json();
        
        if (response.ok) {
            resultElement.textContent = `MongoDB Status: ${data.status}`;
        } else {
            resultElement.textContent = `Error: ${data.error}`;
        }
    } catch (error) {
        resultElement.textContent = `Error: ${error.message}`;
    }
}

async function checkElasticsearchStatus() {
    const resultElement = document.getElementById('elasticsearch-result');
    resultElement.textContent = 'Checking Elasticsearch status...';

    try {
        const response = await fetch('/elasticsearch-status');
        const data = await response.json();

        if (response.ok) {
            resultElement.textContent = `Elasticsearch Status: ${data.status}`;
        } else {
            resultElement.textContent = `Error: ${data.error}`;
        }
    } catch (error) {
        resultElement.textContent = `Error: ${error.message}`;
    }
}
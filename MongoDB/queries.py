# %%

import pandas as pd
from pymongo import MongoClient

# Load CSV into a Pandas DataFrame
# csv_file_path = 'your_dataset.csv'
# data = pd.read_csv(csv_file_path)


# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Adjust if MongoDB is on another host/port
db = client['crime_data']  # Database name
collection = db['crime_records']  # Collection name


# %%

# 1: luogo di maggiore concentrazione di tutti i reati e il nome di quello più comune in questo luogo
documents = collection.aggregate([
  # Step 1: Raggruppare per borough e sommare i reati
  { "$group": { "_id": "$borough", "totalCrimes": { "$sum": "$value" } } },
  
  # Step 2: Ordinare per reati totali in ordine decrescente
  { "$sort": { "totalCrimes": -1 } },
  
  # Step 3: Limitare al borough con il maggior numero di reati
  { "$limit": 1 },

  # Step 4: Trovare il reato più comune in quel borough
  {
    "$lookup": {
      "from": "crime_records",        # Nome della collezione originale
      "localField": "_id",            # Borough corrispondente
      "foreignField": "borough",      # Campo nel dataset originale
      "as": "boroughCrimes"           # Nuova lista con dettagli
    }
  }
])

# Print the results
for doc in documents:
    print(doc)

# %%

# 2: luogo di minor concentrazione di tutti i reati (più sicuro)
db.crime_records.aggregate([
  { "$group": { "_id": "$borough", "totalCrimes": { "$sum": "$value" } } },
  { "$sort": { "totalCrimes": 1 } },
  { "$limit": 1 }
])

# 3: luogo con minor concentrazione di violenza contro le persone e le cose. Ritorna il numero di reati totali di quel tipo
db.crime_records.aggregate([
  # Step 1: Filtrare solo i reati legati a "Violence Against the Person" o "Criminal Damage"
  {
    "$match": {
      "major_category": { "$in": ["Violence Against the Person", "Criminal Damage"] }
    }
  },

  # Step 2: Raggruppare per borough e sommare il numero di reati
  {
    "$group": {
      "_id": "$borough",
      "totalCrimes": { "$sum": "$value" }
    }
  },

  # Step 3: Ordinare per numero di reati in ordine crescente
  { "$sort": { "totalCrimes": 1 } },

  # Step 4: Limitare al primo risultato (il luogo con meno reati)
  { "$limit": 1 }
])

# 4: mese con più reati e la sua tipologia. Ritorna anno, mese, luogo con relativi valori
db.crime_records.aggregate([
  # Step 1: Raggruppare per anno, mese, borough, e tipologia di reato, sommando i valori
  {
    "$group": {
      "_id": {
        "year": "$year",
        "month": "$month",
        "borough": "$borough",
        "major_category": "$major_category"
      },
      "totalCrimes": { "$sum": "$value" }
    }
  },

  # Step 2: Ordinare per numero totale di crimini in ordine decrescente
  { "$sort": { "totalCrimes": -1 } },

  # Step 3: Limitare al primo risultato (mese con più reati)
  { "$limit": 1 }
])

# 5: anno con più reati, dove si sono concentrati e i mesi peggiori
db.crime_records.aggregate([
  # Step 1: Raggruppare per anno e sommare i reati totali
  {
    "$group": {
      "_id": "$year",
      "totalCrimes": { "$sum": "$value" }
    }
  },

  # Step 2: Ordinare per numero totale di reati in ordine decrescente
  { "$sort": { "totalCrimes": -1 } },

  # Step 3: Limitare al primo risultato (anno con il massimo numero di reati)
  { "$limit": 1 },

  # Step 4: Espandere i dettagli dei reati di quell'anno
  {
    "$lookup": {
      "from": "crime_records",    # Nome della collezione originale
      "localField": "_id",        # L'anno individuato
      "foreignField": "year",     # Campo "year" nella collezione originale
      "as": "yearDetails"
    }
  }
])

# 6: reato meno frequente e quartiere che ne ha di più
db.crime_records.aggregate([
  # Step 1: Raggruppare per major_category (reato) e sommare il numero di reati
  {
    "$group": {
      "_id": "$major_category",  # Raggruppa per tipologia di reato
      "totalCrimes": { "$sum": "$value" }  # Somma il numero di reati per ogni tipo
    }
  },

  # Step 2: Ordinare per numero di reati in ordine crescente (reato meno frequente)
  { "$sort": { "totalCrimes": 1 } },

  # Step 3: Limitare al primo risultato (reato meno frequente)
  { "$limit": 1 },

  # Step 4: Espandere i dettagli per trovare il quartiere (borough) con il massimo di quel tipo di reato
  {
    "$lookup": {
      "from": "crime_records",     # Nome della collezione originale
      "localField": "_id",         # Tipo di reato trovato
      "foreignField": "major_category",  # Campo corrispondente "major_category"
      "as": "crimeDetails"         # Dettagli dei reati di quel tipo
    }
  }
])

# 7: Numero di crimini contro le persone in Covent Garden e South Bank
db.crime_records.aggregate([
  # Step 1: Filter records for Covent Garden and South Bank, and "Violence Against the Person"
  {
    "$match": {
      "borough": { "$in": ["Covent Garden", "South Bank"] },
      "major_category": "Violence Against the Person"
    }
  },

  # Step 2: Group by borough, month, and type
  {
    "$group": {
      "_id": {
        "borough": "$borough",
        "month": "$month",
        "type": "$minor_category"
      },
      "totalCrimes": { "$sum": "$value" }
    }
  },

  # Step 3: Sort results by borough and month
  { "$sort": { "_id.borough": 1, "_id.month": 1 } }
])

# 8: Nel mese di dicembre, qual'è il luogo con più reati e quello con meno reati
db.crime_records.aggregate([
  # Step 1: Filter records for December
  {
    "$match": { "month": 12 }
  },

  # Step 2: Group by borough to sum up crimes
  {
    "$group": {
      "_id": "$borough",
      "totalCrimes": { "$sum": "$value" }
    }
  },

  # Step 3: Sort results to find boroughs with most and least crimes
  { "$sort": { "totalCrimes": 1 } },

  # Step 4: Add fields for least and most crimes
  {
    "$facet": {
      "leastCrimes": [{ "$limit": 1 }],
      "mostCrimes": [{ "$sort": { "totalCrimes": -1 } }, { "$limit": 1 }]
    }
  }
])

# 9: A dicembre, nel luogo con numero di reati minore, quali sono i reati commessi maggiormente
db.crime_records.aggregate([
  # Step 1: Filter for December in the borough with the least crimes
  {
    "$match": {
      "month": 12,
      "borough": "<BOROUGH_NAME>"
    }
  },

  # Step 2: Group by major category to sum up crimes
  {
    "$group": {
      "_id": "$major_category",
      "totalCrimes": { "$sum": "$value" }
    }
  },

  # Step 3: Sort by the number of crimes to find the most common types
  { "$sort": { "totalCrimes": -1 } },

  # Step 4: Limit to the top results
  { "$limit": 5 }
])

# 10: Percentuale di crimini per "Major Category". Ritorna il podio
db.crime_records.aggregate([
  { "$group": { "_id": "$major_category", "totalCrimes": { "$sum": "$value" } } },
  { "$group": { 
      "_id": null, 
      "categories": { "$push": { "category": "$_id", "count": "$totalCrimes" } },
      "total": { "$sum": "$totalCrimes" }
  } },
  { "$unwind": "$categories" },
  { "$project": {
      "_id": 0,
      "category": "$categories.category",
      "percentage": { "$multiply": [{ "$divide": ["$categories.count", "$total"] }, 100] }
  } },
  { "$sort": { "percentage": -1 } },
  { "$limit": 3 }
])

# %%

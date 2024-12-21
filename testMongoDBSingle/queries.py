# %%
# import kagglehub

# # Download latest version
# path = kagglehub.dataset_download("jboysen/london-crime")

# print("Path to dataset files:", path)

import pandas as pd
from pymongo import MongoClient

# Load CSV into a Pandas DataFrame
csv_file_path = 'your_dataset.csv'
data = pd.read_csv(csv_file_path)


# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Adjust if MongoDB is on another host/port
db = client['crime_data']  # Database name
collection = db['crime_records']  # Collection name


# %%

cc = 0
Totcategoriess = []
for _, row in data.iterrows():
    record = {
        "lsoa_code": row["lsoa_code"],
        "borough": row["borough"],
        "major_category": row["major_category"],
        "minor_category": row["minor_category"],
        "value": row["value"],
        "year": row["year"],
        "month": row["month"]
    }
    collection.insert_one(record)  # Insert the record into MongoDB
    cc += 1


print("Data has been successfully imported into MongoDB!")
print("Total number of entries in MongoDB " + str(cc))


# %%
# Query the collection
documents = collection.find({"value": {"$gt": 2}})  # Find documents where age > 25

# Print the results
for doc in documents:
    print(doc)




# descrizione di test per queries:

# 1: luogo di maggiore concentrazione di tutti i reati e il nome di quello più comune in questo luogo
db.crime_records.aggregate([
  // Step 1: Raggruppare per borough e sommare i reati
  { $group: { _id: "$borough", totalCrimes: { $sum: "$value" } } },
  
  // Step 2: Ordinare per reati totali in ordine decrescente
  { $sort: { totalCrimes: -1 } },
  
  // Step 3: Limitare al borough con il maggior numero di reati
  { $limit: 1 },

  // Step 4: Trovare il reato più comune in quel borough
  {
    $lookup: {
      from: "crime_records",        // Nome della collezione originale
      localField: "_id",            // Borough corrispondente
      foreignField: "borough",      // Campo nel dataset originale
      as: "boroughCrimes"           // Nuova lista con dettagli
    }
  },
  
  // Step 5: Determinare il reato più comune nel borough selezionato
  {
    $addFields: {
      mostCommonCrime: {
        $arrayElemAt: [
          {
            $sortArray: {
              input: {
                $reduce: {
                  input: "$boroughCrimes",
                  initialValue: [],
                  in: {
                    $concatArrays: [
                      "$$value",
                      [
                        {
                          category: "$$this.major_category",
                          count: "$$this.value"
                        }
                      ]
                    ]
                  }
                }
              },
              by: "$category"
            }
          },
        ]
      }
    }
  },

]);

# 2: luogo di minor concentrazione di tutti i reati (più sicuro)
db.crime_records.aggregate([
  { "$group": { "_id": "$borough", "totalCrimes": { "$sum": "$value" } } },
  { "$sort": { "totalCrimes": 1 } },
  { "$limit": 1 }
])

# 3: luogo con minor concentrazione di violenza contro le persone e le cose. Ritorna il numero di reati totali di quel tipo
db.crime_records.aggregate([
  // Step 1: Filtrare solo i reati legati a "Violence Against the Person" o "Criminal Damage"
  {
    $match: {
      major_category: { $in: ["Violence Against the Person", "Criminal Damage"] }
    }
  },

  // Step 2: Raggruppare per borough e sommare il numero di reati
  {
    $group: {
      _id: "$borough",
      totalCrimes: { $sum: "$value" }
    }
  },

  // Step 3: Ordinare per numero di reati in ordine crescente
  { $sort: { totalCrimes: 1 } },

  // Step 4: Limitare al primo risultato (il luogo con meno reati)
  { $limit: 1 }
]);

# 4: mese con più reati e la sua tipologia. Ritorna anno, mese, luogo con relativi valori
db.crime_records.aggregate([
  // Step 1: Raggruppare per anno, mese, borough, e tipologia di reato, sommando i valori
  {
    $group: {
      _id: {
        year: "$year",
        month: "$month",
        borough: "$borough",
        major_category: "$major_category"
      },
      totalCrimes: { $sum: "$value" }
    }
  },

  // Step 2: Ordinare per numero totale di crimini in ordine decrescente
  { $sort: { totalCrimes: -1 } },

  // Step 3: Limitare al primo risultato (mese con più reati)
  { $limit: 1 }
]);

# 5: anno con più reati, dove si sono concentrati e i mesi peggiori
db.crime_records.aggregate([
  // Step 1: Raggruppare per anno e sommare i reati totali
  {
    $group: {
      _id: "$year",
      totalCrimes: { $sum: "$value" }
    }
  },

  // Step 2: Ordinare per numero totale di reati in ordine decrescente
  { $sort: { totalCrimes: -1 } },

  // Step 3: Limitare al primo risultato (anno con il massimo numero di reati)
  { $limit: 1 },

  // Step 4: Espandere i dettagli dei reati di quell'anno
  {
    $lookup: {
      from: "crime_records",    // Nome della collezione originale
      localField: "_id",        // L'anno individuato
      foreignField: "year",     // Campo "year" nella collezione originale
      as: "yearDetails"
    }
  },

  // Step 5: Determinare il luogo (borough) con maggiore concentrazione di reati
  {
    $addFields: {
      boroughCrimes: {
        $arrayElemAt: [
          {
            $sortArray: {
              input: {
                $reduce: {
                  input: "$yearDetails",
                  initialValue: [],
                  in: {
                    $concatArrays: [
                      "$$value",
                      [
                        {
                          borough: "$$this.borough",
                          count: "$$this.value"
                        }
                      ]
                    ]
                  }
                }
              },
              by: { count: -1 }
            }
          },
          0
        ]
      }
    }
  },

  // Step 6: Raggruppare per mese e calcolare i peggiori
  {
    $project: {
      year: "$_id",
      totalCrimes: 1,
      topBorough: "$boroughCrimes.borough",
      boroughCrimes: "$boroughCrimes.count",
      worstMonths: {
        $reduce: {
          input: "$yearDetails",
          initialValue: {},
          in: {
            $mergeObjects: [
              "$$value",
              {
                "$$this.month": { $sum: "$$this.value" }
              }
            ]
          }
        }
      }
    }
  }
])

# 6: reato meno frequente e quartiere che ne ha di più
db.crime_records.aggregate([
  // Step 1: Raggruppare per major_category (reato) e sommare il numero di reati
  {
    $group: {
      _id: "$major_category",  // Raggruppa per tipologia di reato
      totalCrimes: { $sum: "$value" }  // Somma il numero di reati per ogni tipo
    }
  },

  // Step 2: Ordinare per numero di reati in ordine crescente (reato meno frequente)
  { $sort: { totalCrimes: 1 } },

  // Step 3: Limitare al primo risultato (reato meno frequente)
  { $limit: 1 },

  // Step 4: Espandere i dettagli per trovare il quartiere (borough) con il massimo di quel tipo di reato
  {
    $lookup: {
      from: "crime_records",     // Nome della collezione originale
      localField: "_id",         // Tipo di reato trovato
      foreignField: "major_category",  // Campo corrispondente "major_category"
      as: "crimeDetails"         // Dettagli dei reati di quel tipo
    }
  },

  // Step 5: Raggruppare per borough e sommare i reati in quel quartiere per quel tipo di reato
  {
    $unwind: "$crimeDetails"
  },
  
  {
    $group: {
      _id: "$crimeDetails.borough",   // Raggruppa per borough
      totalCrimesInBorough: { $sum: "$crimeDetails.value" }  // Somma i reati per quel tipo nel borough
    }
  },

  // Step 6: Ordinare per numero di reati in ordine decrescente per trovare il quartiere con più reati
  { $sort: { totalCrimesInBorough: -1 } },

  // Step 7: Limitare al primo risultato (quartiere con più reati del tipo meno frequente)
  { $limit: 1 }
])

# 7: Numero di crimini contro le persone in Covent Garden e South Bank, i due quartieri più turistici di Londra secondo edreams. Ritorna il valore numerico per mese e la tipologia
ToDO : controlla che siano effettivamente presenti i due quartieri nel dataset
# 8: Nel mese di dicembre, uno tra quelli con più turisti a Londra, qual'è il luogo con più reati e quello con meno reati (per poterci soggiornare)
# 9: A dicembre, nel luogo con numero di reati minore (ritornato sopra), quali sono i reati commessi maggiormente



# 10: Percentuale di crimini per "Major Category". Ritorna il podio
db.crime_records.aggregate([
  { "$group": { "_id": "$major_category", "totalCrimes": { "$sum": "$value" } } },
  { "$group": { 
      "_id": "null", 
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



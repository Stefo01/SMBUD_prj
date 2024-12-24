
import pandas as pd
from pymongo import MongoClient


# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')  # Adjust if MongoDB is on another host/port
db = client['crime_data']  # Database name
collection = db['crime_records']  # Collection name



document1 = db.crime_records.aggregate([
  { "$group": { "_id": "$borough", "totalCrimes": { "$sum": "$value" } } },
  { "$sort": { "totalCrimes": -1 } },
  { "$limit": 1 },
],allowDiskUse=True)

document2 = db.crime_records.aggregate([
  { "$group": { "_id": "$borough", "totalCrimes": { "$sum": "$value" } } },
  { "$sort": { "totalCrimes": 1 } },
  { "$limit": 2 }
],allowDiskUse=True)

document3 = db.crime_records.aggregate([
  {
    "$match": {
      "major_category": { "$in": ["Violence Against the Person", "Criminal Damage"] }
    }
  },

  {
    "$group": {
      "_id": "$borough",
      "totalCrimes": { "$sum": "$value" }
    }
  },

  { "$sort": { "totalCrimes": 1 } },

  { "$limit": 2 }
],allowDiskUse=True)



document4 = db.crime_records.aggregate([
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

  { "$sort": { "totalCrimes": -1 } },

  { "$limit": 1 }
],allowDiskUse=True)

document5 = db.crime_records.aggregate([
  {
    "$group": {
      "_id": "$year",
      "totalCrimes": { "$sum": "$value" }
    }
  },

  { "$sort": { "totalCrimes": -1 } },
  
  { "$limit": 1 }

],allowDiskUse=True)

document6 = db.crime_records.aggregate([
  {
    "$group": {
      "_id": "$major_category",  
      "totalCrimes": { "$sum": "$value" }  
    }
  },

  { "$sort": { "totalCrimes": 1 } },

  { "$limit": 2 },


],allowDiskUse=True)

document7 = db.crime_records.aggregate([
  {
    "$match": {
      "borough": { "$in": ["Westminster", "Lambeth"] },
      "major_category": "Violence Against the Person"
    }
  },

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

  { "$sort": { "_id.borough": 1, "_id.month": 1 } }
],allowDiskUse=True)

document8 = db.crime_records.aggregate([

  {
    "$match": { "month": 12 }
  },

  {
    "$group": {
      "_id": "$borough",
      "totalCrimes": { "$sum": "$value" }
    }
  },

  { "$sort": { "totalCrimes": 1 } },
  {
    "$facet": {
      "leastCrimes": [{ "$limit": 2 }],
      "mostCrimes": [{ "$sort": { "totalCrimes": -1 } }, { "$limit": 2 }]
    }
  }
],allowDiskUse=True)

document9 = db.crime_records.aggregate([
  {
    "$match": {
      "month": 12,
      "borough": "Kingston upon Thames"
    }
  },

  {
    "$group": {
      "_id": "$major_category",
      "totalCrimes": { "$sum": "$value" }
    }
  },

  { "$sort": { "totalCrimes": -1 } },

  { "$limit": 5 }
],allowDiskUse=True)

document10 = db.crime_records.aggregate([
  { "$group": { "_id": "$major_category", "totalCrimes": { "$sum": "$value" } } },
  { "$group": { 
      "_id": 0, 
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
],allowDiskUse=True)


print("Document 1 results:")
for doc in document1:
    print(doc)
print("Document 2 results:")
for doc in document2:
    print(doc)
print("Document 3 results:")
for doc in document3:
    print(doc)
print("Document 4 results:")
for doc in document4:
    print(doc)
print("Document 5 results:")
for doc in document5:
    print(doc)
print("Document 6 results:")
for doc in document6:
    print(doc)
print("Document 7 results:")
for doc in document7:
    print(doc)
print("Document 8 results:")
for doc in document8:
    print(doc)
print("Document 9 results:")
for doc in document9:
    print(doc)
print("Document 10 results:")
for doc in document10:
    print(doc)



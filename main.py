# docker run --name es01 --net elastic -p 9200:9200 -it -m 1GB docker.elastic.co/elasticsearch/elasticsearch:8.17.0
import pandas as pd
from elasticsearch import Elasticsearch, helpers

# Initialize Elasticsearch
# username="elastic", password="changeme"
username = "elastic"
password = "mQexcVF8QeRhWYuHW4IE"
es = Elasticsearch(
    ["https://localhost:9200"],
    basic_auth=(username, password), verify_certs=False
)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)# Path to the GTD dataset (CSV file)
dataset_path = "globalterrorismdb.csv"

# Load the dataset
print("Loading the dataset...")
df = pd.read_csv(dataset_path, encoding="latin1")

# Preprocess the data (optional: handle missing values, column renaming, etc.)
print("Preprocessing the dataset...")
df = df.fillna("")
df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"), inplace=True)

# Index name in Elasticsearch
index_name = "global_terrorism"

# Convert DataFrame to a list of dictionaries
records = df.to_dict(orient="records")

# Create Elasticsearch index
if not es.indices.exists(index=index_name):
    print(f"Creating index: {index_name}")
    es.indices.create(index=index_name)

# Bulk upload data to Elasticsearch
print("Uploading data to Elasticsearch...")
actions = [
    {
        "_index": index_name,
        "_source": record
    }
    for record in records
]

helpers.bulk(es, actions)
print("Data upload complete!")

# Define the index name
index_name = "global_terrorism"

# 1. Total Number of Incidents
def query_total_incidents():
    query = {
        "size": 0
    }
    response = es.search(index=index_name, body=query)
    print(f"Total incidents: {response['hits']['total']['value']}")

# 2. Top 5 Countries with Most Incidents
def query_top_5_countries():
    query = {
        "size": 0,
        "aggs": {
            "top_countries": {
                "terms": { "field": "country_txt.keyword", "size": 5 }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    countries = response['aggregations']['top_countries']['buckets']
    print("Top 5 countries with most incidents:")
    for country in countries:
        print(f"{country['key']}: {country['doc_count']} incidents")

# 3. Incidents by Year
def query_incidents_by_year():
    query = {
        "size": 0,
        "aggs": {
            "incidents_per_year": {
                "date_histogram": {
                    "field": "iyear",  # Field for the year of the incident
                    "calendar_interval": "year"
                }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    years = response['aggregations']['incidents_per_year']['buckets']
    print("Incidents by Year:")
    for year in years:
        year_key = year.get('key_as_string', year.get('key'))  # Check for both key and key_as_string
        print(f"Year: {year_key}, Incidents: {year['doc_count']}")

# 4. Most Common Attack Types
def query_attack_types():
    query = {
        "size": 0,
        "aggs": {
            "attack_types": {
                "terms": { "field": "attacktype1_txt.keyword", "size": 5 }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    attack_types = response['aggregations']['attack_types']['buckets']
    print("Most Common Attack Types:")
    for attack in attack_types:
        print(f"{attack['key']}: {attack['doc_count']} incidents")

# 5. Number of Incidents by Region
def query_incidents_by_region():
    query = {
        "size": 0,
        "aggs": {
            "incidents_by_region": {
                "terms": { "field": "region_txt.keyword", "size": 5 }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    regions = response['aggregations']['incidents_by_region']['buckets']
    print("Incidents by Region:")
    for region in regions:
        print(f"{region['key']}: {region['doc_count']} incidents")

# 6. Top 5 Deadliest Groups
def query_deadliest_groups():
    query = {
        "size": 0,
        "aggs": {
            "deadliest_groups": {
                "terms": { "field": "gname.keyword", "size": 5 },
                "aggs": {
                    "total_deaths": { "sum": { "field": "nkill" } }
                }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    groups = response['aggregations']['deadliest_groups']['buckets']
    print("Top 5 Deadliest Groups:")
    for group in groups:
        print(f"{group['key']}: {group['total_deaths']['value']} total deaths")

# 7. Incidents Involving Hostages
def query_incidents_with_hostages():
    query = {
        "query": {
            "range": { "nhostkid": { "gt": 0 } }
        }
    }
    response = es.search(index=index_name, body=query)
    print(f"Incidents involving hostages: {response['hits']['total']['value']}")

# 8. Average Casualties per Incident
def query_average_casualties():
    query = {
        "size": 0,
        "aggs": {
            "average_casualties": {
                "avg": { "field": "nkill" }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    print(f"Average casualties per incident: {response['aggregations']['average_casualties']['value']}")

# 9. Incidents in Afghanistan
def query_incidents_in_afghanistan():
    query = {
        "query": {
            "match": { "country_txt": "Afghanistan" }
        }
    }
    response = es.search(index=index_name, body=query)
    print(f"Incidents in Afghanistan: {response['hits']['total']['value']}")



# 11. Trends in Suicide Attacks
def query_suicide_attacks():
    query = {
        "query": {
            "term": { "suicide": 1 }
        }
    }
    response = es.search(index=index_name, body=query)
    print(f"Suicide attacks: {response['hits']['total']['value']} incidents")

# 12. Incidents by Weapon Type
def query_weapon_types():
    query = {
        "size": 0,
        "aggs": {
            "weapon_types": {
                "terms": { "field": "weaptype1_txt.keyword" }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    weapon_types = response['aggregations']['weapon_types']['buckets']
    print("Incidents by Weapon Type:")
    for weapon in weapon_types:
        print(f"{weapon['key']}: {weapon['doc_count']} incidents")

# 13. Most Targeted Types of Victims
def query_target_types():
    query = {
        "size": 0,
        "aggs": {
            "target_types": {
                "terms": { "field": "targtype1_txt.keyword" }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    target_types = response['aggregations']['target_types']['buckets']
    print("Most Targeted Types of Victims:")
    for target in target_types:
        print(f"{target['key']}: {target['doc_count']} incidents")

# 14. Number of Incidents with Fatalities
def query_incidents_with_fatalities():
    query = {
        "query": {
            "range": { "nkill": { "gt": 0 } }
        }
    }
    response = es.search(index=index_name, body=query)
    print(f"Incidents with fatalities: {response['hits']['total']['value']} incidents")

# 15. Average Number of Casualties by Attack Type
def query_avg_casualties_by_attack_type():
    query = {
        "size": 0,
        "aggs": {
            "avg_casualties_by_type": {
                "terms": { "field": "attacktype1_txt.keyword" },
                "aggs": {
                    "avg_casualties": { "avg": { "field": "nkill" } }
                }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    attack_types = response['aggregations']['avg_casualties_by_type']['buckets']
    print("Average Casualties by Attack Type:")
    for attack in attack_types:
        print(f"{attack['key']}: {attack['avg_casualties']['value']} average casualties")

# 16. Top 10 Most Frequent Countries for Terrorist Attacks
def query_top_10_countries_for_attacks():
    query = {
        "size": 0,
        "aggs": {
            "top_countries": {
                "terms": { "field": "country_txt.keyword", "size": 10 }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    countries = response['aggregations']['top_countries']['buckets']
    print("Top 10 Countries for Attacks:")
    for country in countries:
        print(f"{country['key']}: {country['doc_count']} attacks")

# 17. Percentage of Incidents Involving Bombs
def query_bomb_incidents_percentage():
    query = {
        "query": {
            "term": { "weaptype1_txt.keyword": "Bomb" }
        }
    }
    response = es.search(index=index_name, body=query)
    bomb_count = response['hits']['total']['value']
    total_incidents = es.count(index=index_name)['count']
    print(f"Percentage of incidents involving bombs: {(bomb_count / total_incidents) * 100:.2f}%")

# 18. Most Active Terrorist Groups in 2020
def query_active_groups_in_2020():
    query = {
        "size": 0,
        "query": {
            "term": { "iyear": 2020 }
        },
        "aggs": {
            "active_groups": {
                "terms": { "field": "gname.keyword", "size": 5 }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    groups = response['aggregations']['active_groups']['buckets']
    print("Most Active Terrorist Groups in 2020:")
    for group in groups:
        print(f"{group['key']}: {group['doc_count']} attacks")

# 19. Incidents in a Specific Region (e.g., Middle East)
def query_incidents_in_middle_east():
    query = {
        "query": {
            "term": { "region_txt.keyword": "Middle East & North Africa" }
        }
    }
    response = es.search(index=index_name, body=query)
    print(f"Incidents in Middle East & North Africa: {response['hits']['total']['value']} incidents")



# Execute all queries
def run_all_queries():
    query_total_incidents()
    query_top_5_countries()
    query_attack_types()
    query_incidents_by_region()
    query_deadliest_groups()
    query_incidents_with_hostages()
    query_average_casualties()
    query_incidents_in_afghanistan()
    query_suicide_attacks()
    query_weapon_types()
    query_target_types()
    query_incidents_with_fatalities()
    query_avg_casualties_by_attack_type()
    query_top_10_countries_for_attacks()
    query_bomb_incidents_percentage()
    query_active_groups_in_2020()
    query_incidents_in_middle_east()

# Run the queries
run_all_queries()


import pandas as pd
from elasticsearch import Elasticsearch, helpers








username = "elastic"
password = "password"
es = Elasticsearch(
    ["https://localhost:9200"],
    basic_auth=(username, password), verify_certs=False
)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
dataset_path = "globalterrorismdb.csv"

print("Loading the dataset...")
df = pd.read_csv(dataset_path, encoding="latin1")

print("Preprocessing the dataset...")
df = df.fillna("")
df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"), inplace=True)

index_name = "global_terrorism"

records = df.to_dict(orient="records")

if not es.indices.exists(index=index_name):
    print(f"Creating index: {index_name}")
    es.indices.create(index=index_name)

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

index_name = "global_terrorism"
def update_max_result_window(index_name, max_window):
    try:
        response = es.indices.put_settings(
            index=index_name,
            settings={
                "index": {
                    "max_result_window": max_window
                }
            }
        )
        print(f"Updated max_result_window to {max_window} for index '{index_name}': {response}")
    except Exception as e:
        print(f"Error updating max_result_window: {e}")

update_max_result_window(index_name, 50000)
def query_total_incidents():
    try:
        response = es.count(index=index_name)
        total_incidents = response['count']
        print(f"Total incidents: {total_incidents}")
        return total_incidents
    except Exception as e:
        print(f"Error querying total incidents: {e}")
        return None


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

def query_incidents_by_year():
    query = {
        "size": 0,
        "aggs": {
            "incidents_per_year": {
                "date_histogram": {
                    "field": "iyear",
                    "calendar_interval": "year"
                }
            }
        }
    }
    response = es.search(index=index_name, body=query)
    years = response['aggregations']['incidents_per_year']['buckets']
    print("Incidents by Year:")
    for year in years:
        year_key = year.get('key_as_string', year.get('key'))
        print(f"Year: {year_key}, Incidents: {year['doc_count']}")

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

def query_incidents_with_hostages():
    try:
        query = {
            "query": {
                "range": { "nhostkid": { "gt": 0 } }  # Find incidents with hostages
            }
        }
        # Use the count API to get the exact number of matching documents
        response = es.count(index=index_name, body=query)
        total_hostage_incidents = response['count']
        print(f"Incidents involving hostages: {total_hostage_incidents}")
        return total_hostage_incidents
    except Exception as e:
        print(f"Error querying incidents with hostages: {e}")
        return None

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

def query_incidents_in_afghanistan():
    try:
        query = {
            "query": {
                "match": { "country_txt": "Afghanistan" }
            }
        }
        # Use the count API to get the exact number of matching documents
        response = es.count(index=index_name, body=query)
        total_incidents = response['count']
        print(f"Incidents in Afghanistan: {total_incidents}")
        return total_incidents
    except Exception as e:
        print(f"Error querying incidents in Afghanistan: {e}")
        return None




def query_suicide_attacks():
    try:
        query = {
            "query": {
                "term": { "suicide": 1 }
            }
        }
        # Use the count API to get the exact number of matching documents
        response = es.count(index=index_name, body=query)
        total_suicide_attacks = response['count']
        print(f"Suicide attacks: {total_suicide_attacks} incidents")
        return total_suicide_attacks
    except Exception as e:
        print(f"Error querying suicide attacks: {e}")
        return None


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

def query_incidents_with_fatalities():
    try:
        query = {
            "query": {
                "range": { "nkill": { "gt": 0 } }
            }
        }
        response = es.count(index=index_name, body=query)
        total_incidents = response['count']
        print(f"Incidents with fatalities: {total_incidents} incidents")
        return total_incidents
    except Exception as e:
        print(f"Error querying incidents with fatalities: {e}")
        return None


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

def query_incidents_in_middle_east():
    try:
        query = {
            "query": {
                "term": { "region_txt.keyword": "Middle East & North Africa" }
            }
        }
        # Use the count API to get the exact number of matching documents
        response = es.count(index=index_name, body=query)
        total_incidents = response['count']
        print(f"Incidents in Middle East & North Africa: {total_incidents} incidents")
        return total_incidents
    except Exception as e:
        print(f"Error querying incidents in Middle East & North Africa: {e}")
        return None


def query_fatalities_and_wounds_by_attack_type_and_region():
    query = {
        "size": 0,
        "aggs": {
            "regions": {
                "terms": {
                    "field": "region_txt.keyword",
                    "size": 5
                },
                "aggs": {
                    "attack_types": {
                        "terms": {
                            "field": "attacktype1_txt.keyword",
                            "size": 5
                        },
                        "aggs": {
                            "total_fatalities": {
                                "sum": {
                                    "field": "nkill"
                                }
                            },
                            "total_injuries": {
                                "sum": {
                                    "field": "nwound"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    # Execute the query
    response = es.search(index="global_terrorism", body=query)

    # Print the results
    regions = response['aggregations']['regions']['buckets']

    print("Fatalities and Wounds by Attack Type and Region:")

    for region in regions:
        print(f"Region: {region['key']}")
        attack_types = region['attack_types']['buckets']

        for attack in attack_types:
            attack_type = attack['key']
            total_fatalities = attack['total_fatalities']['value']
            total_injuries = attack['total_injuries']['value']

            print(f"  Attack Type: {attack_type}")
            print(f"    Total Fatalities: {total_fatalities}")
            print(f"    Total Injuries: {total_injuries}")
        print()


def query_attack_vs_weapon():
    query = {
        "size": 0,
        "aggs": {
            "attack_types": {
                "terms": {
                    "field": "attacktype1_txt.keyword",
                    "size": 5
                },
                "aggs": {
                    "weapon_types": {
                        "terms": {
                            "field": "weaptype1_txt.keyword",
                            "size": 5
                        },
                        "aggs": {
                            "incident_count": {
                                "value_count": {
                                    "field": "eventid"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    response = es.search(index="global_terrorism", body=query)
    results = response['aggregations']['attack_types']['buckets']

    data = []
    for attack in results:
        attack_type = attack['key']
        for weapon in attack['weapon_types']['buckets']:
            data.append({
                "Attack Type": attack_type,
                "Weapon Type": weapon['key'],
                "Incidents": weapon['incident_count']['value']
            })

    def visualize_attack_vs_weapon(data):
        import matplotlib.pyplot as plt
        import seaborn as sns
        import pandas as pd
        df = pd.DataFrame(data)
        pivot = df.pivot(index="Attack Type", columns="Weapon Type", values="Incidents")

        plt.figure(figsize=(14, 10))  
        heatmap = sns.heatmap(pivot, annot=True, cmap="YlGnBu", fmt="g", linewidths=0.5)

        plt.title("Correlation: Attack Types vs Weapon Types", fontsize=16, pad=20)
        plt.ylabel("Attack Type", fontsize=12)
        plt.xlabel("Weapon Type", fontsize=12)

        plt.xticks(rotation=35, ha="right", fontsize=10)
        plt.yticks(fontsize=10)

        heatmap.figure.subplots_adjust(left=0.2, right=0.9, top=0.9, bottom=0.2)

        plt.savefig("attack_vs_weapon_heatmap.png", dpi=300, bbox_inches="tight")

        plt.show()

    visualize_attack_vs_weapon(data)
    return data


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
    query_active_groups_in_2020()
    query_incidents_in_middle_east()
    query_fatalities_and_wounds_by_attack_type_and_region()
    query_attack_vs_weapon()

# Run the queries
run_all_queries()


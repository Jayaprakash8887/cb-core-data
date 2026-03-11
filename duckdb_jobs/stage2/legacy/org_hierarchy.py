import findspark
findspark.init()
import requests
import pandas as pd
import psycopg2
import time
import datetime
from elasticsearch import Elasticsearch
from elasticsearch.connection import RequestsHttpConnection
import logging
import redis
import json
from pyspark.sql.functions import lit


# Configure logger at the top of your script (add this near your imports)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Elasticsearch settings
es = Elasticsearch(
    ['http://10.175.5.10:9200'],
    headers={"Content-Type": "application/json"},
    connection_class=RequestsHttpConnection
)  # Update with your ES host
index_name = 'org_v4'  # Update with your index

# PostgreSQL settings
pg_conn = psycopg2.connect(
    dbname='sunbird',
    user='postgres',
    password='password123',
    host='10.175.5.15',
    port='5432'
)
pg_cursor = pg_conn.cursor()
#pg_cursor.execute('SET search_path TO wingspan;')
pg_conn.commit()

# Initialize Redis client (adjust host/port/db as needed)
#redis_client = redis.Redis(host='localhost', port=6379, db=12)

# Truncate the target table before processing
try:
    pg_cursor.execute("TRUNCATE TABLE org_hierarchy_new;")
    pg_conn.commit()
    pg_cursor.execute("TRUNCATE TABLE org_hierarchy_lookup;")
    pg_conn.commit()
    pg_cursor.execute("TRUNCATE TABLE mdo_children_lookup;")
    pg_conn.commit()
    print("Tables org_hierarchy_new, org_hierarchy_lookup, and mdo_children_lookup truncated successfully.")
except Exception as e:
    print(f"Failed to truncate table: {e}")
    exit(1)

# API and headers
API_URL_TEMPLATE = 'https://spv.igotkarmayogi.gov.in/api/framework/v1/read/{}'
#API_URL_TEMPLATE = 'https://portal.qa.karmayogibharat.net/api/framework/v1/read/{}'
HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'wid': 'a62c1d64-0102-4d05-a31d-6a2cc0018f01',
    #'Authorization': 'bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJJdHlZeDZnWm5pdFVmRUJ6cm0xQWxsSnQwQ3NZN2w4byJ9.XZN6QhXoB-Ld8nA0kf2p51pxpuzfSA2gFlLZIjfuNj8'
    'Authorization': 'bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiI5a04xTW1TcGVuVTAyam8zVHg1U2p0amhTOFVXeGVSUiJ9.LWAgFust4e0wntxqY8_MQjf5WQ9RSD6Hg45jX_NoCXY'
    #'cookie': 'connect.sid=s%3A7aKSUGmhbP9AGgQ7E9ceY28NER-p77Cu.qWCP%2FzyeLqJFM7v%2BUpxM%2Bw2ng%2B3AJhv82a3ltTrw7bw'
}

# Query for Elasticsearch
query_body = {
    "query": {
        "bool": {
            "must": [
                {"terms": {"sbOrgType": ["state", "ministry"]}},
                {"exists": {"field": "orgHierarchyFrameworkId"}}
            ]
        }
    },
    "_source": [
        "identifier", "orgName", "orgHierarchyFrameworkId",
        "ministryorstatetype", "createdDate", "sbOrgType", "updatedDate"
    ]
}

def fetch_es_data():
    scroll = '2m'
    page_size = 1000
    query_body["size"] = page_size  # move size into body

    results = []
    page = es.search(index=index_name, body=query_body, scroll=scroll)
    sid = page['_scroll_id']
    scroll_size = len(page['hits']['hits'])
    results.extend(page['hits']['hits'])

    while scroll_size > 0:
        page = es.scroll(scroll_id=sid, scroll=scroll)
        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])
        results.extend(page['hits']['hits'])

    df = pd.DataFrame([r['_source'] for r in results])
    #print(f"Fetched {df} ")
    return df

def parse_framework_data(data):
    categories = data.get("result", {}).get("framework", {}).get("categories", [])
    level_names = [
        "LevelOne", "LevelTwo", "LevelThree", "LevelFour", "LevelFive",
        "LevelSix", "LevelSeven", "LevelEight", "LevelNine", "LevelTen"
    ]
    # Build a lookup: {level_code: {identifier: term_dict}}
    level_term_lookup = {}
    for cat in categories:
        code = cat.get("code")
        terms = cat.get("terms", [])
        level_term_lookup[code] = {t["identifier"]: t for t in terms}

    results = []

    def walk(term, level_idx, hierarchy):
        if level_idx >= len(level_names):
            # Fill all missing levels
            for idx in range(len(level_names)):
                level = level_names[idx]
                if level not in hierarchy:
                    hierarchy[level] = (None, None)
            results.append(hierarchy.copy())
            return

        level = level_names[level_idx]
        org_id = term.get("additionalProperties", {}).get("orgId")
        name = term.get("name")
        hierarchy[level] = (org_id, name)

        associations = term.get("associations", [])
        if associations:
            next_level = None
            if level_idx + 1 < len(level_names):
                next_level = level_names[level_idx + 1]
            for assoc in associations:
                # If the association is just a reference, look up the full term in the next level
                assoc_term = assoc
                if next_level and "identifier" in assoc and next_level in level_term_lookup:
                    assoc_term = level_term_lookup[next_level].get(assoc["identifier"], assoc)
                walk(assoc_term, level_idx + 1, hierarchy.copy())
        else:
            # Fill remaining levels with None
            for idx in range(level_idx + 1, len(level_names)):
                hierarchy[level_names[idx]] = (None, None)
            results.append(hierarchy.copy())

    # Start from LevelOne terms
    level_one_terms = []
    for cat in categories:
        if cat.get("code") == "LevelOne":
            level_one_terms = cat.get("terms", [])
            break

    for term in level_one_terms:
        walk(term, 0, {})

    # Ensure all results have all levels
    for h in results:
        for idx in range(len(level_names)):
            level = level_names[idx]
            if level not in h:
                h[level] = (None, None)

    return results

def insert_to_postgres(org_name, org_id, hierarchy, created_date, updated_date):
    columns = ["created_date", "updated_date"]
    values = [created_date, updated_date]

    level_names = [
        "LevelOne", "LevelTwo", "LevelThree", "LevelFour", "LevelFive",
        "LevelSix", "LevelSeven", "LevelEight", "LevelNine", "LevelTen"
    ]

    # Always add columns for L1-L10
    for i, level in enumerate(level_names):
        columns.extend([f"l{i+1}_id", f"l{i+1}_name"])
        if level in hierarchy:
            values.append(hierarchy[level][0])
            values.append(hierarchy[level][1])
        else:
            values.extend([None, None])

    # Add org_name and org_id for traceability
    columns.extend(["center_state_id", "center_state_name"])
    values.extend([org_id, org_name])

    placeholders = ", ".join(["%s"] * len(values))
    sql = f"INSERT INTO org_hierarchy_new ({', '.join(columns)}) VALUES ({placeholders})"
    #print(f"Executing SQL: {sql} with values: {values}")
    #print("-----------------------------------------------------------------------")
    pg_cursor.execute(sql, values)

def parse_pg_timestamp(ts):
    if ts is None or pd.isna(ts):
        return None
    try:
        # Replace the last colon before milliseconds with a dot
        # "2025-06-25 10:30:13:387+0000" -> "2025-06-25 10:30:13.387+0000"
        if ts.count(':') == 3:
            # Split off the timezone
            base, tz = ts.rsplit('+', 1)
            # Replace last colon with dot
            base = base[::-1].replace(':', '.', 1)[::-1]
            ts_fixed = f"{base}+{tz}"
        else:
            ts_fixed = ts
        dt = datetime.datetime.strptime(ts_fixed, "%Y-%m-%d %H:%M:%S.%f%z")
        return dt
    except Exception as e:
        print(f"Could not parse timestamp: {ts} ({e})")
        return None

def build_flat_descendant_map(categories):
    """
    Returns a dict: {parent_org_id: [all_descendant_org_ids]}
    For each org_id, the value is a flat list of all descendant org_ids (all levels below).
    Also saves each descendant list in Redis as descendants:<org_id>
    """
    # Build lookup: {identifier: term_dict} and {org_id: term_dict}
    id_to_term = {}
    orgid_to_term = {}
    for cat in categories:
        for t in cat.get("terms", []):
            id_to_term[t["identifier"]] = t
            org_id = t.get("additionalProperties", {}).get("orgId")
            if org_id:
                orgid_to_term[org_id] = t

    # Recursive function to collect all descendant orgIds for a term
    def collect_all_descendants(term):
        descendants = []
        for assoc in term.get("associations", []):
            assoc_org_id = assoc.get("additionalProperties", {}).get("orgId")
            assoc_identifier = assoc.get("identifier")
            # Try to get the full term for further traversal
            assoc_term = id_to_term.get(assoc_identifier)
            if assoc_org_id:
                descendants.append(assoc_org_id)
            if assoc_term:
                # Recursively collect all descendants of this child
                descendants.extend(collect_all_descendants(assoc_term))
        logger.debug(f"Collected descendants for org_id {term.get('additionalProperties', {}).get('orgId')}: {descendants}")
        return descendants

    flat_map = {}
    for org_id, term in orgid_to_term.items():
        # Use set to avoid duplicates, then convert back to list
        all_descendants = list(dict.fromkeys(collect_all_descendants(term)))  # remove duplicates, preserve order
        flat_map[org_id] = all_descendants
        # Save to Redis as a JSON string
        #redis_client.set(f"{org_id}", json.dumps(all_descendants))
        logger.debug(f"Saved descendants for org_id {org_id} in Redis: { json.dumps(all_descendants)}")

    return flat_map


def insert_mdo_children_lookup(mdo_id, children_ids):
    """
    Insert MDO ID and its unique child IDs (comma-separated) into mdo_children_lookup table.
    
    Args:
        mdo_id: The parent MDO ID (varchar(100))
        children_ids: List of unique child MDO IDs
    """
    if not children_ids:
        # If no children, insert with empty string or NULL
        children_str = None
    else:
        # Convert list to comma-separated string of unique IDs
        children_str = ",".join(str(child_id) for child_id in children_ids if child_id)
    
    try:
        sql = """
            INSERT INTO mdo_children_lookup (mdo_id, children_id)
            VALUES (%s, %s)
            ON CONFLICT (mdo_id) 
            DO UPDATE SET children_id = EXCLUDED.children_id
        """
        pg_cursor.execute(sql, (mdo_id, children_str))
        logger.debug(f"Inserted/Updated mdo_children_lookup for mdo_id {mdo_id} with {len(children_ids)} children")
    except Exception as e:
        logger.error(f"Error inserting into mdo_children_lookup for mdo_id {mdo_id}: {e}")
        raise


def find_and_insert_all_children(org_id, data):
    """
    Find all unique child MDO IDs for each MDO ID in the framework and insert into lookup table.
    This method processes the entire framework hierarchy and creates parent-child mappings.
    
    Args:
        data: The framework data from API response
    """
    categories = data.get("result", {}).get("framework", {}).get("categories", [])
    
    # Build the flat descendant map (this already finds all children)
    flat_map = build_flat_descendant_map(categories)
    print(f"Flat descendant map: {flat_map}")
    
    # Insert each parent-children relationship into the lookup table
    for mdo_id, children_ids in flat_map.items():
        if mdo_id:  # Only insert if mdo_id exists
            print(f"Inserting MDO ID {mdo_id} with children {children_ids}")
            insert_mdo_children_lookup(mdo_id, children_ids)
        
    # 👉 Create an org-wide lookup entry with ALL unique descendant MDOs
    all_children = set(flat_map.keys())  # include all parent IDs
    for children_ids in flat_map.values():  # plus any extra children not in keys
        all_children.update(children_ids)

    all_children = list(all_children)
    print(f"Inserting ORG level mapping for org_id={org_id} with children={all_children}")
    insert_mdo_children_lookup(org_id, all_children)
    
    # Commit all inserts
    pg_conn.commit()
    logger.info(f"Inserted {len(flat_map)} MDO parent-children relationships into mdo_children_lookup")


def process_frameworks(df):
    for _, row in df.iterrows():
        org_name = row['orgName']
        org_id = row['identifier']
        framework_id = row['orgHierarchyFrameworkId']
        created_date = parse_pg_timestamp(row.get('createdDate'))
        updated_date = parse_pg_timestamp(row.get('updatedDate'))

        try:
            url = API_URL_TEMPLATE.format(framework_id)
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"Processing framework {framework_id} |||||||||||--|||||||||| {data}")
                hierarchies = parse_framework_data(data)
                insert_hierarchy_lookup(org_id, org_name, data)

                # --- FLAT DESCENDANT MAP LOGGING ---
                categories = data.get("result", {}).get("framework", {}).get("categories", [])
                flat_map = build_flat_descendant_map(categories)
                logger.debug("--------------------------------------------------------------------")
                logger.debug(f"Flat descendant map for framework {framework_id}: {flat_map}")
                logger.debug("--------------------------------------------------------------------")

                # --- NEW: Find and insert all children into lookup table ---
                find_and_insert_all_children(org_id, data)

                # --- Write to Redis here if needed ---
                # redis_client.set(f"flatmap:{framework_id}", json.dumps(flat_map))

                for hierarchy in hierarchies:
                    #logger.debug(f"{hierarchy} |||||| (org_name -- {org_name} org_id ----{org_id})")
                    insert_to_postgres(org_name, org_id, hierarchy, created_date, updated_date)
                    pg_conn.commit()
            else:
                logger.debug(f"Failed to fetch framework for ID {framework_id}: {response.status_code}")
        except Exception as e:
            logger.debug(f"Error processing framework {framework_id}: {e}")
            pg_conn.rollback()
        time.sleep(1)  # Avoid hitting API rate limits


def insert_hierarchy_lookup(org_id, org_name, data):
    categories = data.get("result", {}).get("framework", {}).get("categories", [])
    level_order = [
        "LevelOne", "LevelTwo", "LevelThree", "LevelFour", "LevelFive",
        "LevelSix", "LevelSeven", "LevelEight", "LevelNine", "LevelTen"
    ]
    # Build a lookup: {level_code: {identifier: term_dict}}
    level_term_lookup = {}
    for cat in categories:
        code = cat.get("code")
        terms = cat.get("terms", [])
        level_term_lookup[code] = {t["identifier"]: t for t in terms}

    # For each level, for each term, insert a record with parent info
    for idx, level in enumerate(level_order):
        if level not in level_term_lookup:
            continue
        for term_id, term in level_term_lookup[level].items():
            mdo_id = term.get("additionalProperties", {}).get("orgId")
            mdo_name = term.get("name")
            mdo_level = level

            # Find parent (department) info by reverse lookup from associations in previous level
            department_id = department_name = department_level = None
            if idx > 0:
                prev_level = level_order[idx - 1]
                # Search all terms in previous level for associations pointing to this term
                for prev_term in level_term_lookup.get(prev_level, {}).values():
                    for assoc in prev_term.get("associations", []):
                        if assoc.get("identifier") == term_id:
                            department_id = prev_term.get("additionalProperties", {}).get("orgId")
                            department_name = prev_term.get("name")
                            department_level = prev_level
                            break
                    if department_id:
                        break

            sql = """
                INSERT INTO org_hierarchy_lookup (
                    mdo_name, mdo_id, mdo_level,
                    department_id, department_name, department_level,
                    center_state_id, center_state_name
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = [
                mdo_name, mdo_id, mdo_level,
                department_id, department_name, department_level,
                org_id, org_name
            ]
            pg_cursor.execute(sql, values)
            pg_conn.commit()


if __name__ == "__main__":
    df = fetch_es_data()
    process_frameworks(df)
    pg_cursor.close()
    pg_conn.close()
    logger.debug("All data processed and saved.")

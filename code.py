DATA_DIR = "D:/code/"
EXPORT_DIR = "D:/code/nosql_exports/"

import pandas as pd
from pymongo import MongoClient
import time
import os
import json

# --- Configuration & Paths ---
DATA_DIR = "D:/code/"
EXPORT_DIR = "D:/code/nosql_exports/"
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "tpch_migration_comparison"

# Ensure the export directory exists
os.makedirs(EXPORT_DIR, exist_ok=True)

# Establish connection to the local MongoDB instance
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Clean up existing collections for a fresh run
for coll in db.list_collection_names():
    db.drop_collection(coll)

# Helper function to export data to JSON (Pretty-printed)
def export_to_json(data_list, filename):
    filepath = os.path.join(EXPORT_DIR, filename)
    with open(filepath, 'w') as f:
        # default=str handles any numpy datatypes Pandas might leave behind
        # indent=4 creates a nicely formatted, multi-line JSON file
        json.dump(data_list, f, default=str, indent=4)
    print(f"  -> Saved {filename}")

# --- 1. Define Column Schemas & Load Data ---
region_cols = ['r_regionkey', 'r_name', 'r_comment']
nation_cols = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
supplier_cols = ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment']
part_cols = ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment']
partsupp_cols = ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']
customer_cols = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']
orders_cols = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']
lineitem_cols = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

print("Loading TPC-H .tbl files into Pandas DataFrames...")
region_df = pd.read_csv(f'{DATA_DIR}region.tbl', sep='|', names=region_cols, index_col=False)
nation_df = pd.read_csv(f'{DATA_DIR}nation.tbl', sep='|', names=nation_cols, index_col=False)
supplier_df = pd.read_csv(f'{DATA_DIR}supplier.tbl', sep='|', names=supplier_cols, index_col=False)
part_df = pd.read_csv(f'{DATA_DIR}part.tbl', sep='|', names=part_cols, index_col=False)
partsupp_df = pd.read_csv(f'{DATA_DIR}partsupp.tbl', sep='|', names=partsupp_cols, index_col=False)
customer_df = pd.read_csv(f'{DATA_DIR}customer.tbl', sep='|', names=customer_cols, index_col=False)
orders_df = pd.read_csv(f'{DATA_DIR}orders.tbl', sep='|', names=orders_cols, index_col=False)
lineitem_df = pd.read_csv(f'{DATA_DIR}lineitem.tbl', sep='|', names=lineitem_cols, index_col=False)

# =====================================================================
# METHOD 1: STRICT NORMALIZATION
# =====================================================================
print("\n--- Executing Strict Normalization Migration ---")
norm_data = {
    'norm_region': region_df.to_dict('records'),
    'norm_nation': nation_df.to_dict('records'),
    'norm_supplier': supplier_df.to_dict('records'),
    'norm_part': part_df.to_dict('records'),
    'norm_partsupp': partsupp_df.to_dict('records'),
    'norm_customer': customer_df.to_dict('records'),
    'norm_orders': orders_df.to_dict('records'),
    'norm_lineitem': lineitem_df.to_dict('records')
}

for coll_name, data in norm_data.items():
    db[coll_name].insert_many(data)
    export_to_json(data, f"{coll_name}.json")

# Create indexes to give Normalization a fair chance in the benchmark
db.norm_customer.create_index("c_custkey")
db.norm_nation.create_index("n_nationkey")
db.norm_region.create_index("r_regionkey")
db.norm_lineitem.create_index("l_orderkey")
db.norm_part.create_index("p_partkey")

# =====================================================================
# METHOD 2: TABLE-LEVEL DENORMALIZATION (TLD / BFS)
# =====================================================================
print("\n--- Executing Table-Level Denormalization (TLD) Migration ---")

# Pre-merge smaller tables first (they fit in RAM)
orders_cust = pd.merge(orders_df, customer_df, left_on='o_custkey', right_on='c_custkey')
orders_cust_nat = pd.merge(orders_cust, nation_df, left_on='c_nationkey', right_on='n_nationkey')
orders_cust_nat_reg = pd.merge(orders_cust_nat, region_df, left_on='n_regionkey', right_on='r_regionkey')

# Process lineitem in chunks to avoid MemoryError
CHUNK_SIZE = 50000
tld_json_path = os.path.join(EXPORT_DIR, "tld_flat_collection.json")
first_chunk = True

with open(tld_json_path, 'w') as f:
    f.write('[')
    first_record = True
    for i in range(0, len(lineitem_df), CHUNK_SIZE):
        chunk = lineitem_df.iloc[i:i+CHUNK_SIZE]
        tld_chunk = pd.merge(chunk, orders_cust_nat_reg, left_on='l_orderkey', right_on='o_orderkey')
        tld_chunk = pd.merge(tld_chunk, part_df, left_on='l_partkey', right_on='p_partkey')
        records = tld_chunk.to_dict('records')
        if records:
            db.tld_flat_collection.insert_many(records)
            # Write one record at a time to avoid memory spike
            for rec in records:
                if not first_record:
                    f.write(',\n')
                json.dump(rec, f, default=str, indent=4)
                first_record = False
        print(f"  -> TLD chunk {i//CHUNK_SIZE + 1} done ({len(records)} records)")
        del chunk, tld_chunk, records
    f.write(']')

print("  -> Saved tld_flat_collection.json")
db.tld_flat_collection.create_index([("r_name", 1), ("p_type", 1)])

# Indexing the flat collection for the benchmark query
db.tld_flat_collection.create_index([("r_name", 1), ("p_type", 1)])


# =====================================================================
# METHOD 3: COLUMN-LEVEL DENORMALIZATION WITH ATOMICITY (CLDA)
# =====================================================================
print("\n--- Executing CLDA Migration ---")

# Traverse Solid Edges
c_n_df = pd.merge(customer_df, nation_df, left_on='c_nationkey', right_on='n_nationkey')
c_n_r_df = pd.merge(c_n_df, region_df, left_on='n_regionkey', right_on='r_regionkey')

orders_denorm_df = pd.merge(orders_df, c_n_r_df[['c_custkey', 'r_name']], left_on='o_custkey', right_on='c_custkey')
orders_denorm_df.rename(columns={'r_name': 'o_custkey_c_nationkey_n_regionkey_r_name'}, inplace=True)
orders_denorm_df.drop(columns=['c_custkey'], inplace=True)

lineitem_denorm_df = pd.merge(lineitem_df, part_df[['p_partkey', 'p_type']], left_on='l_partkey', right_on='p_partkey')
lineitem_denorm_df.rename(columns={'p_type': 'l_partkey_p_type'}, inplace=True)
lineitem_denorm_df.drop(columns=['p_partkey'], inplace=True)

# Atomic Aggregates
lineitem_dict = {}
for record in lineitem_denorm_df.to_dict('records'):
    order_key = record['l_orderkey']
    del record['l_orderkey']
    if order_key not in lineitem_dict:
        lineitem_dict[order_key] = []
    lineitem_dict[order_key].append(record)

atomic_orders = []
for order in orders_denorm_df.to_dict('records'):
    o_key = order['o_orderkey']
    order['_id'] = o_key
    order['lineitem'] = lineitem_dict.get(o_key, [])
    atomic_orders.append(order)

db.clda_orders.insert_many(atomic_orders)
export_to_json(atomic_orders, "clda_orders.json")

clda_dims = {
    'clda_region': region_df.to_dict('records'),
    'clda_nation': nation_df.to_dict('records'),
    'clda_supplier': supplier_df.to_dict('records'),
    'clda_part': part_df.to_dict('records'),
    'clda_partsupp': partsupp_df.to_dict('records'),
    'clda_customer': customer_df.to_dict('records')
}

for coll_name, data in clda_dims.items():
    db[coll_name].insert_many(data)
    export_to_json(data, f"{coll_name}.json")

db.clda_orders.create_index("o_custkey")
db.clda_orders.create_index("o_custkey_c_nationkey_n_regionkey_r_name")
db.clda_orders.create_index("lineitem.l_suppkey")
db.clda_orders.create_index("lineitem.l_partkey_p_type")


# =====================================================================
# EXTENSIVE PERFORMANCE MATRIX TEST
# =====================================================================
print("\n" + "="*50)
print("     EXTENSIVE PERFORMANCE MATRIX TEST     ")
print("="*50)

# --- 1. STORAGE ANALYSIS ---
def get_db_storage(collection_names):
    total_size = 0
    for coll in collection_names:
        stats = db.command("collstats", coll)
        total_size += stats.get("storageSize", 0)
    return total_size / (1024 * 1024) # Return in MB

norm_colls = list(norm_data.keys())
clda_colls = ['clda_orders'] + list(clda_dims.keys())

norm_storage = get_db_storage(norm_colls)
tld_storage = get_db_storage(['tld_flat_collection'])
clda_storage = get_db_storage(clda_colls)

print("\n--- Storage Footprint ---")
print(f"1. Strict Normalization: {norm_storage:.2f} MB (Baseline)")
print(f"2. CLDA Methodology:     {clda_storage:.2f} MB ({clda_storage/norm_storage:.2f}x Baseline)")
print(f"3. Table-Level (TLD):    {tld_storage:.2f} MB ({tld_storage/norm_storage:.2f}x Baseline)")


# --- 2. QUERY EXECUTION SPEED ANALYSIS ---
# Simulating a complex analytical query: "Find all lineitems in AMERICA for part type ECONOMY ANODIZED STEEL"

def run_benchmark(collection, pipeline, iterations=3):
    total_time = 0
    for i in range(iterations):
        start = time.time()
        # list() forces the cursor to fetch all results, triggering full execution
        list(collection.aggregate(pipeline))
        total_time += (time.time() - start)
    return total_time / iterations

# Pipeline 1: Strict Normalization (Heavy application-layer emulation via consecutive $lookups)
norm_pipeline = [
    {"$lookup": {"from": "norm_customer", "localField": "o_custkey", "foreignField": "c_custkey", "as": "customer"}},
    {"$unwind": "$customer"},
    {"$lookup": {"from": "norm_nation", "localField": "customer.c_nationkey", "foreignField": "n_nationkey", "as": "nation"}},
    {"$unwind": "$nation"},
    {"$lookup": {"from": "norm_region", "localField": "nation.n_regionkey", "foreignField": "r_regionkey", "as": "region"}},
    {"$unwind": "$region"},
    {"$match": {"region.r_name": "AMERICA"}},
    {"$lookup": {"from": "norm_lineitem", "localField": "o_orderkey", "foreignField": "l_orderkey", "as": "lineitems"}},
    {"$unwind": "$lineitems"},
    {"$lookup": {"from": "norm_part", "localField": "lineitems.l_partkey", "foreignField": "p_partkey", "as": "part"}},
    {"$unwind": "$part"},
    {"$match": {"part.p_type": "ECONOMY ANODIZED STEEL"}}
]

# Pipeline 2: Table-Level Denormalization (Flat scan)
tld_pipeline = [
    {"$match": {"r_name": "AMERICA", "p_type": "ECONOMY ANODIZED STEEL"}}
]

# Pipeline 3: CLDA (Targeted match, unwind, targeted match)
clda_pipeline = [
    {"$match": {"o_custkey_c_nationkey_n_regionkey_r_name": "AMERICA"}},
    {"$unwind": "$lineitem"},
    {"$match": {"lineitem.l_partkey_p_type": "ECONOMY ANODIZED STEEL"}}
]

print("\n--- Average Query Execution Speed (3 Iterations) ---")
print("Executing CLDA Query...")
clda_time = run_benchmark(db.clda_orders, clda_pipeline)

print("Executing TLD Query...")
tld_time = run_benchmark(db.tld_flat_collection, tld_pipeline)

print("Executing Normalization Query (This may take significantly longer due to $lookups)...")
norm_time = run_benchmark(db.norm_orders, norm_pipeline)

print("\n--- Benchmark Results ---")
print(f"1. CLDA Methodology:     {clda_time:.4f} seconds")
print(f"2. Table-Level (TLD):    {tld_time:.4f} seconds")
print(f"3. Strict Normalization: {norm_time:.4f} seconds")
print("==================================================")

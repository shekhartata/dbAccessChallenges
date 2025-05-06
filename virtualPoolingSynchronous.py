from pymongo import MongoClient
import time
import random

# ----- CONFIG -----
MONGO_URI = "mongodb+srv://username:password@cluster1.xxxxx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster1"
DB_NAME = 'graph_entity_search'
COLLECTION_NAME = 'original_products'
NUM_USERS = 300

# ----- Virtual Pool Classes -----
class VirtualMongoPool:
    def __init__(self, uri, pool_id):
        self.id = pool_id
        self.client = MongoClient(uri, maxPoolSize=250, minPoolSize=5)
        self.db = self.client[DB_NAME]
        self.active = 0
        self.latency = 100  # starting latency in ms

    def score(self):
        return self.active * self.latency

    def record_latency(self, duration):
        self.latency = (self.latency * 4 + duration) / 5

# ----- Virtual Pool Test -----
def run_virtual_pool_test():
    pools = [VirtualMongoPool(MONGO_URI, f"pool-{i}") for i in range(4)]
    latencies = []
    start_time = time.time()

    def select_best_pool():
        return min(pools, key=lambda p: p.score())

    for user_id in range(1, NUM_USERS + 1):
        pool = select_best_pool()
        pool.active += 1
        t0 = time.time()
        try:
            pool.db[COLLECTION_NAME].find_one()
            # Simulate variable work time
            if user_id % 5 == 0:
                time.sleep(random.uniform(1.0, 2.0))  # Slow request
            else:
                time.sleep(random.uniform(0.1, 0.5))  # Normal request
        finally:
            duration = (time.time() - t0) * 1000
            pool.record_latency(duration)
            pool.active -= 1
            latencies.append(duration)

    total_time = time.time() - start_time
    avg_latency = sum(latencies) / len(latencies)
    throughput = NUM_USERS / total_time
    return avg_latency, throughput

# ----- Single Pool Test -----
def run_single_pool_test():
    client = MongoClient(MONGO_URI, maxPoolSize=1000, minPoolSize=5)
    db = client[DB_NAME]
    latencies = []
    start_time = time.time()

    for user_id in range(1, NUM_USERS + 1):
        t0 = time.time()
        try:
            db[COLLECTION_NAME].find_one()
            # Simulate variable work time
            if user_id % 10 == 0:
                time.sleep(random.uniform(1.0, 2.0))  # Slow request
            else:
                time.sleep(random.uniform(0.1, 0.5))  # Normal request
        finally:
            duration = (time.time() - t0) * 1000
            latencies.append(duration)

    total_time = time.time() - start_time
    avg_latency = sum(latencies) / len(latencies)
    throughput = NUM_USERS / total_time
    return avg_latency, throughput

# ----- Main Comparison Runner -----
if __name__ == "__main__":
    print("Running Virtual Pool Test (Synchronous)...")
    v_avg_latency, v_throughput = run_virtual_pool_test()
    print(f"Virtual Pool: Avg Latency = {v_avg_latency:.2f} ms, Throughput = {v_throughput:.2f} req/sec\n")

    print("Running Single Pool Test (Synchronous)...")
    s_avg_latency, s_throughput = run_single_pool_test()
    print(f"Single Pool: Avg Latency = {s_avg_latency:.2f} ms, Throughput = {s_throughput:.2f} req/sec\n")

    print("--- Comparison Summary ---")
    print(f"Virtual Pool: Avg Latency = {v_avg_latency:.2f} ms, Throughput = {v_throughput:.2f} req/sec")
    print(f"Single Pool:  Avg Latency = {s_avg_latency:.2f} ms, Throughput = {s_throughput:.2f} req/sec") 

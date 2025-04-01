from pymongo import MongoClient
import time
import random
from datetime import datetime
import hashlib
from typing import Dict, List
import statistics
from pymongo.errors import BulkWriteError

# MongoDB connection
client = MongoClient('mongodb+srv://shekharadmin123:shekhartestmongo@cluster1.eyv8o.mongodb.net/?retryWrites=true&w=majority&appName=Cluster1')
db = client['lookup_comparison_db']

# Collections
orders_collection = db['orders']
customers_collection = db['customers']
lookup_cache = db['lookup_cache']  # For storing hash table results

def add_customer(customer_data: Dict) -> str:
    """
    Add a new customer and update the hash table with versioning.
    Returns the customer_id of the new customer.
    """
    try:
        # Start a session for transaction
        with client.start_session() as session:
            with session.start_transaction():
                # Add version to customer data
                customer_data['version'] = 1
                customer_data['created_at'] = datetime.now()
                
                # Insert customer
                result = customers_collection.insert_one(customer_data, session=session)
                
                # Create hash and update lookup cache
                customer_hash = hashlib.md5(customer_data['customer_id'].encode()).hexdigest()
                cache_entry = {
                    'hash': customer_hash,
                    'customer_data': {
                        'name': customer_data['name'],
                        'city': customer_data['city']
                    },
                    'version': 1
                }
                lookup_cache.insert_one(cache_entry, session=session)
                
                return customer_data['customer_id']
    except Exception as e:
        print(f"Error adding customer: {e}")
        raise

def update_customer(customer_id: str, update_data: Dict) -> bool:
    """
    Update a customer and sync the hash table with versioning.
    Returns True if successful, False otherwise.
    """
    try:
        with client.start_session() as session:
            with session.start_transaction():
                # Get current customer data
                customer = customers_collection.find_one(
                    {'customer_id': customer_id},
                    session=session
                )
                if not customer:
                    return False
                
                current_version = customer.get('version', 1)
                
                # Check if relevant fields changed
                fields_changed = False
                changed_fields = []
                for field in ['name', 'city']:  # Fields we cache in hash table
                    if field in update_data and update_data[field] != customer.get(field):
                        fields_changed = True
                        changed_fields.append(field)
                
                # Update customer
                update_result = customers_collection.update_one(
                    {'customer_id': customer_id},
                    {
                        '$set': {
                            **update_data,
                            'version': current_version + 1 if fields_changed else current_version,
                            'last_updated': datetime.now()
                        }
                    },
                    session=session
                )
                
                if update_result.modified_count > 0 and fields_changed:
                    print(f"\nHash Update Triggered for customer {customer_id}")
                    print(f"Changed fields: {changed_fields}")
                    
                    # Update lookup cache with new version
                    customer_hash = hashlib.md5(customer_id.encode()).hexdigest()
                    lookup_cache.update_one(
                        {'hash': customer_hash},
                        {
                            '$set': {
                                'customer_data': {
                                    'name': update_data.get('name', customer.get('name', '')),
                                    'city': update_data.get('city', customer.get('city', ''))
                                },
                                'version': current_version + 1
                            }
                        },
                        session=session
                    )
                return True
    except Exception as e:
        print(f"Error updating customer: {e}")
        raise

def delete_customer(customer_id: str) -> bool:
    """
    Delete a customer and remove from hash table.
    Returns True if successful, False otherwise.
    """
    try:
        with client.start_session() as session:
            with session.start_transaction():
                # Delete customer
                result = customers_collection.delete_one(
                    {'customer_id': customer_id},
                    session=session
                )
                
                if result.deleted_count > 0:
                    # Remove from lookup cache
                    customer_hash = hashlib.md5(customer_id.encode()).hexdigest()
                    lookup_cache.delete_one(
                        {'hash': customer_hash},
                        session=session
                    )
                    return True
                return False
    except Exception as e:
        print(f"Error deleting customer: {e}")
        raise

def add_order(order_data: Dict) -> str:
    """
    Add a new order with pre-computed hash.
    Returns the order_id of the new order.
    """
    try:
        with client.start_session() as session:
            with session.start_transaction():
                # Verify customer exists and get version
                customer = customers_collection.find_one(
                    {'customer_id': order_data['customer_id']},
                    session=session
                )
                if not customer:
                    raise ValueError(f"Customer {order_data['customer_id']} not found")
                
                # Add hash to order data
                order_data['customer_hash'] = hashlib.md5(order_data['customer_id'].encode()).hexdigest()
                order_data['created_at'] = datetime.now()
                
                # Insert order
                result = orders_collection.insert_one(order_data, session=session)
                return str(result.inserted_id)
    except Exception as e:
        print(f"Error adding order: {e}")
        raise

def update_order(order_id: str, update_data: Dict) -> bool:
    """
    Update an order and sync the hash if customer_id changes.
    Returns True if successful, False otherwise.
    """
    try:
        with client.start_session() as session:
            with session.start_transaction():
                # If customer_id is being updated
                if 'customer_id' in update_data:
                    # Verify new customer exists
                    customer = customers_collection.find_one(
                        {'customer_id': update_data['customer_id']},
                        session=session
                    )
                    if not customer:
                        raise ValueError(f"Customer {update_data['customer_id']} not found")
                    
                    # Update hash
                    update_data['customer_hash'] = hashlib.md5(update_data['customer_id'].encode()).hexdigest()
                
                # Update order
                result = orders_collection.update_one(
                    {'order_id': order_id},
                    {
                        '$set': {
                            **update_data,
                            'last_updated': datetime.now()
                        }
                    },
                    session=session
                )
                return result.modified_count > 0
    except Exception as e:
        print(f"Error updating order: {e}")
        raise

def delete_order(order_id: str) -> bool:
    """
    Delete an order.
    Returns True if successful, False otherwise.
    """
    try:
        with client.start_session() as session:
            with session.start_transaction():
                result = orders_collection.delete_one(
                    {'order_id': order_id},
                    session=session
                )
                return result.deleted_count > 0
    except Exception as e:
        print(f"Error deleting order: {e}")
        raise

# Example usage of write operations
def test_write_operations():
    """Test the write operations with example data."""
    try:
        # Add a new customer
        new_customer = {
            'customer_id': 'CUST1001',
            'name': 'John Doe',
            'email': 'john@example.com',
            'city': 'New York',
            'created_at': datetime.now()
        }
        customer_id = add_customer(new_customer)
        print(f"Added customer: {customer_id}")
        
        # Add an order for the new customer
        new_order = {
            'order_id': 'ORD10001',
            'customer_id': customer_id,
            'amount': 150.50,
            'status': 'pending',
            'created_at': datetime.now()
        }
        order_id = add_order(new_order)
        print(f"Added order: {order_id}")
        
        # Update customer (name change - should trigger hash update)
        update_data = {'name': 'John Smith'}
        if update_customer(customer_id, update_data):
            print(f"Updated customer name: {customer_id}")
        
        # Update customer (city change - should trigger hash update)
        update_data = {'city': 'Los Angeles'}
        if update_customer(customer_id, update_data):
            print(f"Updated customer city: {customer_id}")
        
        # Update customer (email change - should NOT trigger hash update)
        update_data = {'email': 'john.smith@example.com'}
        if update_customer(customer_id, update_data):
            print(f"Updated customer email: {customer_id}")
        
        # Update order
        order_update = {'status': 'completed'}
        if update_order(order_id, order_update):
            print(f"Updated order: {order_id}")
        
        # Delete order
        if delete_order(order_id):
            print(f"Deleted order: {order_id}")
        
        # Delete customer
        if delete_customer(customer_id):
            print(f"Deleted customer: {customer_id}")
            
    except Exception as e:
        print(f"Error in test operations: {e}")
        raise  # Re-raise the exception to see the full traceback

def generate_sample_data(num_customers: int = 1000, num_orders: int = 10000):
    """Generate sample data for customers and orders."""
    # Clear existing collections
    orders_collection.delete_many({})
    customers_collection.delete_many({})
    lookup_cache.delete_many({})
    
    # Generate customers
    customers = []
    for i in range(num_customers):
        customer = {
            'customer_id': f'CUST{i:04d}',
            'name': f'Customer {i}',
            'email': f'customer{i}@example.com',
            'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
            'created_at': datetime.now(),
            'version': 1  # Initial version
        }
        customers.append(customer)
    
    # Insert customers
    customers_collection.insert_many(customers)
    
    # Generate orders
    orders = []
    for i in range(num_orders):
        customer = random.choice(customers)
        order = {
            'order_id': f'ORD{i:06d}',
            'customer_id': customer['customer_id'],
            'amount': round(random.uniform(10, 1000), 2),
            'status': random.choice(['pending', 'completed', 'cancelled']),
            'created_at': datetime.now()
        }
        orders.append(order)
    
    # Insert orders
    orders_collection.insert_many(orders)

def build_hash_table():
    """Build hash table for customer data with versioning."""
    # Clear existing cache
    lookup_cache.delete_many({})
    
    # Get all customers and create hash entries
    customers = list(customers_collection.find({}, {'customer_id': 1, 'name': 1, 'city': 1, 'version': 1}))
    cache_entries = []
    
    for customer in customers:
        customer_hash = hashlib.md5(customer['customer_id'].encode()).hexdigest()
        cache_entries.append({
            'hash': customer_hash,
            'customer_data': {
                'name': customer['name'],
                'city': customer['city']
            },
            'version': customer['version']
        })
    
    # Bulk insert cache entries
    if cache_entries:
        lookup_cache.insert_many(cache_entries)
    
    # Create index on hash field for faster lookups
    lookup_cache.create_index('hash')

def traditional_lookup(limit: int = 100) -> List[Dict]:
    """Perform traditional $lookup operation."""
    pipeline = [
        {'$limit': limit},
        {
            '$lookup': {
                'from': 'customers',
                'localField': 'customer_id',
                'foreignField': 'customer_id',
                'as': 'customer_info'
            }
        },
        {'$unwind': '$customer_info'}
    ]
    return list(orders_collection.aggregate(pipeline))

def hash_table_lookup(limit: int = 100) -> List[Dict]:
    """Perform lookup using hash table approach via aggregation pipeline."""
    pipeline = [
        {'$limit': limit},
        # Lookup from cache collection using pre-computed hash
        {
            '$lookup': {
                'from': 'lookup_cache',
                'localField': 'customer_hash',
                'foreignField': 'hash',
                'as': 'customer_info'
            }
        },
        {'$unwind': '$customer_info'},
        # Project only needed fields
        {
            '$project': {
                'order_id': 1,
                'amount': 1,
                'customer_info': 1,
                '_id': 0
            }
        }
    ]
    return list(orders_collection.aggregate(pipeline))

def measure_performance(func, *args, num_runs: int = 5) -> List[float]:
    """Measure execution time of a function."""
    times = []
    for _ in range(num_runs):
        start_time = time.time()
        func(*args)
        end_time = time.time()
        times.append(end_time - start_time)
    return times

def main():
    # Generate sample data
    print("Generating sample data...")
    generate_sample_data()
    
    # Build hash table
    print("Building hash table...")
    build_hash_table()
    
    # Test write operations
    print("\nTesting write operations...")
    test_write_operations()
    
    # Test different batch sizes
    batch_sizes = [10, 100, 1000]
    
    for batch_size in batch_sizes:
        print(f"\nTesting with batch size: {batch_size}")
        
        # Measure traditional lookup
        traditional_times = measure_performance(traditional_lookup, batch_size)
        print(f"Traditional $lookup - Average time: {statistics.mean(traditional_times):.4f}s")
        
        # Measure hash table lookup
        hash_table_times = measure_performance(hash_table_lookup, batch_size)
        print(f"Hash table lookup - Average time: {statistics.mean(hash_table_times):.4f}s")
        
        # Compare results
        improvement = ((statistics.mean(traditional_times) - statistics.mean(hash_table_times)) 
                      / statistics.mean(traditional_times)) * 100
        print(f"Performance improvement: {improvement:.2f}%")

if __name__ == "__main__":
    main() 
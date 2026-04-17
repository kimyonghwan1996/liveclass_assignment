import os
import time
import random
import json
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

DB_HOST = os.environ.get("DB_HOST", "db")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_USER = os.environ.get("DB_USER", "user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_NAME = os.environ.get("DB_NAME", "event_logs")

NUM_EVENTS = 1000
EVENT_TYPES = ["page_view", "purchase", "error", "signup"]
OUTPUT_DIR = "/app/output"

def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    return conn

def init_db():
    max_retries = 30
    conn = None
    for i in range(max_retries):
        try:
            conn = get_db_connection()
            print("Successfully connected to the database!")
            break
        except psycopg2.OperationalError:
            print(f"Waiting for database to be ready... ({i + 1}/{max_retries})")
            time.sleep(2)
    
    if not conn:
        raise Exception("Database connection failed")

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_events (
                id SERIAL PRIMARY KEY,
                user_id INT NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                event_data JSONB,
                created_at TIMESTAMP NOT NULL
            );
        """)
        cur.execute("TRUNCATE TABLE user_events;")
    conn.commit()
    conn.close()
    print("Database initialized.")

def generate_events():
    events = []
    now = datetime.now()
    
    for _ in range(NUM_EVENTS):
        user_id = random.randint(1, 100)
        event_type = random.choice(EVENT_TYPES)
        
        days_ago = random.uniform(0, 7)
        created_at = now - timedelta(days=days_ago)
        
        event_data = {}
        if event_type == "purchase":
            event_data["amount"] = round(random.uniform(10.0, 500.0), 2)
            event_data["item_id"] = random.randint(1000, 9999)
        elif event_type == "page_view":
            event_data["page"] = random.choice(["/home", "/product", "/checkout", "/about"])
        elif event_type == "error":
            event_data["error_code"] = random.choice([404, 500, 403, 502])
            event_data["message"] = "An error occurred"
            
        events.append((user_id, event_type, json.dumps(event_data), created_at.isoformat()))
        
    print(f"Generated {NUM_EVENTS} mock events.")
    return events

def store_events(events):
    conn = get_db_connection()
    query = """
        INSERT INTO user_events (user_id, event_type, event_data, created_at)
        VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, query, events)
    conn.commit()
    conn.close()
    print("Events stored in the database.")

def analyze_and_visualize_data():
    conn = get_db_connection()
    
    query1 = """
        SELECT event_type, COUNT(*) as event_count
        FROM user_events
        GROUP BY event_type
        ORDER BY event_count DESC;
    """
    df_counts = pd.read_sql(query1, conn)
    print("\n--- Event Counts by Type ---")
    print(df_counts)

    query2 = """
        SELECT 
            DATE(created_at) as event_date,
            COUNT(*) as total_events,
            SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) as error_events
        FROM user_events
        GROUP BY DATE(created_at)
        ORDER BY event_date ASC;
    """
    df_trend = pd.read_sql(query2, conn)
    
    # Check if we have total_events > 0 to avoid division by zero
    if not df_trend.empty:
        df_trend['error_rate'] = (df_trend['error_events'] / df_trend['total_events']) * 100
        print("\n--- Error Rate by Day ---")
        print(df_trend)
    else:
        print("\n--- No data for Error Rate by Day ---")

    conn.close()
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    
    sns.barplot(data=df_counts, x='event_type', y='event_count', ax=axes[0], palette="viridis")
    axes[0].set_title("Total Events by Type", fontsize=14)
    axes[0].set_xlabel("Event Type")
    axes[0].set_ylabel("Count")
    
    if not df_trend.empty:
        sns.lineplot(data=df_trend, x='event_date', y='error_rate', ax=axes[1], marker='o', color='red')
        axes[1].set_title("Error Rate Trend (%) Over Time", fontsize=14)
        axes[1].set_xlabel("Date")
        axes[1].set_ylabel("Error Rate (%)")
    
    plt.tight_layout()
    output_path = os.path.join(OUTPUT_DIR, "dashboard.png")
    plt.savefig(output_path)
    print(f"Visualization saved to: {output_path}")

if __name__ == "__main__":
    print("Starting Event Pipeline...")
    init_db()
    
    while True:
        try:
            events = generate_events()
            store_events(events)
            analyze_and_visualize_data()
            print("Completed a pipeline batch. Waiting for the next batch (10 minutes)...")
            time.sleep(600) # Wait 10 mins before running again, or could be any interval
        except Exception as e:
            print(f"Error in pipeline cycle: {e}")
            time.sleep(60) # Wait 1 min on error

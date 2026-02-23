import json
import time
import random
import boto3
import pandas as pd

REGION = "ap-south-1"
STREAM = "STREAMNAME"

ORDERS = "/kaggle/olist_data/olist_orders_dataset.csv"
PAYMENTS = "/kaggle/olist_data/olist_order_payments_dataset.csv"
ITEMS = "/kaggle/olist_data/olist_order_items_dataset.csv"

MAX_EVENTS = 50000
SLEEP = 0.02

def read_csv(path):
    return pd.read_csv(path).fillna("")

def send(kinesis, event):
    kinesis.put_record(
        StreamName=STREAM,
        Data=json.dumps(event),
        PartitionKey=event.get("order_id", "unknown") or "unknown"
    )

def order_event(r):
    return {
        "event_type": "ORDER_SNAPSHOT",
        "event_time": str(r.get("order_purchase_timestamp", "")),
        "order_id": str(r.get("order_id", "")),
        "customer_id": str(r.get("customer_id", "")),
        "order_status": str(r.get("order_status", "")),
        "data": r
    }

def payment_event(r):
    return {
        "event_type": "PAYMENT_RECEIVED",
        "event_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "order_id": str(r.get("order_id", "")),
        "data": r
    }

def item_event(r):
    return {
        "event_type": "ITEM_ADDED",
        "event_time": str(r.get("shipping_limit_date", "")),
        "order_id": str(r.get("order_id", "")),
        "product_id": str(r.get("product_id", "")),
        "seller_id": str(r.get("seller_id", "")),
        "data": r
    }

def build_events(df, fn):
    return [fn(row.to_dict()) for _, row in df.iterrows()]

def main():
    kinesis = boto3.client("kinesis", region_name=REGION)

    orders = build_events(read_csv(ORDERS), order_event)
    payments = build_events(read_csv(PAYMENTS), payment_event)
    items = build_events(read_csv(ITEMS), item_event)

    events = orders + items + payments
    random.shuffle(events)
    events = events[:MAX_EVENTS]

    print("STREAMING EVENTS:", len(events))

    for i, e in enumerate(events, 1):
        send(kinesis, e)

        if i % 10000 == 0:
            print("Sent:", i)

        time.sleep(SLEEP)
    
    print("Done")

if __name__ == "__main__":
    main()


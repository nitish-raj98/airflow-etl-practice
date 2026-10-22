import csv
import os
from datetime import datetime

from airflow.decorators import dag, task

DATA_DIR = "/opt/airflow/data"


def write_rows(path, rows, fieldnames):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_rows(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


@dag(
    dag_id="parallel_etl_weekday",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["practice"],
)
def parallel_etl_weekday():

    @task
    def extract():
        os.makedirs(DATA_DIR, exist_ok=True)
        products = ["A4", "A3", "Cardboard"] * 3 + ["A4"]
        amounts = [1000, 500, 1500, 800, 1200, 700, 900, 300, 1100, 650]
        rows = [
            {"order_id": i + 1, "product": products[i], "amount": amounts[i]}
            for i in range(10)
        ]
        path = os.path.join(DATA_DIR, "raw_sales.csv")
        write_rows(path, rows, ["order_id", "product", "amount"])
        print(f"Extracted {len(rows)} rows to {path}")
        return path

    @task
    def transform_totals(raw_path: str):
        rows = read_rows(raw_path)
        totals = {}
        for r in rows:
            totals[r["product"]] = totals.get(r["product"], 0) + int(r["amount"])
        out_rows = [{"product": p, "amount": t} for p, t in totals.items()]
        out_path = os.path.join(DATA_DIR, "transform_totals.csv")
        write_rows(out_path, out_rows, ["product", "amount"])
        print(f"Totals written to {out_path}")
        return out_path

    @task
    def transform_tax(raw_path: str):
        rows = read_rows(raw_path)
        out_rows = [
            {**r, "tax_18pct": round(int(r["amount"]) * 0.18, 2)} for r in rows
        ]
        out_path = os.path.join(DATA_DIR, "transform_tax.csv")
        write_rows(out_path, out_rows, ["order_id", "product", "amount", "tax_18pct"])
        print(f"Tax data written to {out_path}")
        return out_path

    @task
    def transform_large_orders(raw_path: str):
        rows = read_rows(raw_path)
        out_rows = [r for r in rows if int(r["amount"]) >= 1000]
        out_path = os.path.join(DATA_DIR, "transform_large_orders.csv")
        write_rows(out_path, out_rows, ["order_id", "product", "amount"])
        print(f"{len(out_rows)} large orders written to {out_path}")
        return out_path

    @task.branch
    def decider(**context):
        # logical_date.weekday(): Monday=0 ... Sunday=6
        weekday = context["logical_date"].weekday()
        print(f"Run weekday: {weekday}")
        if weekday >= 5:  # Saturday or Sunday
            return "skip_load"
        return "load"

    @task
    def skip_load():
        print("Weekend: load skipped")

    @task(trigger_rule="none_failed_min_one_success")
    def load(totals_path: str, tax_path: str, large_path: str):
        for label, path in [("totals", totals_path), ("tax", tax_path), ("large", large_path)]:
            rows = read_rows(path)
            print(f"Loaded {label}: {len(rows)} rows from {path}")
        return "load complete"

    raw = extract()
    totals = transform_totals(raw)
    tax = transform_tax(raw)
    large = transform_large_orders(raw)

    # decider runs after all three transforms finish
    branch = decider()
    [totals, tax, large] >> branch

    skipped = skip_load()
    loaded = load(totals, tax, large)

    branch >> [loaded, skipped]


parallel_etl_weekday()
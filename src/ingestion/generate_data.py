import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200
NUM_ORDERS = 5000
NUM_ORDER_ITEMS = 12000
NUM_REVIEWS = 3000

print("Generating customer data...")

customers = []

for customer_id in range(1, NUM_CUSTOMERS + 1):

    customer = {
        "customer_id": customer_id,
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "state": fake.state(),
        "registration_date": fake.date_between(
            start_date="-3y",
            end_date="today"
        )
    }

    customers.append(customer)

customers_df = pd.DataFrame(customers)

print(customers_df.head())

output_path = "data/raw/customers.csv"

customers_df.to_csv(output_path, index=False)

print(f"Customers saved to {output_path}")
print("Generating product data...")

products = []

categories = [
    "Electronics",
    "Clothing",
    "Home",
    "Books",
    "Sports",
    "Beauty"
]

for product_id in range(1, NUM_PRODUCTS + 1):

    product = {
        "product_id": product_id,
        "product_name": fake.catch_phrase(),
        "category": random.choice(categories),
        "price": round(random.uniform(10, 2000), 2),
        "supplier": fake.company()
    }

    products.append(product)

products_df = pd.DataFrame(products)

print(products_df.head())

products_df.to_csv("data/raw/products.csv", index=False)

print("Products saved to data/raw/products.csv")

print("Generating order data...")

orders = []

order_statuses = [
    "Completed",
    "Pending",
    "Cancelled",
    "Shipped"
]

payment_methods = [
    "Credit Card",
    "Debit Card",
    "UPI",
    "Net Banking",
    "Cash on Delivery"
]

for order_id in range(1, NUM_ORDERS + 1):

    order = {
        "order_id": order_id,
        "customer_id": random.randint(1, NUM_CUSTOMERS),
        "order_date": fake.date_between(
            start_date="-2y",
            end_date="today"
        ),
        "status": random.choice(order_statuses),
        "payment_method": random.choice(payment_methods)
    }

    orders.append(order)

orders_df = pd.DataFrame(orders)

print(orders_df.head())

orders_df.to_csv("data/raw/orders.csv", index=False)

print("Orders saved to data/raw/orders.csv")


print("Generating order item data...")

order_items = []

for _ in range(NUM_ORDER_ITEMS):

    order_item = {
        "order_id": random.randint(1, NUM_ORDERS),
        "product_id": random.randint(1, NUM_PRODUCTS),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10, 2000), 2)
    }

    order_items.append(order_item)

order_items_df = pd.DataFrame(order_items)

print(order_items_df.head())

order_items_df.to_csv(
    "data/raw/order_items.csv",
    index=False
)

print("Order items saved to data/raw/order_items.csv")

print("Generating review data...")

reviews = []

for review_id in range(1, NUM_REVIEWS + 1):

    review = {
        "review_id": review_id,
        "customer_id": random.randint(1, NUM_CUSTOMERS),
        "product_id": random.randint(1, NUM_PRODUCTS),
        "rating": random.randint(1, 5),
        "review_text": fake.paragraph(nb_sentences=2),
        "review_date": fake.date_between(
            start_date="-2y",
            end_date="today"
        )
    }

    reviews.append(review)

reviews_df = pd.DataFrame(reviews)

print(reviews_df.head())

reviews_df.to_csv(
    "data/raw/reviews.csv",
    index=False
)

print("Reviews saved to data/raw/reviews.csv")
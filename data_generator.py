import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

def generate_ecommerce_data(num_customers=1000, num_products=500, num_orders=5000, start_date_str="2022-01-01"):
    """
    Generates synthetic e-commerce customer, product, order, and order item data.

    Args:
        num_customers (int): Number of unique customers to generate.
        num_products (int): Number of unique products to generate.
        num_orders (int): Number of unique orders to generate.
        start_date_str (str): The start date for data generation in YYYY-MM-DD format.

    Returns:
        tuple: A tuple containing four pandas DataFrames:
               (customers_df, products_df, orders_df, order_items_df)
    """
    fake = Faker()
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.now()

    print("Generating customers data...")
    # --- 1. Generate Customers Data ---
    customers_data = []
    customer_ids = set()
    while len(customer_ids) < num_customers:
        customer_id = len(customer_ids) + 1 # Simple incremental ID
        customer_ids.add(customer_id)
        registration_date = fake.date_between(start_date=start_date, end_date=end_date)
        customers_data.append({
            'customer_id': customer_id,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.unique.email(),
            'registration_date': registration_date.strftime("%Y-%m-%d"),
            'country': fake.country_code(representation="alpha-3") # e.g., 'USA', 'CAN'
        })
    customers_df = pd.DataFrame(customers_data)
    print(f"Generated {len(customers_df)} customers.")

    print("Generating products data...")
    # --- 2. Generate Products Data ---
    products_data = []
    product_ids = set()
    product_categories = ["Electronics", "Books", "Home & Kitchen", "Apparel", "Sports", "Beauty", "Toys", "Automotive", "Groceries"]
    while len(product_ids) < num_products:
        product_id = len(product_ids) + 1
        product_ids.add(product_id)
        products_data.append({
            'product_id': product_id,
            'product_name': fake.word().capitalize() + " " + random.choice(["Pro", "Lite", "Max", "Edition", "Kit", "Set"]) + " " + str(fake.unique.random_int(min=100, max=9999)),
            'category': random.choice(product_categories),
            'price': round(random.uniform(5.00, 1500.00), 2)
        })
    products_df = pd.DataFrame(products_data)
    print(f"Generated {len(products_df)} products.")

    print("Generating orders data and order items data...")
    # --- 3. Generate Orders and Order Items Data ---
    orders_data = []
    order_items_data = []
    order_ids = set()
    order_item_counter = 1

    # Convert registration_date to datetime objects for comparison
    customers_df['registration_date_dt'] = pd.to_datetime(customers_df['registration_date'])

    for _ in range(num_orders):
        order_id = len(order_ids) + 1
        order_ids.add(order_id)

        # Select a random customer
        customer = customers_df.sample(1).iloc[0]
        customer_id = customer['customer_id']
        customer_registration_date = customer['registration_date_dt']

        # Ensure order_date is after customer registration date
        min_order_date = max(customer_registration_date, start_date)
        order_date = fake.date_between(start_date=min_order_date, end_date=end_date)

        order_status = random.choice(["COMPLETED", "PENDING", "SHIPPED", "CANCELLED", "RETURNED"])

        # Generate order items for this order (1 to 5 items per order)
        num_items_in_order = random.randint(1, 5)
        current_order_total = 0.0

        selected_products = products_df.sample(num_items_in_order, replace=True) # Products can be repeated

        for idx, product in selected_products.iterrows():
            order_item_id = order_item_counter
            order_item_counter += 1
            product_id = product['product_id']
            unit_price = product['price']
            quantity = random.randint(1, 3) # Quantity per item
            line_item_total = round(unit_price * quantity, 2)
            current_order_total += line_item_total

            order_items_data.append({
                'order_item_id': order_item_id,
                'order_id': order_id,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price
            })

        orders_data.append({
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': order_date.strftime("%Y-%m-%d"),
            'total_amount': round(current_order_total, 2),
            'status': order_status
        })

    orders_df = pd.DataFrame(orders_data)
    order_items_df = pd.DataFrame(order_items_data)

    # Drop the temporary datetime column from customers_df
    customers_df = customers_df.drop(columns=['registration_date_dt'])

    print(f"Generated {len(orders_df)} orders.")
    print(f"Generated {len(order_items_df)} order items.")
    return customers_df, products_df, orders_df, order_items_df

if __name__ == "__main__":
    # Parameters for data generation
    num_customers = 50000
    num_products = 2000
    num_orders = 20000
    start_data_generation_date = "2024-01-01" # Start date for historical data

    customers_df, products_df, orders_df, order_items_df = generate_ecommerce_data(
        num_customers=num_customers,
        num_products=num_products,
        num_orders=num_orders,
        start_date_str=start_data_generation_date
    )

    # Define output directory
    output_dir = "W:/work/Projects/real-time_DE_project/"
    import os
    os.makedirs(output_dir, exist_ok=True)

    # Save DataFrames to CSV
    customers_df.to_csv(os.path.join(output_dir, "customers.csv"), index=False)
    products_df.to_csv(os.path.join(output_dir, "products.csv"), index=False)
    orders_df.to_csv(os.path.join(output_dir, "orders.csv"), index=False)
    order_items_df.to_csv(os.path.join(output_dir, "order_items.csv"), index=False)

    print(f"\nSynthetic datasets saved to the '{output_dir}' directory:")
    print(f"- {output_dir}/customers.csv")
    print(f"- {output_dir}/products.csv")
    print(f"- {output_dir}/orders.csv")
    print(f"- {output_dir}/order_items.csv")

    print("\nSample Data:")
    print("\nCustomers (first 5 rows):")
    print(customers_df.head())
    print("\nProducts (first 5 rows):")
    print(products_df.head())
    print("\nOrders (first 5 rows):")
    print(orders_df.head())
    print("\nOrder Items (first 5 rows):")
    print(order_items_df.head())

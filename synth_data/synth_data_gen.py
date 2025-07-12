import csv
import random
from faker import Faker
from tqdm import tqdm

# --- Configuration ---
NUM_USERS = 1_000_000
NUM_TRANSACTIONS = 1_000_000
USERS_FILENAME = 'users.csv'
TRANSACTIONS_FILENAME = 'transactions.csv'

# Initialize the Faker generator
fake = Faker()

def generate_users_file():
    """Generates a pipe-separated CSV file with user data."""
    print(f"Generating {NUM_USERS} users into {USERS_FILENAME}...")
    with open(USERS_FILENAME, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='|')
        # Write header
        writer.writerow(['user_id', 'name', 'email', 'signup_date'])
        
        # Use tqdm for a progress bar
        for i in tqdm(range(1, NUM_USERS + 1), desc="Creating Users"):
            name = fake.name()
            email = fake.email()
            # Generate a random signup date from the last 5 years
            signup_date = fake.date_time_between(start_date='-5y', end_date='now').isoformat()
            writer.writerow([i, name, email, signup_date])

def generate_transactions_file():
    """Generates a pipe-separated CSV file with transaction data."""
    print(f"\nGenerating {NUM_TRANSACTIONS} transactions into {TRANSACTIONS_FILENAME}...")
    with open(TRANSACTIONS_FILENAME, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='|')
        # Write header
        writer.writerow(['transaction_id', 'user_id', 'amount', 'transaction_date'])
        
        # Use tqdm for a progress bar
        for i in tqdm(range(1, NUM_TRANSACTIONS + 1), desc="Creating Transactions"):
            transaction_id = f'txn_{i:07d}'
            # Pick a random user ID that is guaranteed to exist
            user_id = random.randint(1, NUM_USERS)
            amount = round(random.uniform(5.0, 1000.0), 2)
            transaction_date = fake.date_time_between(start_date='-3y', end_date='now').isoformat()
            writer.writerow([transaction_id, user_id, amount, transaction_date])

if __name__ == '__main__':
    generate_users_file()
    generate_transactions_file()
    print(f"\nData generation complete!")
    print(f"Files created: {USERS_FILENAME}, {TRANSACTIONS_FILENAME}")


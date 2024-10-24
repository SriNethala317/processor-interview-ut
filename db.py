
import psycopg2
from psycopg2 import OperationalError
from os import environ as env
from dotenv import find_dotenv, load_dotenv
from supabase import create_client, Client

ENV_FILE = find_dotenv()

if ENV_FILE:
    load_dotenv(ENV_FILE)


conn = psycopg2.connect(database = "signaPay", user = "postgres", host = 'localhost', password = "welcome123", port = 5432)

# SQL statement to create ENUM type if it doesn't exist
create_enum_sql = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transaction_types') THEN
        CREATE TYPE transaction_types AS ENUM('transfer', 'credit', 'debit');
    END IF;
END $$;
"""

# SQL statement to create tables with constraints
create_tables_sql = """
CREATE TABLE IF NOT EXISTS transaction_table (
    transaction_id SERIAL PRIMARY KEY,
    account_name TEXT NOT NULL,
    card_number VARCHAR(16) NOT NULL CHECK (LENGTH(card_number) = 16),
    transaction_amount DECIMAL(10, 2) NOT NULL,
    transaction_type transaction_types NOT NULL,
    description TEXT, 
    target_card VARCHAR(16) CHECK (transaction_type != 'transfer' OR LENGTH(target_card) = 16),
    transaction_file TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cards (
    card_number VARCHAR(16) PRIMARY KEY,
    card_balance DECIMAL(10, 2) DEFAULT 0,
    CONSTRAINT card_length CHECK (LENGTH(card_number) = 16)
);

CREATE TABLE IF NOT EXISTS card_accounts (
    card_number VARCHAR(16) REFERENCES cards(card_number) ON DELETE CASCADE,
    account_id INT REFERENCES accounts(account_id) ON DELETE CASCADE,
    PRIMARY KEY (card_number, account_id)
);

CREATE TABLE IF NOT EXISTS invalid_table (
    invalid_id SERIAL PRIMARY KEY,
    transaction_id INT REFERENCES transaction_table(transaction_id) ON DELETE CASCADE,
    invalid_reason TEXT NOT NULL
);
"""




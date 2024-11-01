
import psycopg2
from psycopg2 import OperationalError
from os import environ as env
from dotenv import find_dotenv, load_dotenv
from supabase import create_client, Client

ENV_FILE = find_dotenv()

if ENV_FILE:
    load_dotenv(ENV_FILE)

# SQL statement to create tables with constraints
initial_setup_of_tables = """

DROP TABLE IF EXISTS invalid_transactions;
DROP TABLE IF EXISTS card_accounts;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS cards;


DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transaction_types') THEN
        CREATE TYPE transaction_types AS ENUM('transfer', 'credit', 'debit');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS accounts (
    account_id SERIAL PRIMARY KEY,
    account_name TEXT UNIQUE NOT NULL  -- Ensure account_name is unique
);

CREATE TABLE IF NOT EXISTS cards (
    card_number VARCHAR(16) PRIMARY KEY,
    card_balance DECIMAL(10, 2) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    account_id INT REFERENCES accounts(account_id) ON DELETE CASCADE,
    card_number VARCHAR(16) REFERENCES cards(card_number), --NOT NULL CHECK (LENGTH(card_number) = 16)
    transaction_amount DECIMAL(10, 2), --NOT NULL
    transaction_type transaction_types, -- NOT NULL
    description TEXT, 
    target_card VARCHAR(16),
    transaction_file TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS card_accounts (
    card_number VARCHAR(16) REFERENCES cards(card_number) ON DELETE CASCADE,
    account_id INT REFERENCES accounts(account_id) ON DELETE CASCADE,
    PRIMARY KEY (card_number, account_id)  -- Composite primary key for uniqueness
);

CREATE TABLE IF NOT EXISTS invalid_transactions (
    invalid_id SERIAL PRIMARY KEY,
    transaction_id INT REFERENCES transactions(transaction_id) ON DELETE CASCADE,
    invalid_reason TEXT NOT NULL
);

CREATE OR REPLACE FUNCTION delete_account_if_no_transactions()
RETURNS TRIGGER AS $$
BEGIN
    -- Check if there are no transactions remaining for this account
    IF NOT EXISTS (SELECT 1 FROM transactions WHERE account_id = OLD.account_id) THEN
        DELETE FROM accounts WHERE account_id = OLD.account_id;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER after_transaction_delete
AFTER DELETE ON transactions
FOR EACH ROW EXECUTE FUNCTION delete_account_if_no_transactions();

"""

tables_check = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';

"""
with psycopg2.connect(env.get('DATABASE_URI')) as conn:
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print(f"PostgreSQL Database Version: {db_version}")

        def initial_setup():
            cursor.execute(initial_setup_of_tables)
            cursor.execute(tables_check)

            print(cursor.fetchall())
            conn.commit()
        
        def account_exists(account_name):
            cursor.execute("SELECT account_id FROM accounts WHERE account_name = %s", (account_name,))
            return cursor.fetchone()
        
        def create_account(account_name):
            cursor.execute("""INSERT INTO accounts (account_name)
                           VALUES (%s) RETURNING account_id""", (account_name,))
            conn.commit()
            return cursor.fetchone()

        def card_exists(card_number):
            cursor.execute("SELECT card_number FROM cards WHERE card_number = %s", (card_number,))
            return cursor.fetchone()

        def create_card(card_number):
            cursor.execute("""INSERT INTO cards (card_number)
                           VALUES (%s) RETURNING card_number""", (card_number,))
            conn.commit()
            return cursor.fetchone()
        
        def card_and_account_link_exists(card_number, account_id):
            cursor.execute("SELECT card_number FROM card_accounts WHERE card_number = %s AND account_id = %s", (card_number, account_id))
            return cursor.fetchone()

        def link_card_and_account(card_number, account_id):
            cursor.execute("""INSERT INTO card_accounts (card_number, account_id)
                           VALUES (%s, %s) RETURNING card_number""", (card_number, account_id))
            conn.commit()
            return cursor.fetchone()

        def display_accounts():
            cursor.execute("""SELECT * FROM accounts""")
            return cursor.fetchall()
        
        def display_cards():
            cursor.execute("""SELECT * FROM cards""")
            return cursor.fetchall()
        
        def add_to_transactions(account_id, card_number, transaction_amount, transaction_type, description, transaction_file, target_card=None):
            cursor.execute("""INSERT INTO transactions (account_id, card_number, transaction_amount, transaction_type, description, transaction_file, target_card)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""", (account_id, card_number, transaction_amount, transaction_type, description, transaction_file, target_card))
            conn.commit()
            
    except Exception as error:
        print(f'Connect failed. Error: {error}')
        conn.rollback()






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
DROP INDEX IF EXISTS idx_transactions_account_id;
DROP INDEX IF EXISTS idx_transactions_card_number;
DROP INDEX IF EXISTS idx_card_accounts_account_id;
DROP INDEX IF EXISTS idx_transactions_account_name;
DROP INDEX IF EXISTS idx_transactions_card_number;
DROP INDEX IF EXISTS idx_card_accounts_account_name;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transaction_types') THEN
        CREATE TYPE transaction_types AS ENUM('transfer', 'credit', 'debit');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS accounts (
    account_name TEXT UNIQUE NOT NULL PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS cards (
    card_number VARCHAR(16) PRIMARY KEY,
    card_balance DECIMAL(10, 2) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    account_name TEXT REFERENCES accounts(account_name) ON DELETE CASCADE,
    card_number VARCHAR(16) REFERENCES cards(card_number),
    transaction_amount DECIMAL(10, 2) NOT NULL,
    transaction_type transaction_types NOT NULL,
    description TEXT, 
    target_card VARCHAR(16),
    transaction_file TEXT NOT NULL
);

-- Indexes for faster lookups
CREATE INDEX IF NOT EXISTS idx_transactions_account_name ON transactions(account_name);
CREATE INDEX IF NOT EXISTS idx_transactions_card_number ON transactions(card_number);

CREATE TABLE IF NOT EXISTS card_accounts (
    card_number VARCHAR(16) REFERENCES cards(card_number) ON DELETE CASCADE,
    account_name TEXT REFERENCES accounts(account_name) ON DELETE CASCADE,
    PRIMARY KEY (card_number, account_name)
);

CREATE INDEX IF NOT EXISTS idx_card_accounts_account_name ON card_accounts(account_name);

CREATE TABLE IF NOT EXISTS invalid_transactions (
    invalid_id SERIAL PRIMARY KEY,
    account_name TEXT,
    card_number TEXT, 
    transaction_amount TEXT,
    transaction_type TEXT,
    description TEXT,
    target_card TEXT,
    transaction_file TEXT NOT NULL,
    invalid_reason TEXT NOT NULL
);

CREATE OR REPLACE FUNCTION delete_account_if_no_transactions()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM transactions WHERE account_name = OLD.account_name) THEN
        DELETE FROM accounts WHERE account_name = OLD.account_name;
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
        
        
        def create_account_ignore(account_name):
            try:
                # Assuming this function inserts or finds the account, returning account_id
                query = """INSERT INTO accounts (account_name)
                        VALUES (%s)
                        ON CONFLICT (account_name) DO UPDATE SET account_name = EXCLUDED.account_name"""
                cursor.execute(query, (account_name,))
            except Exception as e:
                print(f"Error in create_account_ignore for account_name {account_name}: {e}")
                raise
        
        def create_card_ignore(card_number):
            cursor.execute("""INSERT INTO cards (card_number)
                           VALUES (%s)
                           ON CONFLICT (card_number)
                           DO NOTHING""", (card_number,))
        
        def link_card_and_account_ignore(card_number, account_id):
            cursor.execute("""INSERT INTO card_accounts (card_number, account_id)
                           VALUES (%s, %s)
                           ON CONFLICT (card_number, account_id) DO NOTHING""", (card_number, account_id))
            

        def display_accounts():
            print('got to display accounts')
            cursor.execute("""SELECT * FROM accounts""")
            return cursor.fetchall()
        
        def display_cards():
            print('got to display cards')
            cursor.execute("""SELECT * FROM cards""")
            return cursor.fetchall()

        def display_transactions():
            cursor.execute("SELECT * FROM transactions")
            return cursor.fetchall()
        
        def add_to_transactions_bulk(transaction_data):
            insert_transaction_query = """INSERT INTO transactions (account_name, card_number, transaction_amount, transaction_type, description, transaction_file, target_card)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            # try:
            cursor.executemany(insert_transaction_query, transaction_data)
            # except Exception as e:
            #     print(f"Error during bulk insert: {e}")
            #     conn.rollback()

        def add_to_invalid_transactions(account_id, card_number, transaction_amount, transaction_type, description, transaction_file, invalid_reason, target_card=None):
            cursor.execute("""INSERT INTO invalid_transactions (account_name, card_number, transaction_amount, transaction_type, description, transaction_file, target_card, invalid_reason)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING invalid_id""", (account_id, card_number, transaction_amount, transaction_type, description, transaction_file, target_card, invalid_reason))
            #conn.commit()
            return cursor.fetchone()
        
        def update_card_balance(card_number, transaction_amount):
            cursor.execute("""UPDATE cards
                           SET card_balance = card_balance + %s
                           WHERE card_number = %s""", (transaction_amount, card_number))
            conn.commit()
        
        def display_transfers(transaction_type):
            cursor.execute("""SELECT * FROM transactions 
                           WHERE transaction_type = %s""", (transaction_type,))
            return cursor.fetchall()
        
        def commit_to_db():
            try:
                conn.commit()
            except Exception as e:
                print('Commit failed')
        
        def display_invalid_transactions(transaction_file):
            cursor.execute("""SELECT * FROM invalid_transactions 
                           WHERE transaction_file""", (transaction_file,))
            return cursor.fetchall()

    except Exception as error:
        print(f'Connect failed. Error: {error}')
        conn.rollback()





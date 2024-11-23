import pandas as pd
import io
from db import *
import os
import threading
import concurrent.futures
from test import *
import time
from datetime import datetime

batch_lock = threading.Lock()
invalid_lock = threading.Lock()
account_lock = threading.Lock()
card_lock = threading.Lock()
link_lock = threading.Lock()

RED = '\033[31m'
# ANSI escape code to reset color to default
RESET = '\033[0m'

# Caches and settings
batch_transactions = []  # For bulk insert
the_futures = []
account_cache = set()
card_cache = set()
card_account_link_cache = set()
batch_size = 500

def process_row(i, row):
    thread_name = threading.current_thread().name
    # print(thread_name, 'index:', i)
    account_name = row['account_name']
    card_number = str(row['card_number'])
    transaction_amount = row['transaction_amount']
    transaction_type = str(row['transaction_type']).lower()
    description = row['description']
    target_card = row['target_card']

    is_float = True
    # Preprocess target_card if it's not NaN
    if pd.notna(target_card):
        target_card = str(int(target_card))
    
    # Error checking
    err_msg = ""
    if pd.isna(account_name):
        err_msg += "Account name shouldn't be NaN. "
    if pd.isna(card_number) or len(card_number) != 16 or not card_number.isdigit():
        err_msg += "Card number should be a 16-digit number and not NaN. "

    try:
        transaction_amount = float(transaction_amount)
    except Exception as e:
        is_float = False

    if pd.isna(transaction_amount) or not is_float or not isinstance(transaction_amount, (int, float)):
        err_msg += "Transaction amount shouldn't be NaN and should be a number. "
    
    valid_transaction_types = {"transfer", "credit", "debit"}
    if transaction_type not in valid_transaction_types:
        err_msg += "Transaction type must be either Transfer, Credit, or Debit. "

    if transaction_type == "transfer":
        if pd.isna(target_card) or len(target_card) != 16 or not target_card.isdigit():
            err_msg += "For Transfer, target card shouldn't be NaN and must be a 16-digit number. "
    else:
        if pd.notna(target_card):
            err_msg += f"For {transaction_type}, target card should be NaN. "
            target_card = None
    
    if pd.isna(description):
        err_msg += "Description shouldn't be NaN. "
    
    # Log errors and skip to next row if errors exist
    if err_msg:
        print(f"Row {i} error: {err_msg.strip()}")
        print(f"Invalid entry: {account_name}, {card_number}, {transaction_amount}, {transaction_type}, {description}, {target_card}")
        with invalid_lock:
            add_to_invalid_transactions(account_name, card_number, transaction_amount, transaction_type, description, transaction_file, f"Row {i} error: {err_msg.strip()}")
        return  # Skip processing for this row

     # Cache lookups for accounts and cards
    # print(thread_name, 'waiting for account lock for account look up and creation')
    with account_lock:
        # print(thread_name, 'Got account lock')
        if account_name not in account_cache:
            create_account_ignore(account_name)
            # print('account called for:', i)
            account_cache.add(account_name) 
        # else:
        #     print('account called for:', i, 'already made', account_name)
    
    def deal_with_card(crd_no):
        # print(thread_name, 'waiting for card lock for card look up and creation')
        with card_lock:
            # print(thread_name, 'got card_lock')
            if crd_no not in card_cache:
                create_card_ignore(crd_no)
                # print('card called for:', i)
                card_cache.add(crd_no)

    
    deal_with_card(card_number)

        # Check if card and account link exists
    # print(thread_name, 'waiting for link lock')
    with link_lock:
        # print(thread_name, 'got link lock')
        test1 = (card_number, account_name) not in card_account_link_cache
        # print(thread_name, i, 'if statement', test1)
        if test1:  
            # link_card_and_account_ignore(card_number, account_id)
            # print('link_card_and_account called for:', i)
            card_account_link_cache.add((card_number, account_name))
        # else:
        #     print('already in card account link cache for:', i)

    # Handle transaction balances
    if transaction_type == 'credit':
        # print(thread_name, 'waiting for balance lock for credit')
        with card_lock:
            # print(thread_name, 'got balance lock for credit')
            update_card_balance(card_number, transaction_amount)
    elif transaction_type == 'debit':
        # print(thread_name, 'waiting for balance lock for debit')
        with card_lock:
            # print(thread_name, 'got balance lock for debit')
            update_card_balance(card_number, -transaction_amount)
    elif transaction_type == 'transfer':
        if target_card not in card_cache:
            deal_with_card(target_card)
            # print(thread_name, 'waiting for balance lock for transfer')
            with card_lock:
                # print(thread_name, 'got balance lock for transfer')
                update_card_balance(card_number, -transaction_amount)
                update_card_balance(target_card, transaction_amount)

    # print(thread_name, 'waiting for batch lock')    
    with batch_lock:
        # print(thread_name, 'got batch lock to append to batch transaction')
        batch_transactions.append((account_name, card_number, transaction_amount, transaction_type, description, transaction_file, target_card))
        # print('batch transactions: ', batch_transactions)
        # Bulk insert if batch size is reached
        if len(batch_transactions) >= batch_size:
            print(thread_name, 'waiting for batch lock')
            print(thread_name, 'got batch_lock for bulk transaction')
            add_to_transactions_bulk(batch_transactions)
            batch_transactions.clear()  # Clear the batch after insertion
            print('committed to db')
            commit_to_db()

    return i

def routine(workers):
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        # Load the transaction data from CSV
        transaction_file = "data.csv"
        df = pd.read_csv(os.path.join("uploads", transaction_file), header=None, names=['account_name', 'card_number', 'transaction_amount', 'transaction_type', 'description', 'target_card'])
        # Setup database and initialize variables
        initial_setup()
        print('Display accounts:', display_accounts())
        print('Display cards:', display_cards())
        for i, row in df.iterrows():
            the_futures.append(executor.submit(process_row, i, row))
        
        for future in concurrent.futures.as_completed(the_futures):
            try:
                if future.result() % 1000 == 0:
                    print('Success: ', future.result())
            except Exception as e:
                print('error: ', e)
            
    if batch_transactions:
        print('waiting for batch lock for left over')
        with batch_lock:
            print('got batch lock for leftover')
            add_to_transactions_bulk(batch_transactions)
            batch_transactions.clear()
        commit_to_db()

    print(len(batch_transactions))

    print(display_transactions())

    



thread_list = [50]
total_time = {}
for i in thread_list:
    start_time = datetime.now()
    routine(i)
    end_time = datetime.now()
    print('start_time:', start_time)
    print('end_time:', end_time)
    print(f"{RED} {i} threaded time: start time: {start_time}, end time: {end_time}, Execution time: {(end_time - start_time).total_seconds():.4f} seconds{RESET}")
    total_time[i] = f"{(end_time - start_time).total_seconds():.4f}"

print(total_time)

# start_time = datetime.now()

# non_threaded()

# end_time = datetime.now()

# print('start_time:', start_time)
# print('end_time:', end_time)
# print(f"{RED}non threaded time: start time: {start_time}, end time: {end_time}, Execution time: {(end_time - start_time).total_seconds():.4f} seconds{RESET}")
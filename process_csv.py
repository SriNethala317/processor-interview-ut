import pandas as pd
import io
from db import *

# def process_file(file):
#     file_stream = io.StringIO(file.stream.read().decode("UTF8"))

df = pd.read_csv("test.csv", header=None, names=['account_name', 'card_number', 'transaction_amount', 'transaction_type', 'description', 'target_card'])
i = 0
transaction_file = "test.csv"
initial_setup()

print('display accounts: ', display_accounts())
print('display cards: ', display_cards())

for row in df.itertuples(index=True):
    account_name = row.account_name
    card_number = str(row.card_number)
    transaction_amount = row.transaction_amount
    transaction_type = str(row.transaction_type).lower()
    description = row.description
    target_card = row.target_card
    account_id = -1
    err_msg = ""

    if not pd.isna(target_card):
        target_card = str(int(target_card))

    print('transaction type:', transaction_type)
    print('target_card: ', target_card)
    # Check if account name is NaN
    if pd.isna(account_name):
        err_msg += "Account name shouldn't be NaN. "
        
    # Check if card number is a 16-digit number and not NaN
    if pd.isna(card_number) or len(card_number) != 16 or not card_number.isdigit():
        err_msg += "Card number should be a 16-digit number and not NaN. "

    # Check if transaction amount is NaN and is a number
    if pd.isna(transaction_amount) or not isinstance(transaction_amount, (int, float)):
        err_msg += "Transaction amount shouldn't be NaN and should be a number. "

    # Check transaction types
    valid_transaction_types = {"transfer", "credit", "debit"}
    if transaction_type not in valid_transaction_types:
        err_msg += "Transaction type must be either Transfer, Credit, or Debit. "

    # Additional checks based on transaction type
    if transaction_type == "transfer":
        # Target card shouldn't be NaN and should be a 16-digit number
        if pd.isna(target_card) or len(target_card) != 16 or not target_card.isdigit():
            err_msg += "For Transfer, target card shouldn't be NaN and must be a 16-digit number. "
    else:
        # For Credit and Debit, target card should be NaN
        if not pd.isna(target_card):
            err_msg += f"For {transaction_type}, target card should be NaN. "
        else:
            target_card = None

    # Check if description is NaN
    if pd.isna(description):
        err_msg += "Description shouldn't be NaN. "

    #TODO: enter into transactions table- creating cards for card_number and also target_card

    print('index: ', i)
    i+= 1
    # Print or log the error message if there are any issues
    if err_msg:
        #TODO: enter into invalid db
        print(f"Row {row.Index} error: {err_msg.strip()}")
        print(f"acc: {account_name} crd_no: {card_number} trans_amt: {transaction_amount} trans_type: {transaction_type} desc: {description} targ_crd: {target_card}")
    else:

        account_id = account_exists(account_name) 
        if not account_id:
            account_id = create_account(account_name)
        
        print('account_id: ', account_id)
        
        test1 = card_exists(card_number) 
        print('test1: ', test1)
        if not test1:
            card_number = create_card(card_number)
            print('card_number: ', card_number)

        if not card_and_account_link_exists(card_number, account_id):
            link_card_and_account(card_number, account_id)
        
        if transaction_type == 'credit':
            update_card_balance(card_number, transaction_amount)
        elif transaction_type == 'debit':
            update_card_balance(card_number, transaction_amount*-1)
        else:
            if not card_exists(target_card):
                target_card = create_card(target_card)
            update_card_balance(card_number, transaction_amount*-1)
            update_card_balance(target_card, transaction_amount)
        
        print('transaction id: ', add_to_transactions(account_id, card_number, transaction_amount, transaction_type, description, transaction_file, target_card))

        

# print('display transactions: ', display_transactions())
for i in display_transfers('transfer'):
    print('transfer: ', i)

for i in display_transfers('credit'):
    print('credit: ', i)

for i in display_transfers('debit'):
    print('debit: ', i)

for i in display_cards():
    print('card: ', i)

        

        


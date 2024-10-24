import pandas as pd
import io

# def process_file(file):
#     file_stream = io.StringIO(file.stream.read().decode("UTF8"))

df = pd.read_csv("test.csv", columns=['account_name', 'card_number', 'transaction_amount', 'transaction_type', 'description', 'target_card'])

for row in df.itertuples(index=False):
    account_name = row.account_name
    card_number = row.account_number
    transaction_amount = row.transaction_amount
    transaction_type = row.transaction_type
    description = row.description
    target_card = row.target_card

    err_msg = ""

    # Check if account name is NaN
    if pd.isna(account_name):
        err_msg += "Account name shouldn't be NaN. "

    # Check if card number is a 16-digit number and not NaN
    if pd.isna(card_number) or len(str(card_number)) != 16 or not str(card_number).isdigit():
        err_msg += "Card number should be a 16-digit number and not NaN. "

    # Check if transaction amount is NaN and is a number
    if pd.isna(transaction_amount) or not isinstance(transaction_amount, (int, float)):
        err_msg += "Transaction amount shouldn't be NaN and should be a number. "

    # Check transaction types
    valid_transaction_types = {"Transfer", "Credit", "Debit"}
    if transaction_type not in valid_transaction_types:
        err_msg += "Transaction type must be either Transfer, Credit, or Debit. "

    # Additional checks based on transaction type
    if transaction_type == "Transfer":
        # Target card shouldn't be NaN and should be a 16-digit number
        if pd.isna(target_card) or len(str(target_card)) != 16 or not str(target_card).isdigit():
            err_msg += "For Transfer, target card shouldn't be NaN and must be a 16-digit number. "
    else:
        # For Credit and Debit, target card should be NaN
        if not pd.isna(target_card):
            err_msg += f"For {transaction_type}, target card should be NaN. "

    # Check if description is NaN
    if pd.isna(description):
        err_msg += "Description shouldn't be NaN. "

    #TODO: enter into transactions table- creating cards for card_number and also target_card

    # Print or log the error message if there are any issues
    if err_msg:
        #TODO: enter into invalid db
        print(f"Row {row.Index} error: {err_msg.strip()}")
    
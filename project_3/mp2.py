import psycopg2
import uuid
from datetime import datetime

from config import read_config
from messages import *
from seller import Seller

"""
    Splits given command string by spaces and trims each token.
    Returns token list.
"""
def tokenize_command(command):
    tokens = command.split(" ")
    return [t.strip() for t in tokens]

class Mp2Client:
    def __init__(self, config_filename):
        self.db_conn_params = read_config(filename=config_filename, section="postgresql")
        self.conn = None

    """
        Connects to PostgreSQL database and returns connection object.
    """
    def connect(self):
        self.conn = psycopg2.connect(**self.db_conn_params)
        self.conn.autocommit = False

    """
        Disconnects from PostgreSQL database.
    """
    def disconnect(self):
        self.conn.close()

    """
        Prints list of available commands of the software.
    """
    def help(self):
        # prints the choices for commands and parameters
        print("\n*** Please enter one of the following commands ***")
        print("> help")
        print("> sign_up <seller_id> <password> <plan_id>")
        print("> sign_in <seller_id> <password>")
        print("> sign_out")
        print("> show_plans")
        print("> show_subscription")
        print("> change_stock <product_id> <add or remove> <amount>")
        print("> subscribe <plan_id>")
        print("> ship <order_id>")
        print("> show_cart <customer_id>")
        print("> change_cart <customer_id> <product_id> <seller_id> <add or remove> <amount>")
        print("> purchase_cart <customer_id>")
        print("> quit")
    
    """
        Saves seller with given details.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """
    def sign_up(self, seller_id, password, plan_id):
        # TODO: Implement this function
        return False, CMD_EXECUTION_FAILED

    """
        Retrieves seller information if seller_id and password is correct and seller's session_count < max_parallel_sessions.
        - Return type is a tuple, 1st element is a Seller object and 2nd element is the response message from messages.py.
        - If seller_id or password is wrong, return tuple (None, USER_SIGNIN_FAILED).
        - If session_count < max_parallel_sessions, commit changes (increment session_count) and return tuple (seller, CMD_EXECUTION_SUCCESS).
        - If session_count >= max_parallel_sessions, return tuple (None, USER_ALL_SESSIONS_ARE_USED).
        - If any exception occurs; rollback, do nothing on the database and return tuple (None, USER_SIGNIN_FAILED).
    """
    def sign_in(self, seller_id, password):
        # TODO: Implement this function
        return None, CMD_EXECUTION_FAILED


    """
        Signs out from given seller's account.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - Decrement session_count of the seller in the database.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """
    def sign_out(self, seller):
        # TODO: Implement this function
        return False, CMD_EXECUTION_FAILED


    """
        Quits from program.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - Remember to sign authenticated user out first.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """
    def quit(self, seller):
        # TODO: Implement this function
        return False, CMD_EXECUTION_FAILED


    """
        Retrieves all available plans and prints them.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful; print available plans and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).
        
        Output should be like:
        #|Name|Max Sessions
        1|Basic|2
        2|Advanced|4
        3|Premium|6
    """
    def show_plans(self):
        # TODO: Implement this function
        return False, CMD_EXECUTION_FAILED
    
    """
        Retrieves plan of the authenticated seller.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful; print the seller's plan and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).
        
        Output should be like:
        #|Name|Max Sessions
        1|Basic|2
    """
    def show_subscription(self, seller):
        # TODO: Implement this function
        return False, CMD_EXECUTION_FAILED
    
    """
        Change stock count of a product.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If new stock value is < 0, return tuple (False, CMD_EXECUTION_FAILED).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """
    def change_stock(self, seller, product_id, change_amount):
        # TODO: Implement this function
        return False, CMD_EXECUTION_FAILED

    """
        Subscribe authenticated seller to new plan.
        - Return type is a tuple, 1st element is a Seller object and 2nd element is the response message from messages.py.
        - If the new plan's max_parallel_sessions < current plan's max_parallel_sessions, return tuple (None, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE).
        - If the operation is successful, commit changes and return tuple (seller, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (None, CMD_EXECUTION_FAILED).
    """
    def subscribe(self, seller, plan_id):
        # TODO: Implement this function
        return None, CMD_EXECUTION_FAILED
    
    """
        Change stock amounts of sellers of products included in orders.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - Check shopping cart of the orders, find products and sellers, then update stocks and order status & shipping time. 
        - If everything is OK and the operation is successful, return (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """
    def ship(self, order_ids):
        # TODO: Implement this function
        return False, CMD_EXECUTION_FAILED
    
    """
        Retrieves items on the customer's temporary shopping cart (order status = 'CREATED')
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful; print items on the cart and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).
        
        Output should be like:
        Order Id|Seller Id|Product Id|Amount
        orderX|sellerX|productX|3
        orderX|sellerX|productY|1
        orderX|sellerY|productZ|4
    """
    def show_cart(self, customer_id):
        # TODO: Implement this function
        return False, CMD_EXECUTION_FAILED
        
    """
        Change count of items in temporary shopping cart (order status = 'CREATED')
        - Return type is a tuple, 1st element is boolean and 2nd element is the response message from messages.py.
        - Consider stocks of sellers when you add items to the cart, in case stock is not enough, return (False, STOCK_UNAVAILABLE).
        - Consider weight limit per order, 15 kilograms. return (False, WEIGHT_LIMIT) if it is reached for the whole order.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """
    def change_cart(self, customer_id, product_id, seller_id, change_amount):
        # TODO: Implement this function
        return False, CMD_EXECUTION_FAILED
    
    """
        Purchases items on the cart
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - When there are no items to purchase, return (False, EMPTY_CART).
        - Consider stocks of sellers when you purchase the cart, in case stock is not enough, return (False, STOCK_UNAVAILABLE).
        - If the operation is successful; return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).
        
        Actions:
        - Change stocks on stocks table
        - Update order with status='CREATED' -> status='RECEIVED' and put order_time with current datetime.
    """
    def purchase_cart(self, customer_id):
        # TODO: Implement this function
        return False, CMD_EXECUTION_FAILED
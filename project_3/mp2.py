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

        cur = None
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object
            # Check if the seller_id already exists
            cur.execute("SELECT * FROM sellers WHERE seller_id = %s", (seller_id,))
            existing_seller = cur.fetchone()

            if existing_seller:
                return False, CMD_EXECUTION_FAILED

            # Insert the new seller into the database
            cur.execute("INSERT INTO sellers (seller_id, password, session_count, plan_id) VALUES (%s, %s, 0, %s)",
                        (seller_id, password, plan_id))
            self.conn.commit()

            return True, "OK"
        except psycopg2.Error as e:
            self.conn.rollback()
            print(e)
            return False, CMD_EXECUTION_FAILED
        finally:
            if cur:
                cur.close()  # Close the cursor
            self.disconnect()  # Disconnect from the database

    """
        Retrieves seller information if seller_id and password is correct and seller's session_count < max_parallel_sessions.
        - Return type is a tuple, 1st element is a Seller object and 2nd element is the response message from messages.py.
        - If seller_id or password is wrong, return tuple (None, USER_SIGNIN_FAILED).
        - If session_count < max_parallel_sessions, commit changes (increment session_count) and return tuple (seller, CMD_EXECUTION_SUCCESS).
        - If session_count >= max_parallel_sessions, return tuple (None, USER_ALL_SESSIONS_ARE_USED).
        - If any exception occurs; rollback, do nothing on the database and return tuple (None, USER_SIGNIN_FAILED).
    """

    def sign_in(self, seller_id, password):
        cur = None
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object

            # Check if the seller_id and password match an existing seller
            cur.execute("SELECT * FROM sellers WHERE seller_id = %s AND password = %s", (seller_id, password))
            existing_seller = cur.fetchone()

            if existing_seller:
                session_count = existing_seller[2]  # Get the session count for the seller
                plan_id = existing_seller[3]  # Get the plan id for the seller

                # Check if the session count is less than the maximum allowed parallel sessions for the seller's plan
                cur.execute("SELECT max_parallel_sessions FROM plans WHERE plan_id = %s", (plan_id,))
                max_parallel_sessions = cur.fetchone()[0]

                if session_count >= max_parallel_sessions:
                    return False, USER_ALL_SESSIONS_ARE_USED

                # Create a Seller instance using retrieved data and
                # add 1 to session count since we are about to sign them in
                authenticated_seller = Seller(seller_id=existing_seller[0], session_count=existing_seller[2] + 1,
                                              plan_id=existing_seller[3])

                # Increment the session count for the seller
                cur.execute("UPDATE sellers SET session_count = session_count + 1 WHERE seller_id = %s", (seller_id,))
                self.conn.commit()

                return authenticated_seller, "OK"
            else:
                return False, USER_SIGNIN_FAILED

        except psycopg2.Error as e:
            self.conn.rollback()
            print(e)
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur:
                cur.close()  # Close the cursor
            self.disconnect()  # Disconnect from the database

    """
        Signs out from given seller's account.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - Decrement session_count of the seller in the database.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """

    def sign_out(self, seller):
        cur = None
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object

            # Ensure that the session count is at least 0
            if seller.session_count <= 0:
                return False, CMD_EXECUTION_FAILED

            # Decrement the session count for the seller
            cur.execute("UPDATE sellers SET session_count = session_count - 1 WHERE seller_id = %s",
                        (seller.seller_id,))
            self.conn.commit()

            return True, "OK"

        except psycopg2.Error as e:
            self.conn.rollback()
            print(e)
            return False, CMD_EXECUTION_FAILED
        finally:
            if cur:
                cur.close()  # Close the cursor
            self.disconnect()  # Disconnect from the database

    """
        Quits from program.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - Remember to sign authenticated user out first.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """

    def quit(self, seller):
        cur = None
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object

            # Check if a seller is authenticated and sign them out
            if seller:
                sign_out_success, sign_out_message = self.sign_out(seller)
                if not sign_out_success:
                    return False, sign_out_message

            return True, CMD_EXECUTION_SUCCESS

        except psycopg2.Error as e:
            self.conn.rollback()
            print(e)
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur:
                cur.close()  # Close the cursor
            self.disconnect()  # Disconnect from the database

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
        cur = None
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object

            # Query all available plans
            cur.execute("SELECT * FROM plans")
            plans = cur.fetchall()

            # Print header
            print("#|Name|Max Sessions")

            # Print each plan
            for idx, plan in enumerate(plans, start=1):
                plan_id, name, max_sessions = plan
                print(f"{idx}|{name}|{max_sessions}")

            return True, CMD_EXECUTION_SUCCESS

        except psycopg2.Error as e:
            self.conn.rollback()
            print(e)
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur:
                cur.close()  # Close the cursor
            self.disconnect()  # Disconnect from the database

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
        cur = None
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object

            # Query the plan details of the authenticated seller
            cur.execute("SELECT p.plan_id, p.name, p.max_parallel_sessions "
                        "FROM plans p "
                        "INNER JOIN sellers s ON p.plan_id = s.plan_id "
                        "WHERE s.seller_id = %s", (seller.seller_id,))
            subscription = cur.fetchone()

            if subscription:
                plan_id, name, max_sessions = subscription
                print("#|Name|Max Sessions")
                print(f"1|{name}|{max_sessions}")
                return True, CMD_EXECUTION_SUCCESS
            else:
                return False, CMD_EXECUTION_FAILED

        except psycopg2.Error as e:
            self.conn.rollback()
            print(e)
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur:
                cur.close()  # Close the cursor
            self.disconnect()  # Disconnect from the database

    """
        Change stock count of a product.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If new stock value is < 0, return tuple (False, CMD_EXECUTION_FAILED).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """

    def change_stock(self, seller, product_id, change_amount):
        cur = None
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object

            # Retrieve the current stock count for the specified product
            cur.execute("SELECT stock_count FROM stocks WHERE product_id = %s AND seller_id = %s",
                        (product_id, seller.seller_id))
            current_stock = cur.fetchone()

            if current_stock is None:
                return False, CMD_EXECUTION_FAILED  # Product not found in stock table

            current_stock = current_stock[0]

            # Check if stock count falls below 0
            new_stock = current_stock + change_amount
            if new_stock < 0:
                return False, CMD_EXECUTION_FAILED  # Cannot reduce stock below 0

            # Update the stock count in the database
            cur.execute("UPDATE stocks SET stock_count = %s WHERE product_id = %s AND seller_id = %s",
                        (new_stock, product_id, seller.seller_id))
            self.conn.commit()

            return True, "OK"
        except psycopg2.Error as e:
            self.conn.rollback()
            print(e)
            return False, CMD_EXECUTION_FAILED
        finally:
            if cur:
                cur.close()  # Close the cursor
            self.disconnect()  # Disconnect from the database

    """
        Subscribe authenticated seller to new plan.
        - Return type is a tuple, 1st element is a Seller object and 2nd element is the response message from messages.py.
        - If the new plan's max_parallel_sessions < current plan's max_parallel_sessions, return tuple (None, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE).
        - If the operation is successful, commit changes and return tuple (seller, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (None, CMD_EXECUTION_FAILED).
    """

    def subscribe(self, seller, plan_id):
        cur = None
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object

            # Retrieve the current plan of the authenticated seller
            cur.execute("SELECT plan_id FROM sellers WHERE seller_id = %s", (seller.seller_id,))
            current_plan_id = cur.fetchone()[0]

            # Retrieve the details of the new plan
            cur.execute("SELECT max_parallel_sessions FROM plans WHERE plan_id = %s", (plan_id,))
            new_max_sessions = cur.fetchone()[0]

            # Retrieve the max_parallel_sessions of the current plan
            cur.execute("SELECT max_parallel_sessions FROM plans WHERE plan_id = %s", (current_plan_id,))
            current_max_sessions = cur.fetchone()[0]

            # Compare max_parallel_sessions of the current plan and the new plan
            if new_max_sessions >= current_max_sessions:
                # Update the plan of the authenticated seller to the new plan
                cur.execute("UPDATE sellers SET plan_id = %s WHERE seller_id = %s", (plan_id, seller.seller_id))
                self.conn.commit()
                return seller, CMD_EXECUTION_SUCCESS
            else:
                # New plan's max_parallel_sessions is smaller than current plan's max_parallel_sessions
                return None, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE

        except psycopg2.Error as e:
            self.conn.rollback()
            print(e)
            return None, CMD_EXECUTION_FAILED
        finally:
            if cur:
                cur.close()  # Close the cursor
            self.disconnect()  # Disconnect from the database

    """
        Change stock amounts of sellers of products included in orders.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - Check shopping cart of the orders, find products and sellers, then update stocks and order status & shipping time. 
        - If everything is OK and the operation is successful, return (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """

    def ship(self, order_ids):
        cur = None
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object

            for order_id in order_ids:
                # Retrieve order details
                cur.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,))
                order = cur.fetchone()

                if not order:
                    # Order does not exist
                    self.conn.rollback()
                    return False, CMD_EXECUTION_FAILED

                # Retrieve products in the order from shopping carts
                cur.execute("SELECT product_id, seller_id, amount FROM shopping_carts WHERE order_id = %s", (order_id,))
                products = cur.fetchall()

                # Update stock amounts in the stocks table
                for product in products:
                    product_id, seller_id, amount = product
                    # Check if there are enough stocks available
                    cur.execute("SELECT stock_count FROM stocks WHERE product_id = %s AND seller_id = %s", (product_id, seller_id))
                    current_stock = cur.fetchone()[0]
                    if current_stock < amount:
                        self.conn.rollback()
                        return False, CMD_EXECUTION_FAILED  # Rollback and return False if insufficient stock
                    else:
                        # Update stock count only if there are enough stocks available
                        cur.execute(
                            "UPDATE stocks SET stock_count = stock_count - %s WHERE product_id = %s AND seller_id = %s",
                            (amount, product_id, seller_id))

                # Update order status to "SHIPPED" and set shipping datetime
                cur.execute(
                    "UPDATE orders SET status = 'SHIPPED', shipping_time = CURRENT_TIMESTAMP WHERE order_id = %s",
                    (order_id,))

            # Commit changes if all updates are successful
            self.conn.commit()
            return True, CMD_EXECUTION_SUCCESS

        except psycopg2.Error as e:
            self.conn.rollback()
            print(e)
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur:
                cur.close()  # Close the cursor
            self.disconnect()  # Disconnect from the database

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
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object

            # Retrieve items on the customer's temporary shopping cart
            cur.execute("""SELECT sc.order_id, sc.seller_id, sc.product_id, sc.amount 
                            FROM shopping_carts sc
                            JOIN orders o ON sc.order_id = o.order_id
                            WHERE o.customer_id = %s AND o.status = 'CREATED'""", (customer_id,))
            cart_items = cur.fetchall()

            if not cart_items:
                return False, CMD_EXECUTION_FAILED

            # Print items on the cart
            print("Order Id|Seller Id|Product Id|Amount")
            for item in cart_items:
                print("|".join(map(str, item)))

            return True, CMD_EXECUTION_SUCCESS

        except psycopg2.Error as e:
            print(e)
            return False, CMD_EXECUTION_FAILED

        finally:
            self.disconnect()  # Disconnect from the database

    """
        Change count of items in temporary shopping cart (order status = 'CREATED')
        - Return type is a tuple, 1st element is boolean and 2nd element is the response message from messages.py.
        - Consider stocks of sellers when you add items to the cart, in case stock is not enough, return (False, STOCK_UNAVAILABLE).
        - Consider weight limit per order, 15 kilograms. return (False, WEIGHT_LIMIT) if it is reached for the whole order.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """

    def change_cart(self, customer_id, product_id, seller_id, change_amount):
        cur = None
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object

            # Check if there is a order for the customer with status 'CREATED'
            cur.execute("SELECT order_id FROM orders WHERE customer_id = %s AND status = 'CREATED'", (customer_id,))
            existing_order = cur.fetchone()

            if not existing_order:
                # If no existing order with status 'CREATED', create a new order
                order_id = str(uuid.uuid4())  # Generate a new UUID for the order
                cur.execute("INSERT INTO orders (order_id, customer_id, status) VALUES (%s, %s, "
                            "'CREATED')",
                            (order_id, customer_id))
            else:
                # Get the existing order id
                order_id = existing_order[0]

            # Determine operation type and handle accordingly
            if change_amount >= 0:
                # Check if there is enough stock available
                cur.execute("SELECT stock_count FROM stocks WHERE product_id = %s AND seller_id = %s",
                            (product_id, seller_id))
                stock_count = cur.fetchone()[0]

                if stock_count < change_amount:
                    self.conn.rollback()
                    return False, STOCK_UNAVAILABLE  # Return False if stock is unavailable

                # Check weight limit
                # Retrieve the weight of the product
                cur.execute("SELECT weight FROM products WHERE product_id = %s", (product_id,))
                product_weight = cur.fetchone()[0]

                # Retrieve the total weight of items in the current order
                cur.execute(
                    "SELECT SUM(p.weight * sc.amount) FROM products p, shopping_carts sc WHERE p.product_id = "
                    "sc.product_id AND sc.order_id = %s",
                    (order_id,))
                total_weight_result = cur.fetchone()
                total_weight = total_weight_result[0] if total_weight_result[0] else 0

                if total_weight + (product_weight * change_amount) > 15:
                    self.conn.rollback()
                    return False, WEIGHT_LIMIT  # Return False if weight limit is exceeded

                # Check if the entry already exists in the shopping cart
                cur.execute(
                    "SELECT amount FROM shopping_carts WHERE order_id = %s AND product_id = %s AND seller_id = %s",
                    (order_id, product_id, seller_id))
                existing_amount = cur.fetchone()

                if existing_amount:
                    new_amount = existing_amount[0] + change_amount
                    cur.execute(
                        "UPDATE shopping_carts SET amount = %s WHERE order_id = %s AND product_id = %s AND seller_id "
                        "= %s",
                        (new_amount, order_id, product_id, seller_id))
                else:
                    # Add items to the shopping cart
                    cur.execute(
                        "INSERT INTO shopping_carts (order_id, product_id, seller_id, amount) VALUES (%s, %s, %s, %s)",
                        (order_id, product_id, seller_id, change_amount))

            else:
                # Check if the current count is greater than the change amount
                cur.execute(
                    "SELECT amount FROM shopping_carts WHERE order_id = %s AND product_id = %s AND seller_id = %s",
                    (order_id, product_id, seller_id))
                current_count_result = cur.fetchone()

                if not current_count_result:
                    self.conn.rollback()
                    return False, CMD_EXECUTION_FAILED

                current_count = current_count_result[0]
                new_count = current_count + change_amount

                if new_count <= 0:
                    # If the new count is zero or negative, remove the entry from the shopping cart
                    cur.execute("DELETE FROM shopping_carts WHERE order_id = %s AND product_id = %s AND seller_id = %s",
                                (order_id, product_id, seller_id))
                else:
                    # Update the count in the shopping cart
                    cur.execute(
                        "UPDATE shopping_carts SET amount = %s WHERE order_id = %s AND product_id = %s AND seller_id "
                        "= %s",
                        (new_count, order_id, product_id, seller_id))

            # Commit changes if successful
            self.conn.commit()
            return True, CMD_EXECUTION_SUCCESS

        except psycopg2.Error as e:
            self.conn.rollback()
            print(e)
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur:
                cur.close()  # Close the cursor
            self.disconnect()  # Disconnect from the database

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
        cur = None
        try:
            self.connect()  # Connect to the database
            cur = self.conn.cursor()  # Create a cursor object

            # Check for items in the cart
            cur.execute(
                "SELECT * FROM shopping_carts WHERE order_id IN (SELECT order_id FROM orders WHERE customer_id = %s "
                "AND status = 'CREATED')",
                (customer_id,))
            cart_items = cur.fetchall()

            if not cart_items:
                # No items in the cart
                self.conn.rollback()
                return False, EMPTY_CART

            # Iterate through cart items
            for item in cart_items:
                order_id, product_id, seller_id, amount = item

                # Check stock availability
                cur.execute("SELECT stock_count FROM stocks WHERE product_id = %s AND seller_id = %s",
                            (product_id, seller_id))
                stock_count = cur.fetchone()[0]

                if stock_count < amount:
                    # Insufficient stock
                    self.conn.rollback()
                    return False, STOCK_UNAVAILABLE

                # Update stock counts
                cur.execute("UPDATE stocks SET stock_count = stock_count - %s WHERE product_id = %s AND seller_id = %s",
                            (amount, product_id, seller_id))

            # Update order status and time
            cur.execute(
                "UPDATE orders SET status = 'RECEIVED', order_time = CURRENT_TIMESTAMP WHERE order_id IN (SELECT "
                "order_id FROM orders WHERE customer_id = %s AND status = 'CREATED')",
                (customer_id,))

            # Commit changes
            self.conn.commit()
            return True, CMD_EXECUTION_SUCCESS

        except psycopg2.Error as e:
            self.conn.rollback()
            print(e)
            return False, CMD_EXECUTION_FAILED

        finally:
            if cur:
                cur.close()  # Close the cursor
            self.disconnect()  # Disconnect from the database


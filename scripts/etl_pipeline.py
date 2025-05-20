# etl_pipeline.py
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='etl_pipeline.log'
)

class ETLPipeline:
    def __init__(self):
        self.source_config = {
            'host': 'localhost',
            'database': 'ecommerce_transactions',
            'user': 'etl_user',
            'password': 'etl_password'
        }
        
        self.dw_config = {
            'host': 'localhost',
            'database': 'ecommerce_dw',
            'user': 'etl_user',
            'password': 'etl_password'
        }
        
        self.last_run_date = self.get_last_run_date()
        
    def get_last_run_date(self):
        try:
            conn = mysql.connector.connect(**self.dw_config)
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(full_date) FROM dim_date")
            result = cursor.fetchone()
            return result[0] if result[0] else datetime(2000, 1, 1).date()
        except Error as e:
            logging.error(f"Error getting last run date: {e}")
            return datetime(2000, 1, 1).date()
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
    
    def run_etl(self):
        try:
            logging.info("Starting ETL process")
            
            # Connect to source and DW databases
            source_conn = mysql.connector.connect(**self.source_config)
            dw_conn = mysql.connector.connect(**self.dw_config)
            
            source_cursor = source_conn.cursor(dictionary=True)
            dw_cursor = dw_conn.cursor()
            
            # Step 1: Extract and load date dimension
            self.load_date_dimension(dw_cursor)
            dw_conn.commit()
            
            # Step 2: Extract and transform customers
            self.extract_transform_customers(source_cursor, dw_cursor)
            dw_conn.commit()
            
            # Step 3: Extract and transform products
            self.extract_transform_products(source_cursor, dw_cursor)
            dw_conn.commit()
            
            # Step 4: Extract and transform sales
            self.extract_transform_sales(source_cursor, dw_cursor)
            dw_conn.commit()
            
            # Step 5: Extract and transform inventory
            self.extract_transform_inventory(source_cursor, dw_cursor)
            dw_conn.commit()
            
            logging.info("ETL process completed successfully")
            
        except Error as e:
            logging.error(f"ETL process failed: {e}")
            if 'dw_conn' in locals() and dw_conn.is_connected():
                dw_conn.rollback()
        finally:
            if 'source_cursor' in locals():
                source_cursor.close()
            if 'dw_cursor' in locals():
                dw_cursor.close()
            if 'source_conn' in locals() and source_conn.is_connected():
                source_conn.close()
            if 'dw_conn' in locals() and dw_conn.is_connected():
                dw_conn.close()
    
    def load_date_dimension(self, dw_cursor):
        """Populate date dimension table with dates"""
        start_date = self.last_run_date + timedelta(days=1)
        end_date = datetime.now().date() + timedelta(days=365)  # Load 1 year ahead
        
        current_date = start_date
        insert_count = 0
        
        while current_date <= end_date:
            date_key = int(current_date.strftime('%Y%m%d'))
            day_of_week = current_date.weekday() + 1  # Monday=1, Sunday=7
            day_name = current_date.strftime('%A')
            month_name = current_date.strftime('%B')
            quarter = (current_date.month - 1) // 3 + 1
            is_weekend = 1 if day_of_week in [6, 7] else 0
            
            query = """
            INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, 
                                 day_of_month, day_of_year, week_of_year, 
                                 month_name, month_of_year, quarter, year, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE date_key=date_key
            """
            
            dw_cursor.execute(query, (
                date_key, current_date, day_of_week, day_name,
                current_date.day, current_date.timetuple().tm_yday,
                current_date.isocalendar()[1], month_name, current_date.month,
                quarter, current_date.year, is_weekend
            ))
            
            insert_count += dw_cursor.rowcount
            current_date += timedelta(days=1)
        
        logging.info(f"Loaded {insert_count} records into dim_date")
    
    def extract_transform_customers(self, source_cursor, dw_cursor):
        """ETL for customer dimension (SCD Type 2)"""
        # Get new/updated customers from source
        query = """
        SELECT c.* FROM customers c
        WHERE c.registration_date >= %s OR c.last_updated >= %s
        """
        source_cursor.execute(query, (self.last_run_date, self.last_run_date))
        customers = source_cursor.fetchall()
        
        if not customers:
            logging.info("No new or updated customers to process")
            return
        
        # For each customer, check if we need to insert new version
        insert_count = 0
        update_count = 0
        
        for customer in customers:
            # Check if customer exists in DW
            dw_cursor.execute("""
            SELECT customer_key FROM dim_customers 
            WHERE customer_id = %s AND current_flag = 1
            """, (customer['customer_id'],))
            existing = dw_cursor.fetchone()
            
            if existing:
                # Customer exists, check if any attributes changed
                dw_cursor.execute("""
                SELECT * FROM dim_customers 
                WHERE customer_id = %s AND current_flag = 1
                AND first_name = %s AND last_name = %s AND email = %s
                AND city = %s AND state = %s AND country = %s
                """, (
                    customer['customer_id'],
                    customer['first_name'],
                    customer['last_name'],
                    customer['email'],
                    customer['city'],
                    customer['state'],
                    customer['country']
                ))
                
                if not dw_cursor.fetchone():
                    # Attributes changed, expire old record
                    dw_cursor.execute("""
                    UPDATE dim_customers 
                    SET current_flag = 0, expiry_date = CURDATE() - INTERVAL 1 DAY
                    WHERE customer_id = %s AND current_flag = 1
                    """, (customer['customer_id'],))
                    update_count += dw_cursor.rowcount
                    
                    # Insert new version
                    self._insert_customer_record(dw_cursor, customer)
                    insert_count += 1
            else:
                # New customer
                self._insert_customer_record(dw_cursor, customer)
                insert_count += 1
        
        logging.info(f"Processed customers: {insert_count} inserts, {update_count} updates")
    
    def _insert_customer_record(self, dw_cursor, customer):
        """Helper to insert a customer record"""
        dw_cursor.execute("""
        INSERT INTO dim_customers (
            customer_id, first_name, last_name, email, city, state, country,
            registration_date, effective_date, expiry_date, current_flag
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            customer['customer_id'],
            customer['first_name'],
            customer['last_name'],
            customer['email'],
            customer['city'],
            customer['state'],
            customer['country'],
            customer['registration_date'],
            datetime.now().date(),
            '9999-12-31',
            1
        ))

    def extract_transform_products(self, source_cursor, dw_cursor):
        """ETL for product dimension (SCD Type 2)"""
        # Get new/updated products from source
        query = """
        SELECT 
            p.*, 
            c.category_name,
            pc.category_name AS parent_category_name
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.category_id
        LEFT JOIN categories pc ON c.parent_category_id = pc.category_id
        WHERE p.updated_at >= %s OR p.product_id NOT IN (
            SELECT product_id FROM dim_products
        )
        """
        source_cursor.execute(query, (self.last_run_date,))
        products = source_cursor.fetchall()
        
        if not products:
            logging.info("No new or updated products to process")
            return
        
        insert_count = 0
        update_count = 0
        
        for product in products:
            # Check if product exists in DW
            dw_cursor.execute("""
            SELECT product_key FROM dim_products 
            WHERE product_id = %s AND current_flag = 1
            """, (product['product_id'],))
            existing = dw_cursor.fetchone()
            
            if existing:
                # Product exists, check if any attributes changed
                dw_cursor.execute("""
                SELECT * FROM dim_products 
                WHERE product_id = %s AND current_flag = 1
                AND product_name = %s AND category_name = %s 
                AND parent_category_name = %s AND price = %s AND cost = %s
                """, (
                    product['product_id'],
                    product['product_name'],
                    product['category_name'],
                    product['parent_category_name'],
                    product['price'],
                    product['cost']
                ))
                
                if not dw_cursor.fetchone():
                    # Attributes changed, expire old record
                    dw_cursor.execute("""
                    UPDATE dim_products 
                    SET current_flag = 0, expiry_date = CURDATE() - INTERVAL 1 DAY
                    WHERE product_id = %s AND current_flag = 1
                    """, (product['product_id'],))
                    update_count += dw_cursor.rowcount
                    
                    # Insert new version
                    self._insert_product_record(dw_cursor, product)
                    insert_count += 1
            else:
                # New product
                self._insert_product_record(dw_cursor, product)
                insert_count += 1
        
        logging.info(f"Processed products: {insert_count} inserts, {update_count} updates")
    
    def _insert_product_record(self, dw_cursor, product):
        """Helper to insert a product record"""
        dw_cursor.execute("""
        INSERT INTO dim_products (
            product_id, product_name, category_name, parent_category_name,
            price, cost, effective_date, expiry_date, current_flag
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            product['product_id'],
            product['product_name'],
            product['category_name'],
            product['parent_category_name'],
            product['price'],
            product['cost'],
            datetime.now().date(),
            '9999-12-31',
            1
        ))
    
    def extract_transform_sales(self, source_cursor, dw_cursor):
        """ETL for sales fact table"""
        # Get new sales data from source
        query = """
        SELECT 
            o.order_id,
            o.customer_id,
            o.order_date,
            oi.product_id,
            oi.quantity,
            oi.unit_price,
            oi.subtotal,
            p.cost,
            (oi.quantity * p.cost) AS cost_amount
        FROM orders o
        JOIN order_items oi ON o.order_id = oi.order_id
        JOIN products p ON oi.product_id = p.product_id
        WHERE o.order_date >= %s
        """
        source_cursor.execute(query, (self.last_run_date,))
        sales = source_cursor.fetchall()
        
        if not sales:
            logging.info("No new sales to process")
            return
        
        insert_count = 0
        
        for sale in sales:
            # Get dimension keys
            customer_key = self._get_customer_key(dw_cursor, sale['customer_id'])
            product_key = self._get_product_key(dw_cursor, sale['product_id'])
            date_key = int(sale['order_date'].strftime('%Y%m%d'))
            
            # Insert fact record
            dw_cursor.execute("""
            INSERT INTO fact_sales (
                order_id, customer_key, product_key, date_key,
                quantity, unit_price, subtotal, cost_amount
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE order_id=order_id
            """, (
                sale['order_id'],
                customer_key,
                product_key,
                date_key,
                sale['quantity'],
                sale['unit_price'],
                sale['subtotal'],
                sale['cost_amount']
            ))
            
            insert_count += dw_cursor.rowcount
        
        logging.info(f"Loaded {insert_count} records into fact_sales")
    
    def extract_transform_inventory(self, source_cursor, dw_cursor):
        """ETL for inventory fact table"""
        # Get inventory changes since last run
        query = """
        SELECT 
            il.product_id,
            DATE(il.changed_at) AS change_date,
            SUM(CASE WHEN il.change_type = 'sale' THEN -il.change_quantity ELSE 0 END) AS items_sold,
            SUM(CASE WHEN il.change_type = 'purchase' THEN il.change_quantity ELSE 0 END) AS items_received,
            SUM(CASE WHEN il.change_type IN ('adjustment', 'damage') THEN il.change_quantity ELSE 0 END) AS items_adjusted,
            p.stock_quantity AS ending_quantity
        FROM inventory_log il
        JOIN products p ON il.product_id = p.product_id
        WHERE il.changed_at >= %s
        GROUP BY il.product_id, DATE(il.changed_at)
        """
        source_cursor.execute(query, (self.last_run_date,))
        inventory_changes = source_cursor.fetchall()
        
        if not inventory_changes:
            logging.info("No inventory changes to process")
            return
        
        insert_count = 0
        
        for change in inventory_changes:
            product_key = self._get_product_key(dw_cursor, change['product_id'])
            date_key = int(change['change_date'].strftime('%Y%m%d'))
            
            # Get starting quantity (ending quantity from previous day)
            dw_cursor.execute("""
            SELECT ending_quantity 
            FROM fact_inventory 
            WHERE product_key = %s AND date_key = %s - 1
            """, (product_key, date_key))
            
            result = dw_cursor.fetchone()
            starting_quantity = result[0] if result else 0
            
            # Insert fact record
            dw_cursor.execute("""
            INSERT INTO fact_inventory (
                product_key, date_key, starting_quantity, ending_quantity,
                items_sold, items_received, items_adjusted
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                starting_quantity = VALUES(starting_quantity),
                ending_quantity = VALUES(ending_quantity),
                items_sold = VALUES(items_sold),
                items_received = VALUES(items_received),
                items_adjusted = VALUES(items_adjusted)
            """, (
                product_key,
                date_key,
                starting_quantity,
                change['ending_quantity'],
                change['items_sold'],
                change['items_received'],
                change['items_adjusted']
            ))
            
            insert_count += dw_cursor.rowcount
        
        logging.info(f"Loaded {insert_count} records into fact_inventory")
    
    def _get_customer_key(self, dw_cursor, customer_id):
        """Helper to get current customer_key"""
        dw_cursor.execute("""
        SELECT customer_key FROM dim_customers 
        WHERE customer_id = %s AND current_flag = 1
        """, (customer_id,))
        result = dw_cursor.fetchone()
        return result[0] if result else None
    
    def _get_product_key(self, dw_cursor, product_id):
        """Helper to get current product_key"""
        dw_cursor.execute("""
        SELECT product_key FROM dim_products 
        WHERE product_id = %s AND current_flag = 1
        """, (product_id,))
        result = dw_cursor.fetchone()
        return result[0] if result else None
    
if __name__ == "__main__":
    etl = ETLPipeline()
    etl.run_etl()
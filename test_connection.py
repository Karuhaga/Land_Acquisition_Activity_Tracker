import pyodbc

# Define DATABASE_CONFIG properly
DATABASE_CONFIG = {
    'SERVER': 'DELTA-PC\\SQLEXPRESS',  # Replace with your actual SQL Server instance
    'DATABASE': 'BankReconciliationTracker',
    'USERNAME': 'brt1',
    'PASSWORD': 'BUdaka123$%',
}

# Construct the connection string
conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={DATABASE_CONFIG['SERVER']};"
    f"DATABASE={DATABASE_CONFIG['DATABASE']};"
    f"UID={DATABASE_CONFIG['USERNAME']};"
    f"PWD={DATABASE_CONFIG['PASSWORD']};"
    f"Trusted_Connection=no;"  # Set to 'yes' if using Windows Authentication
)

# Attempt to connect
try:
    conn = pyodbc.connect(conn_str)
    print("Connected successfully!")
    conn.close()
except pyodbc.Error as e:
    print("Connection failed:", e)

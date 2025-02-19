import pyodbc
from BankReconciliation import app, login_manager
from flask import flash
from flask_login import UserMixin
from config import DATABASE_CONFIG
from flask_bcrypt import Bcrypt
from datetime import datetime

bcrypt = Bcrypt(app)  # Initialize bcrypt


@login_manager.user_loader
def load_user(username):
    return User.get_by_username(username)  # User lookup


# Establishing the connection to MS SQL
def get_db_connection():
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={DATABASE_CONFIG['SERVER']};"
        f"DATABASE={DATABASE_CONFIG['DATABASE']};"
        f"UID={DATABASE_CONFIG['USERNAME']};"
        f"PWD={DATABASE_CONFIG['PASSWORD']};"
        f"Trusted_Connection=no;"  # Set to 'yes' if using Windows Authentication
    )

    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        flash("No database connection", category="danger")  # Flash error message
        print("Database connection error:", e)  # Log the error for debugging
        return None  # Return None when the connection fails


# User model for Flask-Login
class User(UserMixin):
    def __init__(self, id, username, fname, mname, sname, password_hash, email_address):
        self.id = id
        self.username = username
        self.fname = fname
        self.mname = mname
        self.sname = sname
        self.password_hash = password_hash
        self.email_address = email_address


    @staticmethod
    def get_by_username(username):
        conn = get_db_connection()
        if conn is None:
            return None  # Return None if the database connection fails

        cursor = conn.cursor()
        cursor.execute("SELECT ID, Username, Fname, Mname, Sname, Password, Email FROM users WHERE Username = ?", (username,))
        row = cursor.fetchone()
        conn.close()

        if row:
            return User(*row)
        return None


    def get_id(self):
        return self.username  # Ensure Flask-Login gets username instead of an integer ID


    @staticmethod
    def hash_password(password):
        return bcrypt.generate_password_hash(password).decode('utf-8')  # Hash password

    def check_password(self, password):
        return bcrypt.check_password_hash(self.password_hash, password)  # Verify password


class FileUploadBatch:
    def __init__(self, id, user_id, date_time):
        self.id = id
        self.user_id = user_id
        self.date_time = date_time

    @staticmethod
    def check_batch_submission_status(user_id):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        # check if User has a request pending submission
        cursor.execute("SELECT COUNT(submission_status) FROM file_upload_batch WHERE submission_status = ? and "
                       "user_id = ? ", (0, user_id))

        number_of_unsubmitted_requests = cursor.fetchone()[0]  # Fetch last batch_id
        conn.close()
        return number_of_unsubmitted_requests

    @staticmethod
    def insert_into_file_upload_batch(user_id):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        # Get the last batch_id
        cursor.execute("SELECT MAX(id) FROM file_upload_batch")
        last_batch_id = cursor.fetchone()[0]  # Fetch last batch_id

        # Set new batch_id
        new_batch_id = (last_batch_id + 1) if last_batch_id else 1

        # Insert new batch record
        now = datetime.now()

        try:
            cursor.execute(
                "INSERT INTO file_upload_batch (id, user_id, date_time, submission_status) VALUES (?, ?, ?, ?)",
                (new_batch_id, user_id, now, 0),
            )
            conn.commit()
            return new_batch_id
        except pyodbc.Error as e:
            print("Database insert error:", e)
            conn.rollback()
            return None
        finally:
            conn.close()


class FileUpload:
    def __init__(self, id, batch_id, file_name, date_time):
        self.id = id
        self.batch_id = batch_id
        self.file_name = file_name
        self.date_time = date_time

    @staticmethod
    def insert_into_file_upload(batch_id, file_name):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        # Get the last batch_id
        cursor.execute("SELECT MAX(id) FROM file_upload")
        last_file_id = cursor.fetchone()[0]  # Fetch last batch_id

        # Set new batch_id
        new_file_id = (last_file_id + 1) if last_file_id else 1

        try:
            cursor.execute(
                "INSERT INTO file_upload (id, batch_id, file_name, removed_by_user_on_upload_page) "
                "VALUES (?, ?, ?, ?)",(new_file_id, batch_id, file_name, 0),
            )
            conn.commit()
            return new_file_id
        except pyodbc.Error as e:
            print("Database insert error:", e)
            conn.rollback()
            return None
        finally:
            conn.close()

    @staticmethod
    def get_uploaded_pending_submission_files_by_user(batch_id):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # Raw MSSQL Query to fetch all files that are not marked as removed
            query = """
                            SELECT file_name 
                            FROM file_upload 
                            WHERE batch_id = ?
                            AND removed_by_user_on_upload_page = 0
                        """

            # Execute the query with user_id as parameter
            cursor.execute(query, (batch_id,))
            result = cursor.fetchall()

            # Extract file names from result
            files = [row.file_name for row in result]
            return files
        except Exception as e:
            print("Database error:", e)
            return None
        finally:
            # Close cursor and connection
            cursor.close()
            conn.close()

    @staticmethod
    def get_submitted_reconciliations(user_id):
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            # Fetch submitted reconciliations
            query = """
                SELECT b.id, b.batch_id, b.file_name, a.date_time
                FROM file_upload_batch a 
                LEFT OUTER JOIN file_upload b ON a.id = b.batch_id 
                WHERE a.user_id = ?	AND b.removed_by_user_on_upload_page = 0
            """
            cursor.execute(query, (user_id,))
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            reconciliations = [FileUpload(row.id, row.batch_id, row.file_name, row.date_time) for row in result]
            return reconciliations
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()


class FileDelete:
    def __init__(self, filename):
        self.filename = filename

    @staticmethod
    def remove_file_by_user_on_upload_page(file_name):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        # Check if file exists in the database
        cursor.execute("SELECT COUNT(*) FROM file_upload WHERE file_name = ?", (file_name,))
        file_exists = cursor.fetchone()[0]

        if file_exists:
            try:
                # Update `removed_by_user_on_upload_page` to 1
                cursor.execute(
                    "UPDATE file_upload SET removed_by_user_on_upload_page = 1 WHERE file_name = ?",
                    (file_name,)
                )
                conn.commit()
                return file_name
            except pyodbc.Error as e:
                print("Database update error:", e)
                conn.rollback()
                return None
            finally:
                conn.close()

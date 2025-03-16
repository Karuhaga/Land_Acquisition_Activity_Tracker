import pyodbc
from BankReconciliation import app, login_manager
from flask_login import UserMixin
from flask_bcrypt import Bcrypt
from datetime import datetime
from BankReconciliation.database import get_db_connection


bcrypt = Bcrypt(app)  # Initialize bcrypt


@login_manager.user_loader
def load_user(username):
    return User.get_by_username(username)  # User lookup


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
        cursor.execute("SELECT ID, Username, Fname, Mname, Sname, Password, Email FROM users WHERE Username = ?",
                       (username,))
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

    def has_permission(self, work_flow_breakdown_name):
        # Check if the user has permission for a specific workflow breakdown
        return any(
            work_flow_breakdown_name in [wfb.name for r in self.roles for wfb in r.workflows]
        )


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

        number_of_unsubmitted_batches = cursor.fetchone()[0]
        conn.close()
        return number_of_unsubmitted_batches

    @staticmethod
    def allocate_batch_id():
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # Get the last batch_id
            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM file_upload_batch")
            last_batch_id = cursor.fetchone()[0]  # Fetch last batch_id

            # Set new batch_id
            new_batch_id = (last_batch_id + 1) if last_batch_id else 1

            return new_batch_id
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def insert_into_file_upload_batch(user_id, new_batch_id):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

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

    @staticmethod
    def get_latest_batch_pending_submission_by_user(user_id):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # check if User has a request pending submission
            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM file_upload_batch WHERE submission_status = 0 AND "
                           "user_id = ? ", user_id)

            batch_id = cursor.fetchone()[0]  # Fetch last batch_id
            return batch_id
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def update_batch_submission_status(batch_id):
        """
        Updates the submission_status of a batch in the file_upload_batch table.
        """
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # check if User has a request pending submission
            cursor.execute("SELECT submission_status FROM file_upload_batch WHERE id = ?", batch_id)

            batch_submission_status = cursor.fetchone()[0]  # Fetch last batch_id

            if batch_submission_status == 0:
                query = """
                UPDATE file_upload_batch
                SET submission_status = 1
                WHERE id = ?
                """
                cursor.execute(query, (batch_id,))
                conn.commit()
            else:
                pass
            return batch_id
        except Exception as e:
            print(f"Error updating batch submission status: {e}")
        finally:
            cursor.close()
            conn.close()


class FileUpload:
    def __init__(self, id, bank_account, year, month, batch_id, file_name, date_time, approve_as):
        self.id = id
        self.bank_account = bank_account
        self.year = year
        self.month = month
        self.batch_id = batch_id
        self.file_name = file_name
        self.date_time = date_time
        self.approve_as = approve_as

    @staticmethod
    def insert_into_file_upload(batch_id, file_name, bank_account, year, month):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        # Get the last batch_id
        cursor.execute("SELECT MAX(id) FROM file_upload")
        last_file_id = cursor.fetchone()[0]  # Fetch last batch_id

        # Set new batch_id
        new_file_id = (last_file_id + 1) if last_file_id else 1

        # Insert new batch record
        now = datetime.now()

        try:
            cursor.execute(
                "INSERT INTO file_upload (id, batch_id, file_name, bank_account_id, year, month, "
                "removed_by_user_on_upload_page, last_modified)"
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (new_file_id, batch_id, file_name, bank_account, year, month, 0, now),
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
    def unsubmitted_files_num(user_id):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # Check if a record with the given bank_account, year, and month exists
            cursor.execute(
                "SELECT COUNT(*) FROM file_upload a LEFT OUTER JOIN bank_account b ON a.bank_account_id = b.id "
                "LEFT OUTER JOIN file_upload_batch c ON c.id = a.batch_id "
                "LEFT OUTER JOIN users d on d.id = c.user_id WHERE c.user_id = ? AND a.submission_status = 0 "
                "AND removed_by_user_on_upload_page = 0", user_id  # Parameters must be in a tuple
            )

            result = cursor.fetchone()[0]  # Fetch the result, which is a tuple like
            return result
        except Exception as e:
            print("Database error:", e)
            return None  # Return None to indicate an error occurred
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_uploaded_pending_submission_files_by_user(user_id):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # Raw MSSQL Query to fetch all files that are not marked as removed
            query = """
                            SELECT b.name as bank_account_id, year, 
                            (select DateName(month, DateAdd(month, month, 0) - 1)) as month, file_name 
                            FROM file_upload a 
                            LEFT OUTER JOIN bank_account b ON a.bank_account_id = b.id
                            LEFT OUTER JOIN file_upload_batch c ON c.id = a.batch_id
                            LEFT OUTER JOIN users d on d.id = c.user_id
                            WHERE c.user_id = ? AND a.submission_status = 0 AND removed_by_user_on_upload_page = 0
                            ORDER BY b.name
                        """
            # Execute the query with user_id as parameter
            cursor.execute(query, (user_id,))
            result = cursor.fetchall()

            # Convert query results to a list of dictionaries
            files = [
                {
                    "bank_account": row.bank_account_id,
                    "year": row.year,
                    "month": row.month,
                    "file_name": row.file_name
                }
                for row in result
            ]
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
            query = """SELECT b.id, c.name as bank_account, b.year, (select DateName(month, DateAdd(month, b.month, 
            0) - 1)) as month, b.batch_id, b.file_name, FORMAT(b.last_modified, 'yyyy-MM-dd HH:mm:ss') AS date_time, 
            '' as approve_as FROM file_upload_batch a LEFT OUTER JOIN file_upload b ON a.id = b.batch_id LEFT OUTER 
            JOIN bank_account c ON b.bank_account_id = c.id WHERE a.user_id = ? AND b.submission_status != 0 AND 
            b.removed_by_user_on_upload_page = 0 ORDER BY c.name"""
            cursor.execute(query, (user_id,))
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            reconciliations = [FileUpload(row.id, row.bank_account, row.year, row.month, row.batch_id, row.file_name,
                                          row.date_time, row.approve_as) for row in result]
            return reconciliations
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_reconciliations_pending_approval(user_id):
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            # Fetch submitted reconciliations
            query = """
                WITH UserRoles AS (
                    -- Get the roles assigned to the logged-in user
                    SELECT ur.role_id, r.name AS approve_as, u.organisation_unit_id
                    FROM user_role ur
                    JOIN users u ON ur.user_id = u.ID
                    JOIN role r ON ur.role_id = r.id
                    WHERE ur.user_id = ?  -- Replace with the logged-in user ID
                ),
                UserWorkflows AS (
                    -- Get workflow breakdowns assigned to user roles
                    SELECT wb.id AS workflow_breakdown_id, wb.is_responsibility_global, wb.level, ur.organisation_unit_id, ur.approve_as
                    FROM workflow_breakdown wb
                    JOIN UserRoles ur ON wb.responsible_role_id = ur.role_id
                ),
                GlobalFiles AS (
                    -- Get files where is_responsibility_global = 1
                    SELECT 
                        f.id, 
                        b.name AS bank_name, 
                        f.year, 
                        DATENAME(month, DATEADD(month, f.month, 0) - 1) AS month,
                        f.batch_id, 
                        f.file_name, 
                        FORMAT(f.last_modified, 'yyyy-MM-dd HH:mm:ss') AS date_time,
                        uw.approve_as
                    FROM file_upload f
                    JOIN bank_account ba ON f.bank_account_id = ba.id
                    JOIN bank b ON ba.bank_id = b.id
                    JOIN UserWorkflows uw ON f.submission_status = uw.level - 1
                    WHERE uw.is_responsibility_global = 1
                ),
                OrgBasedFiles AS (
                    -- Get files where is_responsibility_global = 0
                    SELECT 
                        f.id, 
                        b.name AS bank_name, 
                        f.year, 
                        DATENAME(month, DATEADD(month, f.month, 0) - 1) AS month,
                        f.batch_id, 
                        f.file_name, 
                        FORMAT(f.last_modified, 'yyyy-MM-dd HH:mm:ss') AS date_time,
                        uw.approve_as
                    FROM file_upload f
                    JOIN file_upload_batch fub ON f.batch_id = fub.id
                    JOIN bank_account ba ON f.bank_account_id = ba.id
                    JOIN bank b ON ba.bank_id = b.id
                    JOIN users u ON fub.user_id = u.ID
                    JOIN UserWorkflows uw ON f.submission_status = uw.level - 1
                    WHERE uw.is_responsibility_global = 0
                    AND u.organisation_unit_id IN (
                        -- Get org units where the parent matches the logged-in user's org unit
                        SELECT ou.id FROM organisation_unit ou
                        WHERE ou.parent_org_unit_id IN (SELECT organisation_unit_id FROM UserRoles)
                    )
                )
                -- Combine results and order by bank_name ASC
                SELECT * FROM GlobalFiles
                UNION
                SELECT * FROM OrgBasedFiles
                ORDER BY bank_name, year, month ASC;
            """
            cursor.execute(query, (user_id,))
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            reconciliations = [FileUpload(row.id, row.bank_name, row.year, row.month, row.batch_id, row.file_name,
                                          row.date_time, row.approve_as) for row in result]
            return reconciliations
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()


    @staticmethod
    def check_for_already_existing_reconciliation(bank_account, year, month):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # Check if a record with the given bank_account, year, and month exists
            cursor.execute(
                "SELECT COUNT(*) FROM file_upload WHERE bank_account_id = ? AND year = ? AND month = ?",
                (bank_account, year, month)  # Parameters must be in a tuple
            )

            result = cursor.fetchone()  # Fetch the result, which is a tuple like (count,)
            return result[0] > 0  # Returns True if at least one record exists
        except Exception as e:
            print("Database error:", e)
            return None  # Return None to indicate an error occurred
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def update_file_submission_status(bank_account_id, year, month, file_name):
        """
        Updates the submission_status of a file in the file_upload table.
        """
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # pick bank_account_id of bank_name in the uploadedFilesTable
            cursor.execute("SELECT id FROM bank_account WHERE name = ? ", bank_account_id)
            bank_account_id = cursor.fetchone()[0]  # Fetch last batch_id
            # pick value of month of name of month in the uploadedFilesTable
            # Mapping of month names to their corresponding integer values
            month_map = {
                "January": 1,
                "February": 2,
                "March": 3,
                "April": 4,
                "May": 5,
                "June": 6,
                "July": 7,
                "August": 8,
                "September": 9,
                "October": 10,
                "November": 11,
                "December": 12
            }

            # Convert the month name to an integer using the month_map
            month_int = month_map.get(month, None)  # Default to None if not found

            if month_int is None:
                raise ValueError(f"Invalid month name: {month}")

            query = """
            UPDATE file_upload
            SET submission_status = 1
            WHERE bank_account_id = ? AND year = ? AND month = ? AND file_name = ?
            """
            cursor.execute(query, (bank_account_id, year, month_int, file_name))
            conn.commit()
            return file_name
        except Exception as e:
            print(f"Error updating file submission status: {e}")
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


class BankAccount:
    def __init__(self, id, name, bank_id, currency_id, strategic_business_unit_id):
        self.id = id
        self.name = name
        self.bank_id = bank_id
        self.currency_id = currency_id
        self.strategic_business_unit_id = strategic_business_unit_id

    @staticmethod
    def get_bank_accounts_for_dropdown_menu():
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            # Fetch submitted reconciliations
            query = """SELECT id, name, bank_id, currency_id, strategic_business_unit_id FROM bank_account ORDER BY 
                        name"""
            cursor.execute(query)
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            bank_accounts = [BankAccount(row.id, row.name, row.bank_id, row.currency_id, row.strategic_business_unit_id)
                             for row in result]
            return bank_accounts
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_bank_account_name_by_id(bank_account):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # check if User has a request pending submission
            cursor.execute("SELECT name FROM bank_account WHERE id = ? ", bank_account)

            bank_account_name = cursor.fetchone()[0]  # Fetch last batch_id
            return bank_account_name
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()


class Role:
    def __init__(self, id, name, organisation_unit_id):
        self.id = id
        self.name = name
        self.organisation_unit_id = organisation_unit_id


class OrganisationUnit:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class Workflow:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class WorkflowBreakdown:
    def __init__(self, id, workflow_id, level, name, responsible_role_id, is_responsibility_global, menu_item):
        self.id = id
        self.workflow_id = workflow_id
        self.level = level
        self.name = name
        self.responsible_role_id = responsible_role_id
        self.is_responsibility_global = is_responsibility_global
        self.menu_item = menu_item


class UserRole:
    def __init__(self, id, user_id, role_id, start_datetime, expiry_datetime):
        self.id = id
        self.user_id = user_id
        self.role_id = role_id
        self.start_datetime = start_datetime
        self.expiry_datetime = expiry_datetime


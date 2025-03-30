import pyodbc
from sqlalchemy import null

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
    def get_count_of_batch_pending_submission_by_user(user_id):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # check if User has a request pending submission
            cursor.execute("SELECT COALESCE(COUNT(id), 0) FROM file_upload_batch WHERE submission_status = 0 AND "
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
    def get_batch_id_of_reconciliation_record_to_approve(bank_account_id, year, month, file_name):
        """
        Updates the submission_status of a batch in the file_upload_batch table.
        """
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # pick bank_account_id of bank_name in the uploadedFilesTable
            cursor.execute("SELECT id FROM bank_account WHERE name = ?", (bank_account_id,))
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

            cursor.execute(
                "SELECT batch_id FROM file_upload WHERE bank_account_id = ? AND year = ? AND month = ? AND file_name "
                "= ?", (bank_account_id, year, month_int, file_name))

            batch_id = cursor.fetchone()[0]  # Fetch last batch_id
            return batch_id
        except Exception as e:
            print("Database error:", e)
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
            query = """
                UPDATE file_upload_batch
                SET submission_status = submission_status + 1
                WHERE id = ? AND submission_status = 0
            """
            cursor.execute(query, (batch_id,))
            conn.commit()

            return batch_id
        except Exception as e:
            print(f"Error updating batch submission status: {e}")
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_id_of_file_upload(bank_account_id, year, month, file_name):
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

            # check if User has a request pending submission
            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM file_upload WHERE bank_account_id = ? AND "
                           "year = ? AND month = ? AND file_name = ?", bank_account_id, year, month_int, file_name)

            id_of_file_upload = cursor.fetchone()[0]  # Fetch last batch_id

            return id_of_file_upload
        except Exception as e:
            print("Database error:", e)
            return []
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
                "removed_by_user_on_upload_page, creation_datetime)"
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
                        (select DateName(month, DateAdd(month, month, 0) - 1)) as month, file_name, batch_id 
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
                    "file_name": row.file_name,
                    "batch_id": row.batch_id
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
            0) - 1)) as month, b.batch_id, b.file_name, FORMAT(b.creation_datetime, 'yyyy-MM-dd HH:mm:ss') AS date_time, 
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
    def get_approved_reconciliations(user_id):
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            # Fetch submitted reconciliations
            query = """SELECT a.id, 
                       b.name AS bank_account, 
                       a.year, 
                       (SELECT DATENAME(month, DATEADD(month, a.month, 0) - 1)) AS month, 
                       a.batch_id, 
                       a.file_name, 
                       FORMAT(a.creation_datetime, 'yyyy-MM-dd HH:mm:ss') AS date_time, 
                       '' AS approve_as 
                        FROM file_upload a 
                        LEFT OUTER JOIN bank_account b ON a.bank_account_id = b.id
                        LEFT OUTER JOIN reconciliation_approvals ra ON a.id = ra.file_upload_id
                        WHERE ra.approver_id = ?
                        GROUP BY a.id, b.name, a.year, a.month, a.batch_id, a.file_name, a.creation_datetime
                        ORDER BY b.name;
                        """
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
               DECLARE @logged_in_user_id INT = ?; -- Set logged-in user's ID

                WITH GlobalFiles AS (
                    -- Get files where responsibility is global
                    SELECT 
                        f.id, 
                        ba.name AS bank_account_name, 
                        f.year, 
                        DATENAME(month, DATEADD(month, f.month, 0) - 1) AS month,
                        f.batch_id, 
                        f.file_name, 
                        FORMAT(f.creation_datetime, 'yyyy-MM-dd HH:mm:ss') AS date_time, 
                        (SELECT TOP 1 r.name 
                         FROM role r 
                         LEFT OUTER JOIN user_role ur ON r.id = ur.role_id
                         LEFT OUTER JOIN role_workflow_breakdown rwb ON ur.role_id = rwb.role_id
                         LEFT OUTER JOIN workflow_breakdown wb ON rwb.workflow_breakdown_id = wb.id
                         WHERE wb.is_workflow_level = 1 
                         AND wb.level - 1 = f.submission_status
                         AND ur.user_id = @logged_in_user_id) AS approve_as,
                        ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.creation_datetime DESC) AS row_num
                    FROM file_upload f
                    JOIN reconciliation_approvals ra ON f.id = ra.file_upload_id
                    JOIN file_upload_batch fub ON f.batch_id = fub.id
                    JOIN users u ON fub.user_id = u.ID
                    JOIN bank_account ba ON f.bank_account_id = ba.id
                    JOIN bank b ON ba.bank_id = b.id
                    JOIN user_role ur ON u.ID = ur.user_id
                    JOIN role r ON ur.role_id = r.id
                    WHERE ra.approver_id IN
                    (SELECT DISTINCT(a.ID) 
                     FROM users a
                     JOIN organisation_unit_tier b ON a.organisation_unit_tier_id = b.id
                     WHERE b.parent_org_unit_tier_id IN 
                     (SELECT d.organisation_unit_tier_id FROM users d WHERE d.ID = @logged_in_user_id))
                    AND f.submission_status IN 
                     (SELECT DISTINCT(a.level) - 1 
                      FROM workflow_breakdown a
                      JOIN role_workflow_breakdown b ON a.id = b.workflow_breakdown_id
                      JOIN role c ON b.role_id = c.id
                      JOIN user_role d ON c.id = d.role_id
                      JOIN users e ON d.user_id = e.ID
                      WHERE a.is_responsibility_global = 1
                      AND e.ID = @logged_in_user_id)
                ),
                OrgBasedFiles AS (
                    -- Get files where responsibility is restricted to specific organizational units
                    SELECT 
                        f.id, 
                        ba.name AS bank_account_name, 
                        f.year, 
                        DATENAME(month, DATEADD(month, f.month, 0) - 1) AS month,
                        f.batch_id, 
                        f.file_name, 
                        FORMAT(f.creation_datetime, 'yyyy-MM-dd HH:mm:ss') AS date_time,
                        (SELECT TOP 1 r.name 
                         FROM role r 
                         LEFT OUTER JOIN user_role ur ON r.id = ur.role_id
                         LEFT OUTER JOIN role_workflow_breakdown rwb ON ur.role_id = rwb.role_id
                         LEFT OUTER JOIN workflow_breakdown wb ON rwb.workflow_breakdown_id = wb.id
                         WHERE wb.is_workflow_level = 1 
                         AND wb.level - 1 = f.submission_status
                         AND ur.user_id = @logged_in_user_id) AS approve_as,
                        ROW_NUMBER() OVER (PARTITION BY f.id ORDER BY f.creation_datetime DESC) AS row_num
                        FROM file_upload f
                        JOIN reconciliation_approvals ra ON f.id = ra.file_upload_id
                        JOIN file_upload_batch fub ON f.batch_id = fub.id
                        JOIN users u ON fub.user_id = u.ID
                        JOIN bank_account ba ON f.bank_account_id = ba.id
                        JOIN bank b ON ba.bank_id = b.id
                        JOIN user_role ur ON u.ID = ur.user_id
                        JOIN role r ON ur.role_id = r.id
                        WHERE ra.approver_id IN
                        (SELECT DISTINCT(a.ID) 
                         FROM users a
                         JOIN organisation_unit b ON a.organisation_unit_id = b.id
                         WHERE b.parent_org_unit_id IN 
                         (SELECT d.organisation_unit_id FROM users d WHERE d.ID = @logged_in_user_id))
                        AND f.submission_status IN 
                         (SELECT DISTINCT(a.level) - 1 
                          FROM workflow_breakdown a
                          JOIN role_workflow_breakdown b ON a.id = b.workflow_breakdown_id
                          JOIN role c ON b.role_id = c.id
                          JOIN user_role d ON c.id = d.role_id
                          JOIN users e ON d.user_id = e.ID
                          WHERE a.is_responsibility_global = 0
                          AND e.ID = @logged_in_user_id)
                    )
                    
                    -- Combine results and order by bank_name, year, month
                    SELECT id, bank_account_name, year, month, batch_id, file_name, date_time, approve_as
                    FROM (
                        SELECT * FROM GlobalFiles WHERE row_num = 1
                        UNION
                        SELECT * FROM OrgBasedFiles WHERE row_num = 1
                    ) AS UniqueResults
                    ORDER BY bank_account_name, year, month ASC;
            """
            cursor.execute(query, (user_id,))
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            reconciliations = [FileUpload(row.id, row.bank_account_name, row.year, row.month, row.batch_id,
                                          row.file_name, row.date_time, row.approve_as) for row in result]
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

    @staticmethod
    def update_file_approval_status(bank_account_id, year, month, file_name, action):
        """
        Updates the submission_status of a file in the file_upload table.
        """
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # pick bank_account_id of bank_name in the uploadedFilesTable
            cursor.execute("SELECT id FROM bank_account WHERE name = ?", (bank_account_id,))
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

            print(action)

            if action == "reject":
                query = """
                    UPDATE file_upload
                    SET submission_status = 0
                    WHERE bank_account_id = ? AND year = ? AND month = ? AND file_name = ?;
                """

            else:
                query = """
                    UPDATE file_upload
                    SET submission_status = submission_status + 1
                    WHERE bank_account_id = ? AND year = ? AND month = ? AND file_name = ?
                """

            cursor.execute(query, (bank_account_id, year, month_int, file_name))
            conn.commit()
            return file_name
        except Exception as e:
            print(f"Error updating approval status of reconciliation record: {e}")
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def update_file_approval_status_following_a_rejected_approval(file_upload_id):
        """
        Updates the submission_status of a file in the file_upload table.
        """
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            query = """
                UPDATE file_upload
                SET submission_status = 0
                WHERE id = ?
            """
            file_upload_id = cursor.execute(query, file_upload_id)
            conn.commit()
            return file_upload_id
        except Exception as e:
            print(f"Error updating status of file in file_upload table following a rejected request: {e}")
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
    def __init__(self, id, workflow_id, level, name, is_responsibility_global, menu_item, role_name):
        self.id = id
        self.workflow_id = workflow_id
        self.level = level
        self.name = name
        self.is_responsibility_global = is_responsibility_global
        self.menu_item = menu_item
        self.role_name = role_name

    @staticmethod
    def get_workflow_breakdown_for_reconciliation_approval(workflow_id, is_workflow_level=True):
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            # Get workflow breakdown
            query = """SELECT wb.id, wb.workflow_id, wb.level, wb.name, 
                    wb.is_responsibility_global, wb.menu_item_id, r.name AS role_name 
                    FROM workflow_breakdown wb
                    JOIN role_workflow_breakdown rwb ON wb.id = rwb.workflow_breakdown_id
                    JOIN role r ON rwb.role_id = r.id
                    WHERE wb.workflow_id = ? AND wb.is_workflow_level = ?
                    ORDER BY wb.level ASC"""
            cursor.execute(query, (workflow_id, is_workflow_level))
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            workflows = [WorkflowBreakdown(row[0], row[1], row[2], row[3], row[4], row[5], row[6]) for row in result]

            return workflows
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()


class UserRole:
    def __init__(self, id, user_id, role_id, start_datetime, expiry_datetime):
        self.id = id
        self.user_id = user_id
        self.role_id = role_id
        self.start_datetime = start_datetime
        self.expiry_datetime = expiry_datetime


class ReconciliationApprovals:
    def __init__(self, id, file_upload_id, decision, approver_id, approver, level, comment, date_time):
        self.id = id
        self.file_upload_id = file_upload_id
        self.decision = decision
        self.approver_id = approver_id
        self.approver = approver
        self.level = level
        self.comment = comment
        self.date_time = date_time

    @staticmethod
    def insert_into_reconciliation_approvals(file_upload_id, decision, approver_id, level, comment):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        now = datetime.now()

        try:
            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM reconciliation_approvals")
            last_reconciliation_approvals_id = cursor.fetchone()[0]  # Fetch last batch_id

            # Set new batch_id
            last_reconciliation_approvals_id = (last_reconciliation_approvals_id + 1) \
                if last_reconciliation_approvals_id else 1

            cursor.execute(
                "INSERT INTO reconciliation_approvals (id, file_upload_id, decision, approver_id, level, comment, "
                "date_time)"
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (last_reconciliation_approvals_id, file_upload_id, decision, approver_id, level, comment, now),
            )
            conn.commit()
            return last_reconciliation_approvals_id
        except pyodbc.Error as e:
            print("Database insert error:", e)
            conn.rollback()
            return None
        finally:
            conn.close()

    @staticmethod
    def get_latest_reconciliation_approval_level(file_upload_id):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # check if User has a request pending submission
            cursor.execute("SELECT TOP 1 COALESCE(level, 0) FROM reconciliation_approvals WHERE file_upload_id = ? "
                           "ORDER BY date_time DESC;", file_upload_id)

            latest_approval_level = cursor.fetchone()[0]  # Fetch last batch_id
            return latest_approval_level
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_reconciliation_approval_levels_of_given_file(file_upload_id):
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            # Get the latest approval level for the given file
            query = """SELECT ra.level, CASE WHEN ra.decision = 1 
                    THEN 'Submitted' WHEN ra.decision = 2 THEN 'Approved' WHEN ra.decision = 3 THEN 'Rejected' 
                    ELSE 'Pending' END AS decision,
                    CONCAT(u.Fname, ' ', u.Mname, ' ', u.Sname) AS approver, ra.date_time 
                    FROM reconciliation_approvals ra
                    LEFT OUTER JOIN users u ON ra.approver_id = u.ID
                    WHERE ra.file_upload_id = ? ORDER BY ra.date_time
                    """
            cursor.execute(query, (file_upload_id,))  # Pass the parameter twice
            result = cursor.fetchall()  # Fetch results properly

            return result if result else []
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

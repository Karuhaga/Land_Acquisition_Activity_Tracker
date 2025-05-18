import pyodbc
from sqlalchemy import null

from BankReconciliation import app, login_manager
from flask_login import UserMixin
from flask_bcrypt import Bcrypt
from datetime import datetime
from BankReconciliation.database import get_db_connection
from BankReconciliation import mail  # Import mail from __init__.py
from flask_mail import Message


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
        self.roles = self.get_roles()

    def get_roles(self):
        """Fetch user roles from the database."""
        conn = get_db_connection()
        if conn is None:
            return []  # Return an empty list if the database connection fails

        cursor = conn.cursor()

        try:
            cursor.execute("SELECT r.name FROM role r INNER JOIN user_role ur ON r.id = ur.role_id WHERE ur.user_id = ?", (self.id,))
            return [row[0] for row in cursor.fetchall()]  # Return a list of role names
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

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

    @staticmethod
    def load_user(self, username):
        """Load user from database by user_id."""
        conn = get_db_connection()
        if conn is None:
            return None  # Return None if the database connection fails

        cursor = conn.cursor()

        try:
            cursor.execute("SELECT ID, Username, Fname, Mname, Sname, Password, Email FROM users WHERE Username = ?",
                           (username,))
            row = cursor.fetchone()
            if row:
                return User(row.id, row.username, row.fname, row.mname, row.sname, row.password, row.email) # Create a User object
            return None
        except Exception as e:
            print("Database error:", e)
            return None
        finally:
            cursor.close()
            conn.close()

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


class EmailHelper(UserMixin):
    def __init__(self):
        self.id = id

    @staticmethod
    def send_submitted_reconciliations_email(current_fname, next_approver_email, next_approver_fname, files):
        subject = "Bank Reconciliations Submitted for Approval"

        # Email body with Poppins font and inline styles
        body = f"""
        <html>
        <head>
            <link href="https:get_reconciliations_pending_approval_report/fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&display=swap" rel="stylesheet">
        </head>
        <body style="font-family: 'Poppins', sans-serif; margin: 20px; color: #333;">
            <p style="font-size: 14px;">Dear {next_approver_fname},</p>
            <p style="font-size: 14px;">{current_fname} has submitted reconciliation files that require your approval.</p>

            <p style="font-size: 14px; font-weight: bold; margin-top: 25px;">Submitted Reconciliations:</p>

            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%; font-size: 14px;">
                <thead>
                    <tr style="background-color: #f2f2f2;">
                        <th style="border: 1px solid #ddd; text-align: center; padding: 8px;">#</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">Bank Account ID</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">Year</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">Month</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">File Name</th>
                    </tr>
                </thead>
                <tbody>
        """

        for index, file in enumerate(files, start=1):
            body += f"""
                    <tr>
                        <td style="border: 1px solid #ddd; text-align: center; padding: 8px;">{index}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('bank_account_id')}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('year')}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('month')}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('file_name')}</td>
                    </tr>
            """

        body += """
                </tbody>
            </table>

            <p style="margin-top: 25px; margin-bottom: 25px;">
                🔗 <a href="http://127.0.0.1:5000/approve-reconciliations" 
                style="color: #4270a8; text-decoration: none; font-weight: 600;">Click here to review and approve</a>
            </p>

            <p style="font-size: 14px;">Your timely action is appreciated.</p>

            <p style="font-size: 14px;">
                <strong>Best Regards,</strong><br>
                Bank Reconciliation Tracker<br>
                📧 support@yourcompany.com | ☎️ +256 [Your Contact Number]
            </p>
        </body>
        </html>
        """

        try:
            msg = Message(subject, recipients=[next_approver_email])
            msg.html = body  # Set HTML content
            mail.send(msg)
            print(f"Approval email sent to {next_approver_email}")
        except Exception as e:
            print(f"Error sending email: {e}")

    @staticmethod
    def send_email_notification_to_next_approver(current_fname, next_approver_email, next_approver_fname, files):
        subject = "Bank Reconciliations Approval"

        # Email body with Poppins font and inline styles
        body = f"""
        <html>
        <head>
            <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&display=swap" rel="stylesheet">
        </head>
        <body style="font-family: 'Poppins', sans-serif; margin: 20px; color: #333;">
            <p style="font-size: 14px;">Dear {next_approver_fname},</p>
            <p style="font-size: 14px;">Please be notified that the following reconciliations have been forwarded to you for approval:</p>

            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%; font-size: 14px;">
                <thead>
                    <tr style="background-color: #f2f2f2;">
                        <th style="border: 1px solid #ddd; text-align: center; padding: 8px;">#</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">Bank Account ID</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">Year</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">Month</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">File Name</th>
                    </tr>
                </thead>
                <tbody>
        """

        for index, file in enumerate(files, start=1):
            body += f"""
                    <tr>
                        <td style="border: 1px solid #ddd; text-align: center; padding: 8px;">{index}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('bank_account_id')}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('year')}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('month')}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('file_name')}</td>
                    </tr>
            """

        body += """
                </tbody>
            </table>

            <p style="margin-top: 25px; margin-bottom: 25px;">
                🔗 <a href="http://127.0.0.1:5000/approve-reconciliations" 
                style="color: #4270a8; text-decoration: none; font-weight: 600;">Click here to review and approve</a>
            </p>

            <p style="font-size: 14px;">Your timely action is appreciated.</p>

            <p style="font-size: 14px;">
                <strong>Best Regards,</strong><br>
                Bank Reconciliation Tracker<br>
                📧 support@yourcompany.com | ☎️ +256 [Your Contact Number]
            </p>
        </body>
        </html>
        """

        try:
            msg = Message(subject, recipients=[next_approver_email])
            msg.html = body  # Set HTML content
            mail.send(msg)
            print(f"Approval email sent to {next_approver_email}")
        except Exception as e:
            print(f"Error sending email: {e}")

    @staticmethod
    def send_approval_summary_emails(current_fname, initiator_approver_email, initiator_approver_fname, files, action):
        action_for_email_subject = "Approval" if action == "approved" else "Rejection"
        subject = "Bank Reconciliation(s) " + action_for_email_subject

        # Email body with Poppins font and inline styles
        body = f"""
        <html>
        <head>
            <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600&display=swap" rel="stylesheet">
        </head>
        <body style="font-family: 'Poppins', sans-serif; margin: 20px; color: #333;">
            <p style="font-size: 14px;">Dear {initiator_approver_fname},</p>
            <p style="font-size: 14px;">The following reconciliation(s) that you submitted have been {action} by {current_fname}.</p>

            <table border="1" cellspacing="0" cellpadding="8" style="border-collapse: collapse; width: 100%; font-size: 14px;">
                <thead>
                    <tr style="background-color: #f2f2f2;">
                        <th style="border: 1px solid #ddd; text-align: center; padding: 8px;">#</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">Bank Account ID</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">Year</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">Month</th>
                        <th style="border: 1px solid #ddd; text-align: left; padding: 8px;">File Name</th>
                    </tr>
                </thead>
                <tbody>
        """

        for index, file in enumerate(files, start=1):
            body += f"""
                    <tr>
                        <td style="border: 1px solid #ddd; text-align: center; padding: 8px;">{index}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('bank_account_id')}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('year')}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('month')}</td>
                        <td style="border: 1px solid #ddd; padding: 8px;">{file.get('file_name')}</td>
                    </tr>
            """

        body += """
                </tbody>
            </table>

            <p style="margin-top: 25px; margin-bottom: 25px;">
                🔗 <a href="http://127.0.0.1:5000/submitted-reconciliations" 
                style="color: #4270a8; text-decoration: none; font-weight: 600;">Click here to view the approved reconciliations</a>
            </p>

            <p style="font-size: 14px;">
                <strong>Best Regards,</strong><br>
                Bank Reconciliation Tracker<br>
                📧 support@yourcompany.com | ☎️ +256 [Your Contact Number]
            </p>
        </body>
        </html>
        """

        try:
            msg = Message(subject, recipients=[initiator_approver_email])
            msg.html = body  # Set HTML content
            mail.send(msg)
            print(f"Email of approval sent to {initiator_approver_email}")
        except Exception as e:
            print(f"Error sending email: {e}")


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

    @staticmethod
    def get_reconciliation_initiator_user_id(bank_account_id, year, month, file_name):
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
            cursor.execute("SELECT fub.user_id FROM file_upload fu LEFT OUTER JOIN file_upload_batch fub ON "
                           "fu.batch_id = fub.id WHERE fu.bank_account_id = ? AND fu.year = ? AND fu.month = ? AND "
                           "fu.file_name = ?", bank_account_id, year, month_int, file_name)

            id_of_initiator = cursor.fetchone()[0]  # Fetch last batch_id

            return id_of_initiator
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_reconciliation_initiator_email_and_fname(user_id):
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            query = """      
                SELECT DISTINCT Fname, Email FROM users WHERE id = ? ;
            """

            cursor.execute(query, (user_id,))
            result = cursor.fetchall()

            # Return a list of dictionaries instead of trying to map to FileUpload
            return [{"Fname": row[0], "Email": row[1]} for row in result]

        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()


class FileUpload:
    def __init__(self, id=None, bank_account=None, year=None, month=None, batch_id=None, file_name=None, date_time=None, approve_as=None, responsible_users=None, next_approver=None, status=None):
        self.id = id
        self.bank_account = bank_account
        self.year = year
        self.month = month
        self.batch_id = batch_id
        self.file_name = file_name
        self.date_time = date_time
        self.approve_as = approve_as
        self.responsible_users = responsible_users
        self.next_approver = next_approver
        self.status = status

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
    def get_batch_id(bank_account_id, year, month, file_name):
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
            cursor.execute("SELECT COALESCE(MAX(batch_id), 0) FROM file_upload WHERE bank_account_id = ? AND "
                           "year = ? AND month = ? AND file_name = ?", bank_account_id, year, month_int, file_name)

            id_of_file_upload = cursor.fetchone()[0]  # Fetch last batch_id

            return id_of_file_upload
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
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
            query = """
                        DECLARE @workflow_id INT = 1; -- workflow_id from workflow table
                        DECLARE @is_workflow_level INT = 1;
                        
                        SELECT c.name AS bank_account, b.year, DATENAME(month, DATEADD(month, b.month - 1, 0)) AS month,
                          (
                            SELECT TOP 1
                              CASE 
                                WHEN ra.decision = 1 THEN 'Submitted by'
                                WHEN ra.decision = 2 THEN 'Approved by'
                                WHEN ra.decision = 3 THEN 'Rejected by'
                                ELSE 'Unknown'
                              END + ' ' + r.name
                            FROM reconciliation_approvals ra
                            LEFT JOIN workflow_breakdown wb ON ra.level = wb.level 
                            LEFT JOIN role_workflow_breakdown rwb ON wb.id = rwb.workflow_breakdown_id
                            LEFT JOIN role r ON rwb.role_id = r.id
                            WHERE 
                              ra.file_upload_id = b.id AND  -- correlate to outer query
                              wb.workflow_id = @workflow_id AND 
                              wb.is_workflow_level = @is_workflow_level
                            ORDER BY ra.id DESC  -- optional: choose latest approval
                          ) AS status,
                          b.file_name,
                          FORMAT(b.creation_datetime, 'yyyy-MM-dd HH:mm:ss') AS date_time
                        FROM 
                          file_upload_batch a
                        LEFT JOIN file_upload b ON a.id = b.batch_id
                        LEFT JOIN bank_account c ON b.bank_account_id = c.id
                        WHERE 
                          a.user_id = ? AND 
                          b.submission_status != 0 AND 
                          b.removed_by_user_on_upload_page = 0
                        ORDER BY 
                          c.name;
            """
            cursor.execute(query, (user_id,))
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            reconciliations = [
                FileUpload(bank_account=row.bank_account, year=row.year, month=row.month, status=row.status,
                           file_name=row.file_name, date_time=row.date_time)
                for row in result
            ]
            return reconciliations
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_reconciliations_pending_approval_report():
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            # Fetch submitted reconciliations
            query = """
                    DECLARE @workflow_id INT = 1; -- workflow_id from workflow table
                    DECLARE @is_workflow_level INT = 1;
                    
                    SELECT c.name as bank_account, b.year, (select DateName(month, DateAdd(month, b.month, 
                    0) - 1)) as month, b.file_name, FORMAT(b.creation_datetime, 'yyyy-MM-dd HH:mm:ss') AS 
                    date_time, (SELECT r.name FROM role r LEFT OUTER JOIN role_workflow_breakdown rwb ON r.id = 
                    rwb.role_id LEFT OUTER JOIN workflow_breakdown wb ON rwb.workflow_breakdown_id = wb.id WHERE 
                    wb.workflow_id = @workflow_id AND wb.is_workflow_level = @is_workflow_level AND wb.level = 
                    b.submission_status + 1) as next_approver FROM file_upload_batch a LEFT OUTER JOIN file_upload b ON 
                    a.id = b.batch_id LEFT OUTER JOIN bank_account c ON b.bank_account_id = c.id WHERE 
                    b.submission_status != 0 AND b.submission_status < (SELECT COALESCE(MAX(level), 0) FROM workflow wf 
                    LEFT OUTER JOIN workflow_breakdown wb ON wf.id = wb.workflow_id WHERE wf.id = @workflow_id) AND 
                    b.removed_by_user_on_upload_page = 0 ORDER BY c.name, b.year, b.month ASC;
                """
            cursor.execute(query, )
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            reconciliations = [
                FileUpload(bank_account=row.bank_account, year=row.year, month=row.month,
                           file_name=row.file_name, date_time=row.date_time, next_approver=row.next_approver)
                for row in result
            ]
            return reconciliations
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_fully_approved_reconciliations_report():
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            # Fetch submitted reconciliations
            query = """
                        DECLARE @workflow_id INT = 1; -- workflow_id from workflow table
                        DECLARE @is_workflow_level INT = 1;
                        
                        SELECT c.name as bank_account, b.year, (select DateName(month, DateAdd(month, b.month, 
                        0) - 1)) as month, b.file_name, ( SELECT TOP 1 CASE WHEN ra.decision = 1 THEN 'Submitted by' 
                        WHEN ra.decision = 2 THEN 'Approved by' WHEN ra.decision = 3 THEN 'Rejected by' ELSE 
                        'Unknown' END + ' ' + r.name FROM reconciliation_approvals ra LEFT JOIN workflow_breakdown wb 
                        ON ra.level = wb.level LEFT JOIN role_workflow_breakdown rwb ON wb.id = 
                        rwb.workflow_breakdown_id LEFT JOIN role r ON rwb.role_id = r.id WHERE ra.file_upload_id = 
                        b.id AND wb.workflow_id = @workflow_id AND wb.is_workflow_level = @is_workflow_level ORDER BY 
                        ra.id DESC ) AS status, FORMAT((SELECT MAX(ra.date_time) FROM reconciliation_approvals ra 
                        WHERE file_upload_id = b.id), 'yyyy-MM-dd HH:mm:ss') AS date_time FROM file_upload_batch a 
                        LEFT OUTER JOIN file_upload b ON a.id = b.batch_id LEFT OUTER JOIN bank_account c ON 
                        b.bank_account_id = c.id WHERE b.submission_status != 0 AND b.submission_status = (SELECT 
                        COALESCE(MAX( wb.level), 0) FROM workflow wf LEFT OUTER JOIN workflow_breakdown wb ON wf.id = 
                        wb.workflow_id WHERE wf.id = @workflow_id) AND b.removed_by_user_on_upload_page = 0 ORDER BY 
                        c.name, b.year, b.month ASC;
                    """
            cursor.execute(query, )
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            reconciliations = [
                FileUpload(bank_account=row.bank_account, year=row.year, month=row.month,
                           file_name=row.file_name, status=row.status, date_time=row.date_time)
                for row in result
            ]
            return reconciliations
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_all_submitted_reconciliations():
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            # Fetch submitted reconciliations
            query = """
                        DECLARE @workflow_id INT = 1; -- workflow_id from workflow table
                        DECLARE @is_workflow_level INT = 1;
                        
                        SELECT c.name as bank_account, b.year, ( select DateName( month, DateAdd(month, 
                        b.month, 0) -1 ) ) as month, ( SELECT TOP 1 CASE WHEN ra.decision = 1 THEN 'Submitted by' 
                        WHEN ra.decision = 2 THEN 'Approved by' WHEN ra.decision = 3 THEN 'Rejected by' ELSE 
                        'Unknown' END + ' ' + r.name FROM reconciliation_approvals ra LEFT JOIN workflow_breakdown wb 
                        ON ra.level = wb.level LEFT JOIN role_workflow_breakdown rwb ON wb.id = 
                        rwb.workflow_breakdown_id LEFT JOIN role r ON rwb.role_id = r.id WHERE ra.file_upload_id = 
                        b.id AND wb.workflow_id = @workflow_id AND wb.is_workflow_level = 
                        @is_workflow_level ORDER BY ra.id DESC) AS status, 
                        b.file_name, FORMAT( b.creation_datetime, 'yyyy-MM-dd HH:mm:ss' ) AS date_time FROM 
                        file_upload_batch a LEFT OUTER JOIN file_upload b ON a.id = b.batch_id LEFT OUTER JOIN 
                        bank_account c ON b.bank_account_id = c.id LEFT OUTER JOIN users d ON a.user_id = d.ID WHERE 
                        b.submission_status != 0 AND b.removed_by_user_on_upload_page = 0 ORDER BY c.name, b.year, 
                        b.month
                    """
            cursor.execute(query, )
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            reconciliations = [
                FileUpload(bank_account=row.bank_account, year=row.year, month=row.month, status=row.status,
                           file_name=row.file_name, date_time=row.date_time)
                for row in result
            ]
            return reconciliations
        except Exception as e:
            print("Database error:", e)
            return []
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def get_reconciliations_pending_submission():
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            # Fetch submitted reconciliations
            query = """
                    WITH MonthList AS (
                        SELECT 
                            ba.id AS bank_account_id,
                            ba.name AS bank_account,
                            DATEFROMPARTS(YEAR(ba.creation_date), MONTH(ba.creation_date), 1) AS start_month
                        FROM 
                            bank_account ba
                    ), AllMonths AS (
                        SELECT 
                            ml.bank_account_id,
                            ml.bank_account,
                            ml.start_month AS recon_month
                        FROM 
                            MonthList ml
                        
                        UNION ALL
                        
                        SELECT 
                            am.bank_account_id,
                            am.bank_account,
                            DATEADD(MONTH, 1, am.recon_month)
                        FROM 
                            AllMonths am
                        WHERE 
                            am.recon_month < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
                    )
                    SELECT 
                        c.name AS bank_account,
                        YEAR(am.recon_month) AS year,
                        DATENAME(month, am.recon_month) AS month, -- changed here
                        STRING_AGG(
                            LTRIM(RTRIM(
                                COALESCE(u.Fname, '') + 
                                CASE WHEN u.Mname IS NOT NULL AND u.Mname <> '' THEN ' ' + u.Mname ELSE '' END + 
                                CASE WHEN u.Sname IS NOT NULL AND u.Sname <> '' THEN ' ' + u.Sname ELSE '' END
                            )),
                            ', '
                        ) AS responsible_users
                    FROM 
                        AllMonths am
                    LEFT JOIN 
                        file_upload f ON f.bank_account_id = am.bank_account_id 
                                       AND f.year = YEAR(am.recon_month)
                                       AND f.month = MONTH(am.recon_month)
                                       AND f.removed_by_user_on_upload_page = 0
                    INNER JOIN 
                        bank_account c ON am.bank_account_id = c.id
                    LEFT JOIN 
                        bank_account_responsible_user bru ON bru.bank_account_id = c.id
                    LEFT JOIN 
                        users u ON bru.user_id = u.ID
                    WHERE 
                        f.id IS NULL -- Means missing reconciliation
                        AND am.recon_month < DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) --EXCLUDE current month
                    GROUP BY 
                        c.name,
                        YEAR(am.recon_month),
                        DATENAME(month, am.recon_month) -- also group by the new month name
                    ORDER BY 
                        c.name, year, 
                        MIN(MONTH(am.recon_month)) -- optional: ensure months are sorted properly
                    OPTION (MAXRECURSION 1000);
                        """
            cursor.execute(query, )
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            reconciliations = [
                FileUpload(
                    bank_account=row.bank_account,
                    year=row.year,
                    month=row.month,
                    responsible_users=row.responsible_users
                )
                for row in result
            ]
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
            query = """
                        DECLARE @workflow_id INT = 1; 
                        DECLARE @is_workflow_level INT = 1;
                        
                        SELECT
                            b.name AS bank_account,
                            a.year,
                            DATENAME(month, DATEADD(month, a.month - 1, 0)) AS month,                                                
                            (
                                SELECT TOP 1
                                    CASE 
                                        WHEN ra2.decision = 1 THEN 'Submitted by'
                                        WHEN ra2.decision = 2 THEN 'Approved by'
                                        WHEN ra2.decision = 3 THEN 'Rejected by'
                                        ELSE 'Unknown'
                                    END + ' ' + r.name
                                FROM reconciliation_approvals ra2
                                LEFT JOIN workflow_breakdown wb ON ra2.level = wb.level AND wb.workflow_id = @workflow_id
                                LEFT JOIN role_workflow_breakdown rwb ON wb.id = rwb.workflow_breakdown_id
                                LEFT JOIN role r ON rwb.role_id = r.id
                                WHERE ra2.file_upload_id = a.id
                                ORDER BY ra2.id DESC
                            ) AS status, a.file_name,
                            FORMAT(a.creation_datetime, 'yyyy-MM-dd HH:mm:ss') AS date_time
                        FROM file_upload a
                        LEFT JOIN bank_account b ON a.bank_account_id = b.id
                        WHERE EXISTS (
                            SELECT 1 FROM reconciliation_approvals ra
                            WHERE ra.file_upload_id = a.id
                            AND ra.approver_id = ? 
                            AND ra.decision IN (2, 3) -- Only Approved or Rejected
                        )
                        ORDER BY b.name, a.year, a.month;
                    """
            cursor.execute(query, (user_id,))
            result = cursor.fetchall()

            # Convert query result into list of Reconciliation objects
            reconciliations = [
                FileUpload(bank_account=row.bank_account, year=row.year, month=row.month, file_name=row.file_name,
                           status=row.status, date_time=row.date_time)
                for row in result
            ]
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
                    AND f.submission_status != 0 AND f.submission_status IN 
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
                        AND f.submission_status != 0 AND f.submission_status IN 
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
    def get_reconciliations_pending_approval_count(user_id):
        conn = get_db_connection()
        if conn is None:
            return 0  # Return 0 if the database connection fails

        cursor = conn.cursor()

        try:
            query = """
                DECLARE @logged_in_user_id INT = ?;

                WITH GlobalFiles AS (
                    SELECT f.id
                    FROM file_upload f
                    JOIN reconciliation_approvals ra ON f.id = ra.file_upload_id
                    JOIN file_upload_batch fub ON f.batch_id = fub.id
                    JOIN users u ON fub.user_id = u.ID
                    JOIN bank_account ba ON f.bank_account_id = ba.id
                    JOIN bank b ON ba.bank_id = b.id
                    JOIN user_role ur ON u.ID = ur.user_id
                    JOIN role r ON ur.role_id = r.id
                    WHERE ra.approver_id IN (
                        SELECT DISTINCT a.ID
                        FROM users a
                        JOIN organisation_unit_tier b ON a.organisation_unit_tier_id = b.id
                        WHERE b.parent_org_unit_tier_id IN (
                            SELECT d.organisation_unit_tier_id
                            FROM users d
                            WHERE d.ID = @logged_in_user_id
                        )
                    )
                    AND f.submission_status != 0
                    AND f.submission_status IN (
                        SELECT DISTINCT a.level - 1
                        FROM workflow_breakdown a
                        JOIN role_workflow_breakdown b ON a.id = b.workflow_breakdown_id
                        JOIN role c ON b.role_id = c.id
                        JOIN user_role d ON c.id = d.role_id
                        WHERE a.is_responsibility_global = 1
                        AND d.user_id = @logged_in_user_id
                    )
                ),
                OrgBasedFiles AS (
                    SELECT f.id
                    FROM file_upload f
                    JOIN reconciliation_approvals ra ON f.id = ra.file_upload_id
                    JOIN file_upload_batch fub ON f.batch_id = fub.id
                    JOIN users u ON fub.user_id = u.ID
                    JOIN bank_account ba ON f.bank_account_id = ba.id
                    JOIN bank b ON ba.bank_id = b.id
                    JOIN user_role ur ON u.ID = ur.user_id
                    JOIN role r ON ur.role_id = r.id
                    WHERE ra.approver_id IN (
                        SELECT DISTINCT a.ID
                        FROM users a
                        JOIN organisation_unit b ON a.organisation_unit_id = b.id
                        WHERE b.parent_org_unit_id IN (
                            SELECT d.organisation_unit_id
                            FROM users d
                            WHERE d.ID = @logged_in_user_id
                        )
                    )
                    AND f.submission_status != 0
                    AND f.submission_status IN (
                        SELECT DISTINCT a.level - 1
                        FROM workflow_breakdown a
                        JOIN role_workflow_breakdown b ON a.id = b.workflow_breakdown_id
                        JOIN role c ON b.role_id = c.id
                        JOIN user_role d ON c.id = d.role_id
                        WHERE a.is_responsibility_global = 0
                        AND d.user_id = @logged_in_user_id
                    )
                )

                SELECT 
                    (SELECT COUNT(*) FROM GlobalFiles) + 
                    (SELECT COUNT(*) FROM OrgBasedFiles) AS TotalPendingApprovals;
            """
            cursor.execute(query, [user_id])
            pending_approvals_count = cursor.fetchone()[0]
            return pending_approvals_count if pending_approvals_count is not None else 0
        except Exception as e:
            print("Database error:", e)
            return 0
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
    def get_submission_status_of_reconciliation(file_upload_id):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            # Check if a record with the given bank_account, year, and month exists
            cursor.execute(
                "SELECT submission_status FROM file_upload WHERE id = ?", file_upload_id
            )
            result = cursor.fetchone()[0]
            return result
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

    @staticmethod
    def get_next_approver_fname_email(user_id, max_submission_status):
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            query = """
                WITH ParentOrgUnits AS (
                    SELECT DISTINCT b.parent_org_unit_id 
                    FROM users a
                    JOIN organisation_unit b ON a.organisation_unit_id = b.id 
                    WHERE a.ID = ?
                ),
                ParentOrgUnitTiers AS (
                    SELECT DISTINCT b.parent_org_unit_tier_id 
                    FROM users a
                    JOIN organisation_unit_tier b ON a.organisation_unit_tier_id = b.id 
                    WHERE a.ID = ?
                ),
                GlobalApprovers AS (
                    SELECT DISTINCT u.Fname, u.Email
                    FROM users u
                    JOIN user_role ur ON u.id = ur.user_id
                    JOIN role r ON ur.role_id = r.id
                    JOIN role_workflow_breakdown rwb ON r.id = rwb.role_id
                    JOIN workflow_breakdown wb ON rwb.workflow_breakdown_id = wb.id
                    WHERE wb.is_workflow_level = 1
                    AND wb.level = ?
                    AND wb.is_responsibility_global = 1
                    AND u.ID IN (
                        SELECT DISTINCT a.ID
                        FROM users a
                        JOIN organisation_unit_tier b ON a.organisation_unit_tier_id = b.id
                        WHERE b.id IN (SELECT parent_org_unit_tier_id FROM ParentOrgUnitTiers)
                    )
                ),
                OrgBasedApprovers AS (
                    SELECT DISTINCT u.Fname, u.Email
                    FROM users u
                    JOIN user_role ur ON u.id = ur.user_id
                    JOIN role r ON ur.role_id = r.id
                    JOIN role_workflow_breakdown rwb ON r.id = rwb.role_id
                    JOIN workflow_breakdown wb ON rwb.workflow_breakdown_id = wb.id
                    WHERE wb.is_workflow_level = 1
                    AND wb.level = ?
                    AND wb.is_responsibility_global = 0
                    AND u.ID IN (
                        SELECT DISTINCT a.ID
                        FROM users a
                        JOIN organisation_unit b ON a.organisation_unit_id = b.id
                        WHERE b.id IN (SELECT parent_org_unit_id FROM ParentOrgUnits)
                    )
                )

                SELECT DISTINCT Fname, Email
                FROM (
                    SELECT Fname, Email FROM GlobalApprovers
                    UNION
                    SELECT Fname, Email FROM OrgBasedApprovers
                ) AS Approvers
                ORDER BY Fname ASC;
            """

            cursor.execute(query, (user_id, user_id, max_submission_status + 1, max_submission_status + 1))
            result = cursor.fetchall()

            # Return a list of dictionaries instead of trying to map to FileUpload
            return [{"Fname": row[0], "Email": row[1]} for row in result]

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


class BankAccount:
    def __init__(self, id, name, bank_id, currency_id, strategic_business_unit_id):
        self.id = id
        self.name = name
        self.bank_id = bank_id
        self.currency_id = currency_id
        self.strategic_business_unit_id = strategic_business_unit_id

    @staticmethod
    def get_bank_accounts_for_dropdown_menu(user_id):
        conn = get_db_connection()
        if conn is None:
            return []  # Return empty list if the database connection fails

        cursor = conn.cursor()

        try:
            # Fetch submitted reconciliations
            query = """SELECT ba.id, ba.name, ba.bank_id, ba.currency_id, ba.strategic_business_unit_id FROM 
            bank_account ba LEFT OUTER JOIN bank_account_responsible_user baru ON ba.id = baru.bank_account_id LEFT 
            OUTER JOIN users u ON baru.user_id = u.ID WHERE u.ID = ? ORDER BY ba.name"""
            cursor.execute(query, (user_id,))
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
            query = """
                        SELECT ra.level, CASE WHEN ra.decision = 1 
                        THEN 'Submitted' WHEN ra.decision = 2 THEN 'Approved' WHEN ra.decision = 3 THEN 'Rejected' 
                        ELSE 'Pending' END AS decision,
                        CONCAT(u.Fname, ' ', u.Mname, ' ', u.Sname) AS approver, ra.date_time, ra.comment 
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


class Audit:
    def __init__(self, user_id, action, details, date_time, ip_address):
        self.id = id
        self.user_id = user_id
        self.action = action
        self.details = details
        self.date_time = date_time
        self.ip_address = ip_address

    @staticmethod
    def log_audit_trail(user_id, action, details="", ip_address=None):
        conn = get_db_connection()
        if conn is None:
            return None  # Handle database connection failure

        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO audit_trail (user_id, action, details, timestamp, ip_address)
                VALUES (?, ?, ?, ?, ?)
            """, (
                user_id,
                action,
                details,
                datetime.now(),
                ip_address
            ))
            conn.commit()
            return action
        except Exception as e:
            print(f"Error while updating audit trail: {e}")
        finally:
            cursor.close()
            conn.close()

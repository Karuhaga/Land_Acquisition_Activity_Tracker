import os
from flask import Flask
from flask_bcrypt import Bcrypt
from flask_login import LoginManager, current_user
from BankReconciliation.database import get_db_connection


app = Flask(__name__)
app.config['SECRET_KEY'] = '4f4726d4610186d9cedfd91b'

# Uploading Files
UPLOAD_FOLDER = os.path.join(os.getcwd(), 'static', 'uploads')  # Use absolute path
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'pdf', 'txt', 'xls', 'xlsx'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)  # Ensure folder exists


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


# Flask-Login setup
bcrypt = Bcrypt(app)
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login_page"
login_manager.login_message_category = "info"
login_manager.session_protection = "strong"  # Set to 'strong' or 'None' if needed
app.app_context().push()

# Import routes after app creation to avoid circular imports
from BankReconciliation import routes


@app.context_processor
def inject_menu_items():
    """Dynamically inject menu items based on user's roles."""
    if not current_user.is_authenticated:
        return {'menu_items': []}

    conn = get_db_connection()
    if conn is None:
        return {'menu_items': []}  # No menu items if the DB connection fails

    cursor = conn.cursor()

    # Get the role IDs assigned to the current user
    cursor.execute("SELECT role_id FROM user_role WHERE user_id = ?", (current_user.id,))
    user_roles = [row[0] for row in cursor.fetchall()]

    if not user_roles:  # If user has no roles, return an empty menu
        conn.close()
        return {'menu_items': []}

    # Fetch workflows assigned to these roles
    cursor.execute("""
        SELECT mi.name
        FROM workflow_breakdown wb
        JOIN menu_item mi ON wb.menu_item_id = mi.id
        WHERE wb.responsible_role_id IN ({})
    """.format(",".join("?" * len(user_roles))), tuple(user_roles))

    menu_items = [row[0] for row in cursor.fetchall()]
    conn.close()

    return {'menu_items': menu_items}

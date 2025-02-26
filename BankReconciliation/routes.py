from BankReconciliation import app, os, allowed_file
from BankReconciliation.models import User, FileUploadBatch, FileUpload, FileDelete, BankAccount
from BankReconciliation.forms import LoginForm
from flask import render_template, redirect, url_for, flash, request, jsonify, session
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.utils import secure_filename
from datetime import datetime


@app.route('/', methods=['GET', 'POST'])
@app.route('/login', methods=['GET', 'POST'])
def login_page():
    form = LoginForm()

    if form.validate_on_submit():
        attempted_user = User.get_by_username(form.username.data)
        if attempted_user and attempted_user.check_password(form.password.data):
            session.permanent = True  # Persist session
            login_user(attempted_user)
            flash(f'Success! You logged in as: {attempted_user.username}', category='success')
            return redirect(url_for('dashboard_page'))
        else:
            flash('Username and/ or Password is incorrect. Please try again!', category='danger')

    return render_template('login.html', form=form)


@app.route('/dashboard', methods=['GET', 'POST'])
@login_required
def dashboard_page():
    if not current_user.is_authenticated:
        return redirect(url_for("login_page"))  # Redirect if user is not authenticated

    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return render_template('dashboard.html')
    return render_template('dashboard.html')


@app.route('/submit-reconciliation', methods=['GET', 'POST'])
@login_required
def submit_reconciliation_page():
    # Fetch bank accounts from the database
    bank_accounts = BankAccount.get_bank_accounts_for_dropdown_menu()
    if bank_accounts is None:
        return jsonify({"error": "No bank account found in the database"}), 500

    #
    num_of_unsubmitted_requests = FileUploadBatch.check_batch_submission_status(current_user.id)
    if num_of_unsubmitted_requests is None:
        return jsonify({"error": "Database error while creating batch file upload"}), 500

    if num_of_unsubmitted_requests != 0:
        batch = FileUploadBatch.get_latest_batch_pending_submission_by_user(current_user.id)
        uploaded_files = FileUpload.get_uploaded_pending_submission_files_by_user(batch)
        if uploaded_files is None:
            return jsonify({"error": "Database error while fetching uploaded files"}), 500
    else:
        uploaded_files = []

    return render_template('submit_reconciliation.html',
                           num_of_unsubmitted_requests=num_of_unsubmitted_requests,
                           uploaded_files=uploaded_files, bank_accounts=bank_accounts)


@app.route('/upload', methods=['POST'])
@login_required  # Optional: Remove if not needed
def upload_files():
    if 'files' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    files = request.files.getlist('files')  # Get list of uploaded files
    uploaded_files = []

    # Initialise the new_batch_id variable
    num_of_pending_batches = FileUploadBatch.get_latest_batch_pending_submission_by_user(current_user.id)
    if num_of_pending_batches == 0:
        new_batch_id = FileUploadBatch.allocate_batch_id()
    else:
        new_batch_id = FileUploadBatch.get_latest_batch_pending_submission_by_user(current_user.id)
    # Insert into file_upload_batch database table
    new_batch_row = FileUploadBatch.insert_into_file_upload_batch(current_user.id, new_batch_id)
    if new_batch_row is None:
        return jsonify({"error": "Database error while creating batch file upload"}), 500

    for i, file in enumerate(files):
        if file.filename == '':
            continue
        if file and allowed_file(file.filename):
            bank_account = request.form.getlist("bank_account")[i]
            year = request.form.getlist("year")[i]
            month = request.form.getlist("month")[i]

            # Generate timestamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

            # Secure filename and prepend timestamp
            filename = secure_filename(file.filename)
            new_filename = f"{timestamp}_{current_user.id}_{filename}"  # Add timestamp at the beginning

            # Save file with the new name
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], new_filename)
            file.save(file_path)

            # Insert into database
            new_file_id = FileUpload.insert_into_file_upload(new_batch_id, new_filename, bank_account, year, month)
            if new_file_id is None:
                return jsonify({"error": "Database error while adding uploaded file"}), 500

            # Fetch bank account name from DB
            bank_account = BankAccount.get_bank_account_name_by_id(bank_account)
            if not bank_account:
                return jsonify({"error": "Invalid bank account selected"}), 400

            # Convert month value to month name
            month_names = {
                "01": "January", "02": "February", "03": "March", "04": "April",
                "05": "May", "06": "June", "07": "July", "08": "August",
                "09": "September", "10": "October", "11": "November", "12": "December"
            }
            month_name = month_names.get(month, "Unknown")

            # Append full metadata for JavaScript processing
            uploaded_files.append({
                "file_name": new_filename,
                "bank_account": bank_account,
                "year": year,
                "month": month_name
            })

    if uploaded_files:
        return jsonify({"message": "Files uploaded successfully!", "files": uploaded_files}), 200
    return jsonify({"error": "No valid files uploaded"}), 400


@app.route('/delete-file', methods=['POST'])
@login_required
def delete_file():
    data = request.get_json()
    filename = data.get('filename').strip()

    if not filename:
        return jsonify({"error": "Filename not provided"}), 400

    # Update the file_upload table, set removed_by_user_on_upload_page column to 1 for corresponding file name of
    # file removed by user
    new_uploaded_file_name = FileDelete.remove_file_by_user_on_upload_page(filename)
    if new_uploaded_file_name is None:
        return jsonify({"error": "Database error while updating status of file removed by User"}), 500

    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    # Remove file from the server
    if os.path.exists(file_path):
        os.remove(file_path)
        return jsonify({"message": f"File '{filename}' deleted successfully!"}), 200
    else:
        return jsonify({"error": "File not found"}), 404


@app.route('/get-uploaded-files', methods=['GET'])
@login_required
def get_uploaded_files():
    # Fetch uploaded files for the current user
    uploaded_files = FileUpload.get_uploaded_pending_submission_files_by_user(current_user.id)

    if uploaded_files is None:
        return jsonify({"error": "Database error while fetching uploaded files"}), 500

    return jsonify({"files": uploaded_files}), 200


@app.route('/submitted-reconciliations', methods=['GET', 'POST'])
@login_required
def submitted_reconciliations_page():
    reconciliations = FileUpload.get_submitted_reconciliations(current_user.id)
    return render_template('submitted_reconciliations.html', reconciliations=reconciliations)


@app.route('/logout')
def logout_page():
    logout_user()
    session.clear()  # Clear session to prevent stored data
    flash("You have been logged out!", category='info')
    return redirect(url_for("login_page"))

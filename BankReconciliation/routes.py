from BankReconciliation import app, os, allowed_file
from BankReconciliation.models import User, FileUploadBatch, FileUpload, FileDelete, BankAccount
from BankReconciliation.forms import LoginForm
from flask import render_template, redirect, url_for, flash, request, jsonify, session, send_from_directory, abort
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.utils import secure_filename
from datetime import datetime
from BankReconciliation.rbac import role_required


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


@app.route('/submit-reconciliations', methods=['GET', 'POST'])
@login_required
@role_required(1)
def submit_reconciliations_page():
    # Fetch bank accounts from the database
    bank_accounts = BankAccount.get_bank_accounts_for_dropdown_menu()
    if bank_accounts is None:
        return jsonify({"error": "No bank account found in the database"}), 500

    num_of_unsubmitted_files = FileUpload.unsubmitted_files_num(current_user.id)
    if num_of_unsubmitted_files is None:
        return jsonify({"error": "Database error while fetching unsubmitted files"}), 500

    uploaded_files = FileUpload.get_uploaded_pending_submission_files_by_user(current_user.id)
    if uploaded_files is None:
        return jsonify({"error": "Database error while fetching uploaded files"}), 500
    return render_template('submit_reconciliations.html',
                           num_of_unsubmitted_files=num_of_unsubmitted_files,
                           uploaded_files=uploaded_files, bank_accounts=bank_accounts)


@app.route('/upload', methods=['POST'])
@login_required
def upload_files():
    if 'files' not in request.files:
        return jsonify({"error": "No file selected for upload"}), 400

    files = request.files.getlist('files')
    uploaded_files = []
    duplicate_rows = []  # Store rows that have duplicates

    num_of_pending_batches = FileUploadBatch.get_latest_batch_pending_submission_by_user(current_user.id)
    if num_of_pending_batches == 0:
        new_batch_id = FileUploadBatch.allocate_batch_id()
        new_batch_row = FileUploadBatch.insert_into_file_upload_batch(current_user.id, new_batch_id)
        if new_batch_row is None:
            return jsonify({"error": "Database error while creating batch file upload"}), 500
    else:
        new_batch_id = FileUploadBatch.get_latest_batch_pending_submission_by_user(current_user.id)

    for i, file in enumerate(files):
        if file.filename == '':
            continue

        bank_account = request.form.getlist("bank_account")[i]
        year = request.form.getlist("year")[i]
        month = request.form.getlist("month")[i]

        # Check for duplicate entry
        exists = FileUpload.check_for_already_existing_reconciliation(bank_account, year, month)
        if exists:
            duplicate_rows.append(i)  # Add the index of duplicate row
            continue  # Skip saving this file

        if file and allowed_file(file.filename):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = secure_filename(file.filename)
            new_filename = f"{timestamp}_{current_user.id}_{filename}"
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], new_filename)
            file.save(file_path)

            new_file_id = FileUpload.insert_into_file_upload(new_batch_id, new_filename, bank_account, year, month)
            if new_file_id is None:
                return jsonify({"error": "Database error while adding uploaded file"}), 500

    # Fetch updated uploaded files
    uploaded_files = FileUpload.get_uploaded_pending_submission_files_by_user(new_batch_id)

    response_data = {"files": uploaded_files}

    if duplicate_rows:
        response_data["duplicates"] = duplicate_rows  # Include duplicates in response
        response_data["message"] = "Some files were not uploaded because their details already exist in the database."

    if uploaded_files:
        response_data["message"] = "Files uploaded successfully!"

    return jsonify(response_data), 200


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


@app.route('/submit_files', methods=['POST'])
@login_required
@role_required(1)
def submit_files():
    """
    Processes the submission of files by updating their submission_status.
    """
    try:
        data = request.get_json()
        # print("Received data:", data)  # Debugging line
        files = data.get("files", [])

        if not files:
            return jsonify({"error": "No files provided"}), 400

        for file in files:
            bank_account_id = file.get("bank_account_id")
            year = file.get("year")
            month = file.get("month")
            file_name = file.get("file_name")
            batch_id = file.get("batch_id")

            # Update file status
            updated_file = FileUpload.update_file_submission_status(bank_account_id, year, month, file_name)
            if updated_file is None:
                return jsonify({"error": "Database error while updating status of file"}), 500

            # Update batch status
            updated_batch = FileUploadBatch.update_batch_submission_status(batch_id)
            if updated_batch is None:
                return jsonify({"error": "Database error while updating status of batch"}), 500

        return jsonify({"message": "Files submitted successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/submitted-reconciliations', methods=['GET', 'POST'])
@login_required
@role_required(2)
def submitted_reconciliations_page():
    reconciliations = FileUpload.get_submitted_reconciliations(current_user.id)
    return render_template('submitted_reconciliations.html', reconciliations=reconciliations)


@app.route('/approve-reconciliations', methods=['GET', 'POST'])
@login_required
@role_required(3, 4)
def approve_reconciliations_page():
    reconciliations = FileUpload.get_reconciliations_pending_approval(current_user.id)
    return render_template('approve_reconciliations.html', reconciliations=reconciliations)


@app.route("/download/<filename>")
@login_required
def download_file(filename):
    """Serves files from the uploads directory."""
    try:
        return send_from_directory(app.config["UPLOAD_FOLDER"], filename, as_attachment=True)
    except FileNotFoundError:
        abort(404)


@app.route('/logout')
def logout_page():
    logout_user()
    session.clear()  # Clear session to prevent stored data
    flash("You have been logged out!", category='info')
    return redirect(url_for("login_page"))

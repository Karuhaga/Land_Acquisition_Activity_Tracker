from BankReconciliation import app, os, allowed_file
from BankReconciliation.models import User, FileUploadBatch, FileUpload, FileDelete
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
            login_user(attempted_user, remember=True)
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
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return render_template('submit_reconciliation.html')
    return render_template('base.html', content=render_template('submit_reconciliation.html'))


@app.route('/upload', methods=['POST'])
@login_required  # Optional: Remove if not needed
def upload_files():
    if 'files' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    files = request.files.getlist('files')  # Get list of uploaded files
    uploaded_files = []

    # Create new batch entry
    new_batch_id = FileUploadBatch.insert_into_file_upload_batch(current_user.id)
    if new_batch_id is None:
        return jsonify({"error": "Database error while creating batch file upload"}), 500

    for file in files:
        if file.filename == '':
            continue
        if file and allowed_file(file.filename):
            # Generate timestamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

            # Secure filename and prepend timestamp
            filename = secure_filename(file.filename)
            new_filename = f"{timestamp}_{current_user.id}_{filename}"  # Add timestamp at the beginning

            # Save file with the new name
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], new_filename)
            file.save(file_path)
            uploaded_files.append(new_filename)

            # Create new row in insert_into_file_upload database table
            new_file_id = FileUpload.insert_into_file_upload(new_batch_id, new_filename)
            if new_file_id is None:
                return jsonify({"error": "Database error while adding uploaded file"}), 500

    if uploaded_files:
        return jsonify({"message": "Files uploaded successfully!", "files": uploaded_files}), 200
    return jsonify({"error": "No valid files uploaded"}), 400


@app.route('/delete-file', methods=['POST'])
@login_required
def delete_file():
    data = request.get_json()
    filename = data.get('filename')

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


@app.route('/previous-submissions', methods=['GET', 'POST'])
@login_required
def previous_submission_page():
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return render_template('previous_submissions.html')
    return render_template('base.html', content=render_template('previous_submissions.html'))


@app.route('/logout')
def logout_page():
    logout_user()
    session.clear()  # Clear session to prevent stored data
    flash("You have been logged out!", category='info')
    return redirect(url_for("login_page"))

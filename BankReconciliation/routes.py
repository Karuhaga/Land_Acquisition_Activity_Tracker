from BankReconciliation import app, os, allowed_file
from BankReconciliation.models import (User, FileUploadBatch, FileUpload, FileDelete, BankAccount,
                                       ReconciliationApprovals, WorkflowBreakdown, EmailHelper, Audit)
from BankReconciliation.forms import LoginForm
from flask import render_template, redirect, url_for, flash, request, jsonify, session, send_from_directory, abort
from flask_login import login_user, logout_user, login_required, current_user
from werkzeug.utils import secure_filename
from datetime import datetime
from BankReconciliation.rbac import role_required
import json
import threading
from BankReconciliation import app
from collections import defaultdict


@app.route('/', methods=['GET', 'POST'])
@app.route('/login', methods=['GET', 'POST'])
def login_page():
    form = LoginForm()

    if form.validate_on_submit():
        attempted_user = User.get_by_username(form.username.data)

        # Case 1: User not found (invalid username)
        if attempted_user is None:
            Audit.log_audit_trail(
                user_id=None,
                action="User Login Failed",
                details=f"Login failed: username '{form.username.data}' not found",
                ip_address=request.remote_addr
            )
            flash('Username and/or Password is incorrect. Please try again!', category='danger')

        # Case 2: User found but password incorrect
        elif not attempted_user.check_password(form.password.data):
            Audit.log_audit_trail(
                user_id=attempted_user.id,
                action="User Login Failed",
                details=f"Login failed: incorrect password for username '{form.username.data}'",
                ip_address=request.remote_addr
            )
            flash('Username and/or Password is incorrect. Please try again!', category='danger')

        # Case 3: Successful login
        else:
            session.permanent = True
            login_user(attempted_user)
            flash('You are logged in!', category='success')

            Audit.log_audit_trail(
                user_id=attempted_user.id,
                action="User Login",
                details=f"Login successful for username '{form.username.data}'",
                ip_address=request.remote_addr
            )

            return redirect(url_for('dashboard_page'))

    return render_template('login.html', form=form)


@app.route('/dashboard', methods=['GET', 'POST'])
@login_required
def dashboard_page():
    if not current_user.is_authenticated:
        return redirect(url_for("login_page"))  # Redirect if user is not authenticated

    user_roles = current_user.roles  # Get user roles

    if "Accountant" in user_roles:
        # Fetch key metrics
        unsubmitted_count = FileUpload.unsubmitted_files_num(current_user.id) or 0
        submitted_count = len(FileUpload.get_submitted_reconciliations(current_user.id))

        # Prepare data for the pie chart
        submission_stats_json = json.dumps({
            "submitted": submitted_count,
            "unsubmitted": unsubmitted_count
        })

        return render_template(
            'dashboard_accountant.html',
            user_roles=user_roles,
            unsubmitted_count=unsubmitted_count,
            submitted_count=submitted_count,
            submission_stats_json=submission_stats_json
        )

    elif "Head of Section" in user_roles or "Head of Department" in user_roles:
        # Fetch reconciliations pending approval and approved reconciliations
        approved_reconciliations = len(FileUpload.get_approved_reconciliations(current_user.id) or [])
        pending_reconciliations = len(FileUpload.get_reconciliations_pending_approval(current_user.id) or [])

        # Prepare data for the pie chart
        reconciliation_stats_json = json.dumps({
            "approved": approved_reconciliations,
            "pending": pending_reconciliations
        })

        return render_template(
            'dashboard_approver.html',
            user_roles=user_roles,
            approved_reconciliations_count=approved_reconciliations,
            reconciliations_pending_approval_submitted_count=pending_reconciliations,
            reconciliation_stats_json=reconciliation_stats_json  # Pass the new data for the pie chart
        )

    return render_template('dashboard.html')


@app.route('/submit-reconciliations', methods=['GET', 'POST'])
@login_required
@role_required(1)
def submit_reconciliations_page():
    # Fetch bank accounts from the database
    bank_accounts = BankAccount.get_bank_accounts_for_dropdown_menu(current_user.id)
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
    duplicate_rows = []  # Store rows that have duplicates

    num_of_pending_batches = FileUploadBatch.get_count_of_batch_pending_submission_by_user(current_user.id)

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
    uploaded_files = FileUpload.get_uploaded_pending_submission_files_by_user(current_user.id)

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
        files = data.get("files", [])

        if not files:
            return jsonify({"error": "No files provided"}), 400

        max_submission_status = None

        for file in files:
            bank_account_id = file.get("bank_account_id")
            year = file.get("year")
            month = file.get("month")
            file_name = file.get("file_name")

            # Update file status
            updated_file = FileUpload.update_file_submission_status(bank_account_id, year, month, file_name)
            if updated_file is None:
                return jsonify({"error": "Database error while updating status of file", "type": "danger"}), 500

            # Get batch_id
            batch_id = FileUpload.get_batch_id(bank_account_id, year, month, file_name)
            if batch_id is None:
                return jsonify({"error": "Database error while updating status of file", "type": "danger"}), 500

            # Pick id of file in file upload table
            id_of_file_upload = FileUploadBatch.get_id_of_file_upload(bank_account_id, year, month, file_name)
            if id_of_file_upload is None:
                return jsonify({"error": "Database error while picking id of uploaded file", "type": "danger"}), 500
            # reconciliation_approvals table
            decision = 1
            level = 1
            comment = ""

            submission_status = FileUpload.get_submission_status_of_reconciliation(id_of_file_upload)
            if max_submission_status is None or submission_status > max_submission_status:
                max_submission_status = submission_status

            last_reconciliation_approvals_id = (ReconciliationApprovals.insert_into_reconciliation_approvals
                                                (id_of_file_upload, decision, current_user.id, level, comment))
            if last_reconciliation_approvals_id is None:
                return jsonify({"error": "Database error while writing to reconciliation_approvals table", "type": "danger"}), 500

            # Update batch status
            updated_batch = FileUploadBatch.update_batch_submission_status(batch_id)
            if updated_batch is None:
                return jsonify({"error": "Database error while updating status of batch", "type": "danger"}), 500

        if max_submission_status is None:
            return jsonify({"error": "Could not determine max submission status", "type": "danger"}), 500

        # Store user details before threading
        user_fname = current_user.fname
        user_id = current_user.id

        # Get next approver(s)
        next_approvers = FileUpload.get_next_approver_fname_email(user_id, max_submission_status)

        if not next_approvers:
            return jsonify({"error": "No next approver found"}), 500

        # Send emails in the background
        # Send emails in the background with app context
        def send_emails():
            with app.app_context():  # Ensure Flask app context is available in the thread
                for approver in next_approvers:
                    EmailHelper.send_submitted_reconciliations_email(user_fname, approver["Email"], approver["Fname"], files)

        email_thread = threading.Thread(target=send_emails)
        email_thread.start()

        # Immediately return response while emails are being sent
        return jsonify({"message": "Files submitted successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e), "type": "danger"}), 500


@app.route('/submitted-reconciliations', methods=['GET', 'POST'])
@login_required
@role_required(2)
def submitted_reconciliations_page():
    reconciliations = FileUpload.get_submitted_reconciliations(current_user.id)
    return render_template('submitted_reconciliations.html', reconciliations=reconciliations)


@app.route('/approve-reconciliations', methods=['GET', 'POST'])
@login_required
@role_required(3, 4, 5, 6)
def approve_reconciliations_page():
    reconciliations = FileUpload.get_reconciliations_pending_approval(current_user.id)
    return render_template(
        'approve_reconciliations.html', reconciliations=reconciliations)


@app.route('/approved-reconciliations', methods=['GET', 'POST'])
@login_required
@role_required(3, 4, 5, 6)
def approved_reconciliations_page():
    reconciliations = FileUpload.get_approved_reconciliations(current_user.id)
    return render_template('approved_reconciliations.html', reconciliations=reconciliations)


@app.route('/approve-reconciliations-update', methods=['POST'])
@login_required
@role_required(3, 4, 5, 6)
def approve_reconciliations_update():

    try:
        data = request.get_json()
        action = data.get("action")
        comment = data.get("comment", "").strip()
        files = data.get("files", [])
        decision = 2 if action == "approve" else 3
        action_for_email = "approved" if action == "approve" else "rejected"
        initiator_approvals = defaultdict(list)
        max_submission_status = None

        if not files:
            return jsonify({"error": "No files provided"}), 400

        if action not in ["approve", "reject"]:
            return jsonify({"error": "Invalid action selected"}), 400

        for file in files:
            bank_account_id = file.get("bank_account_id")
            year = file.get("year")
            month = file.get("month")
            file_name = file.get("file_name")

            # Update file status
            updated_reconciliation_record = FileUpload.update_file_approval_status(bank_account_id, year, month,
                                                                                   file_name, action)

            if updated_reconciliation_record is None:
                return jsonify({"error": "Database error while updating status of file_upload table"}), 500

            # Pick id of file in file upload table
            id_of_file_upload = FileUploadBatch.get_id_of_file_upload(bank_account_id, year, month, file_name)
            if id_of_file_upload is None:
                return jsonify({"error": "Database error while picking id of uploaded file"}), 500

            # Pick latest level of reconciliation file from reconciliation_approvals table
            latest_approval_level = (ReconciliationApprovals.get_latest_reconciliation_approval_level
                                           (id_of_file_upload))
            if latest_approval_level is None:
                return jsonify({"error": "Database error while getting latest_approval_level from "
                                         "reconciliation_approvals table"}), 500

            # reconciliation_approvals table
            level = latest_approval_level + 1

            last_reconciliation_approvals_id = (ReconciliationApprovals.insert_into_reconciliation_approvals
                                                (id_of_file_upload, decision, current_user.id, level, comment))
            if last_reconciliation_approvals_id is None:
                return jsonify({"error": "Database error while writing to reconciliation_approvals table"}), 500

            if decision == 0:
                file_upload_id = FileUpload.update_file_approval_status_following_a_rejected_approval(id_of_file_upload)
                if file_upload_id is None:
                    return jsonify({"error": "Database error while updating status of file in file_upload table "
                                             "following a rejected request"}), 500

            # get the user id of the initiator of the bank reconciliation
            initiator_id = FileUploadBatch.get_reconciliation_initiator_user_id(bank_account_id, year, month, file_name)
            if not initiator_id:
                continue  # Skip if no initiator found

            initiator_approvals[initiator_id].append({
                "bank_account": bank_account_id,
                "year": year,
                "month": month,
                "file_name": file_name
            })

            submission_status = FileUpload.get_submission_status_of_reconciliation(id_of_file_upload)
            if max_submission_status is None or submission_status > max_submission_status:
                max_submission_status = submission_status

        if max_submission_status is None:
            return jsonify({"error": "Could not determine max submission status", "type": "danger"}), 500

        # Store user details before threading
        user_fname = current_user.fname
        user_id = current_user.id

        # Get initiator's email and first name
        reconciliation_initiator_email_and_fname = FileUploadBatch.get_reconciliation_initiator_email_and_fname(initiator_id)

        if not reconciliation_initiator_email_and_fname:
            return jsonify({"error": "No initiator of reconciliation found"}), 500

        # Send emails in the background with app context
        def send_emails():
            try:
                with app.app_context():
                    for initiator in reconciliation_initiator_email_and_fname:
                        EmailHelper.send_approval_summary_emails(user_fname, initiator["Email"], initiator["Fname"],
                                                                 files, action_for_email)
            except Exception as e:
                app.logger.error(f"Error in email thread 1: {e}")

        email_thread = threading.Thread(target=send_emails, daemon=True)
        email_thread.start()

        # Get next approver(s)
        if action == "approve":
            next_approvers = FileUpload.get_next_approver_fname_email(user_id, max_submission_status)

            if next_approvers:
                def send_emails2():
                    try:
                        with app.app_context():  # Ensure Flask app context is available in the thread
                            for approver in next_approvers:
                                EmailHelper.send_email_notification_to_next_approver(user_fname, approver["Email"],
                                                                                     approver["Fname"], files)
                    except Exception as e:
                        app.logger.error(f"Error in email thread 2: {e}")

                email_thread2 = threading.Thread(target=send_emails2, daemon=True)
                email_thread2.daemon = True
                email_thread2.start()

            if not next_approvers:
                # return jsonify({"error": "No next approver found"}), 500
                pass

        return jsonify({"message": "Reconciliation(s) approved successfully"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/get-reconciliation-workflow", methods=["GET"])
def get_reconciliation_workflow():
    bank_account = request.args.get("bank_account")
    reconciliation_year = request.args.get("year")
    reconciliation_month = request.args.get("month")
    file_name = request.args.get("file_name")

    # Pick id of file in file upload table
    id_of_file_upload = FileUploadBatch.get_id_of_file_upload(bank_account, reconciliation_year, reconciliation_month, file_name)
    if id_of_file_upload is None:
        return jsonify({"error": "Database error while picking id of uploaded file", "type": "danger"}), 500

    file_upload_id = id_of_file_upload
    if file_upload_id is None:
        return jsonify({"error": "No matching file found", "type": "danger"}), 500

    # Get the latest approval level for this file
    approvals = ReconciliationApprovals.get_reconciliation_approval_levels_of_given_file(file_upload_id)
    if approvals is None:
        return jsonify({"error": "Database error while picking latest reconciliation approval level of given file",
                        "type": "danger"}), 500

    approval_dict = {a[0]: {"decision": a[1], "approver": a[2], "date": a[3], "comment": a[4]} for a in approvals}

    workflow_id = 1
    # Get workflow breakdown for "Reconciliation Approval" (id=1)
    workflow_steps = WorkflowBreakdown.get_workflow_breakdown_for_reconciliation_approval(workflow_id)
    if workflow_steps is None:
        return jsonify({"error": "Database error while picking workflow breakdown for Reconciliation Approval workflow",
                        "type": "danger"}), 500

    workflow_list = []
    for step in workflow_steps:
        approval = approval_dict.get(step.level, None)
        workflow_list.append({
            "level": step.level,
            "name": step.name,
            "role": step.role_name,
            "status": approval["decision"] if approval else "Pending",
            "approver": approval["approver"] if approval else "N/A",
            "date": approval["date"] if approval else "N/A",
            "comment": approval["comment"] if approval else " "
        })

    return jsonify({"workflow_steps": workflow_list})


@app.route("/download/<filename>")
@login_required
def download_file(filename):
    """Serves files from the uploads directory."""
    try:
        return send_from_directory(app.config["UPLOAD_FOLDER"], filename, as_attachment=True)
    except FileNotFoundError:
        abort(404)


@app.route('/report-reconciliations-pending-submission', methods=['GET', 'POST'])
@login_required
@role_required(7,8,9,10)
def report_reconciliations_pending_submission_page():
    reconciliations = FileUpload.get_reconciliations_pending_submission()
    return render_template('report_reconciliations_pending_submission.html', reconciliations=reconciliations)


@app.route('/report-all-submitted-reconciliations', methods=['GET', 'POST'])
@login_required
@role_required(7,8,9,10)
def report_all_submitted_reconciliations_page():
    reconciliations = FileUpload.get_all_submitted_reconciliations()
    return render_template('report_all_submitted_reconciliations.html', reconciliations=reconciliations)


@app.route('/report-reconciliations-pending-approval', methods=['GET', 'POST'])
@login_required
@role_required(7,8,9,10)
def report_reconciliations_pending_approval_page():
    reconciliations = FileUpload.get_reconciliations_pending_approval_report()
    return render_template('report_reconciliations_pending_approval.html', reconciliations=reconciliations)


@app.route('/report-fully-approved-reconciliations', methods=['GET', 'POST'])
@login_required
@role_required(7,8,9,10)
def report_fully_approved_reconciliations_page():
    reconciliations = FileUpload.get_fully_approved_reconciliations_report()
    return render_template('report_fully_approved_reconciliations.html', reconciliations=reconciliations)


@app.route('/logout')
def logout_page():
    logout_user()
    session.clear()  # Clear session to prevent stored data
    flash("You are logged out!", category='info')
    return redirect(url_for("login_page"))

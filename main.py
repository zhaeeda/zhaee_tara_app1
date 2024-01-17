from flask import Flask, request, render_template, flash, redirect, url_for, session
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
from google.oauth2 import service_account
from datetime import datetime, timezone, timedelta
import logging
import os
from typing import Union
import secrets

app = Flask(__name__)
app.config['SECRET_KEY'] = secrets.token_hex(16)

CLOUD_STORAGE_BUCKET = 'v-mart-stagging'
project_id = 'principal-bird-410107'
dataset_name = 'v_mart_user_details'
table_name = 'user_data'

credentials = service_account.Credentials.from_service_account_file("credentials.json")

ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
app.config['UPLOAD_FOLDER'] = 'uploads'

bigquery_client = bigquery.Client()

def allowed_file(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def upload_file_to_storage(client: storage.Client, bucket_name: str, uploaded_file) -> bool:
    try:
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(uploaded_file.filename)

        blob.upload_from_string(
            uploaded_file.read(),
            content_type=uploaded_file.content_type
        )
        return True
    except Exception as e:
        logging.error(f"Error uploading file to storage: {e}")
        return False

def create_bigquery_table(project_id: str, dataset_id: str, table_id: str) -> None:
    dataset_ref = bigquery_client.dataset(dataset_id, project=project_id)

    try:
        # Create the dataset if not found
        bigquery_client.get_dataset(dataset_ref)
    except NotFound:
        dataset_name = bigquery.Dataset(dataset_ref)
        bigquery_client.create_dataset(dataset_name)

    table_ref = dataset_ref.table(table_id)

    try:
        # Create the table if not found
        bigquery_client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref)

        schema = [
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("filename", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ]

        table.schema = schema

        bigquery_client.create_table(table)
# Call the function to create the BigQuery dataset and table
create_bigquery_table(project_id, dataset_name, table_name)


@app.route('/', methods=['GET', 'POST'])
def index() -> str:
    if request.method == 'POST':
        email = request.form.get('email')
        name = request.form.get('name')

        # Store data in the session
        session['email'] = email
        session['name'] = name

        return redirect(url_for('upload'))
    return render_template("index.html")

@app.route('/upload', methods=['GET', 'POST'])
def upload() -> str:

    #user_email = session.get('email', '')
    #user_name = session.get('name', '')    

    if request.method == 'POST':
        uploaded_file = request.files.get('file')

        if not uploaded_file:
            flash('No file uploaded.', 'error')
            return redirect(url_for('index'))

        if not allowed_file(uploaded_file.filename):
            flash('Invalid file format. Only Excel files are allowed.', 'error')
            return redirect(url_for('index'))

        # Save metadata details
        user_email = session['email']
        user_name = session.['name']
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save user details and metadata in BigQuery
        status = save_user_details(user_email, user_name, uploaded_file.filename, timestamp)

        if status == 'Success':
            flash('File uploaded successfully!', 'success')
        else:
            flash('File upload failed.', 'error')

        # Create a Cloud Storage client.
        gcs = storage.Client(credentials=credentials)

        # Get the bucket that the file will be uploaded to.
        bucket = gcs.get_bucket(CLOUD_STORAGE_BUCKET)

        # Create a new blob and upload the file's content.
        blob = bucket.blob(uploaded_file.filename)
        try:
            blob.upload_from_string(
                uploaded_file.read(),
                content_type=uploaded_file.content_type
            )
            logging.info(f"File {uploaded_file.filename} uploaded to Cloud Storage successfully.")
        except Exception as e:
            logging.error(f"Error uploading file to Cloud Storage: {e}")
            flash('File upload failed.', 'error')
            return redirect(url_for('index'))

        # The public URL can be used to directly access the uploaded file via HTTP.
        return render_template("success.html", email=user_email, name=user_name)

    return render_template("upload.html")

def save_user_details(user_email: str, user_name: str, filename: str, timestamp: str) -> str:
    ist_timestamp = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S")
    dataset_name = 'v_mart_user_details'
    table_name = 'user_data'
    table_ref = bigquery_client.dataset(dataset_name).table(table_name)

    record = {
        'email': user_email,  # Use user_email directly
        'name': user_name,    # Use user_name directly
        'filename': filename,
        'status': 'Success',
        'timestamp': ist_timestamp
    }

    try:
        errors = bigquery_client.insert_rows_json(table_ref, [record])
        if errors:
            logging.error(f"Error inserting record into BigQuery: {errors}")
            return 'Failed'
        else:
            logging.info("Record inserted successfully into BigQuery")
            return 'Success'
    except Exception as e:
        logging.error(f"An error occurred during BigQuery insertion: {e}")
        return 'Failed'

@app.errorhandler(500)
def server_error(e: Union[Exception, int]) -> str:
    logging.exception('An error occurred during a request.')
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for the full stack trace.
    """.format(e), 500

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

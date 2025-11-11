# sftp_server_to_gcs.py
from datetime import datetime, timedelta
import os
import re
import tempfile
import stat
import shutil
import io
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
# keep import for compatibility if you ever want to use SFTPHook elsewhere
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# secret manager
from google.cloud import secretmanager, storage
import google.auth

# paramiko for SSH/SFTP connection at runtime
import paramiko

# ---------- config ----------
JOB_CONFIG = Variable.get("in_sftp_vendor_list", deserialize_json=True)
PIPELINE_CONFIG = {'sftp_vendor_list': JOB_CONFIG['sftp_vendor_list']}
VENDOR_LIST_RAW = PIPELINE_CONFIG['sftp_vendor_list']

if isinstance(VENDOR_LIST_RAW, str):
    VENDORS = [b.strip() for b in VENDOR_LIST_RAW.split(",") if b.strip()]
elif isinstance(VENDOR_LIST_RAW, (list, tuple)):
    VENDORS = [str(b).strip() for b in VENDOR_LIST_RAW if str(b).strip()]
else:
    VENDORS = []

default_args = {
    'owner': 'mdlz_cde_in',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'in_sftp_to_gcs_transfer',
    default_args=default_args,
    description='Transfer files from SFTP to GCS',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
)

# constants you had
SFTP_CONN_ID = 'my_sftp_connection'
GCS_CONN_ID = 'google_cloud_default'
GCS_BUCKET = 'in-cde-sftp-file-transfer-prd-bkt'
SFTP_REMOTE_PATH = '/D/DataHub/in/inbound'
GCS_OBJECT_PREFIX = 'sftp/inbound'

# secret name (env var). Set this in Composer environment variables to the secret id.
SECRET_NAME_ENV = os.environ.get("IN_PRD_SFTP_CREDENTIALS_SECRET_NAME", "IN_PRD_SFTP_CREDENTIALS")

def sanitize_for_task_id(name: str) -> str:
    s = re.sub(r'[^A-Za-z0-9]+', '_', name)
    s = re.sub(r'__+', '_', s).strip('_')
    return s or 'brand'

start = EmptyOperator(task_id='start', dag=dag)
end = EmptyOperator(task_id='end', dag=dag)

# ---------- Secret Manager fetch ----------
def fetch_secret_bytes(secret_name: str) -> bytes:
    """
    Fetch latest secret version payload bytes from Secret Manager.
    secret_name can be:
      - secret id (e.g. "IN_PRD_SFTP_CREDENTIALS")
      - or full resource path "projects/<proj>/secrets/<secret>/versions/<ver>"
    """
    # NOTE: you already hard-coded project earlier; keep that or change to env as needed
    project = 'prd-global-cde-prj'
    if not project:
        try:
            _, project = google.auth.default()
        except Exception:
            project = None
    if not project:
        raise RuntimeError("GCP project id not found in env and default credentials failed.")

    client = secretmanager.SecretManagerServiceClient()

    # allow full resource path or just secret id
    if secret_name.startswith("projects/"):
        # ensure versions/latest if not provided
        secret_path = secret_name if "/versions/" in secret_name else f"{secret_name}/versions/latest"
    else:
        secret_path = f"projects/{project}/secrets/{secret_name}/versions/latest"

    resp = client.access_secret_version(request={"name": secret_path})
    return resp.payload.data  # bytes

# ---------- Transfer logic ----------
def transfer_vendor_using_openssh(vendor: str, **context):
    # read connection info
    conn = BaseHook.get_connection(SFTP_CONN_ID)
    host = conn.host
    username = conn.login
    password = conn.password  # if connection has password (often not)
    port = conn.port
    if not port:
        port = 22
    try:
        port = int(port)
    except Exception:
        port = 22

    extras = {}
    try:
        extras = conn.extra_dejson or {}
    except Exception:
        extras = {}

    # fetch PEM from Secret Manager
    pem_bytes = fetch_secret_bytes(SECRET_NAME_ENV)
    pem_text = pem_bytes.decode('utf-8')

    # ensure the secret payload contains a PEM (basic check)
    if "BEGIN" not in pem_text:
        # helpful error message
        raise RuntimeError("Secret payload does not look like an OpenSSH/RSA PEM. Ensure Secret Manager holds the private key text "
                           "including '-----BEGIN ... PRIVATE KEY-----' lines.")

    # write PEM to secure temp file on *worker*
    tmp_key_file = tempfile.NamedTemporaryFile(delete=False, prefix="sftp_key_", suffix=".pem")
    tmp_key_path = tmp_key_file.name
    try:
        tmp_key_file.write(pem_text.encode('utf-8'))
        tmp_key_file.flush()
        tmp_key_file.close()
        os.chmod(tmp_key_path, stat.S_IRUSR | stat.S_IWUSR)  # 0o600

        # ---------- connect with paramiko using the temp key file ----------
        ssh = None
        sftp = None
        gcs = GCSHook(gcp_conn_id=GCS_CONN_ID)

        # try to load key (try RSA then ED25519)
        pkey = None
        passphrase = extras.get("private_key_passphrase") or None
        key_load_errors = []
        try:
            pkey = paramiko.RSAKey.from_private_key_file(tmp_key_path, password=passphrase)
        except Exception as e_rsa:
            key_load_errors.append(f"RSAKey error: {e_rsa}")
            try:
                pkey = paramiko.Ed25519Key.from_private_key_file(tmp_key_path, password=passphrase)
            except Exception as e_ed:
                key_load_errors.append(f"Ed25519Key error: {e_ed}")
                pkey = None

        try:
            ssh = paramiko.SSHClient()
            # be permissive about host keys (same as no_host_key_check)
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if pkey is not None:
                ssh.connect(hostname=host, port=int(port), username=username, pkey=pkey, password=password, timeout=30)
            else:
                # fallback: let paramiko use the key filename directly by passing key_filename
                # (this may work for newer key formats)
                ssh.connect(hostname=host, port=int(port), username=username, key_filename=tmp_key_path, password=password, timeout=30)

            sftp = ssh.open_sftp()

            # list files in remote directory (wrap in try/except if dir may not exist)
            remote_dir = f"{SFTP_REMOTE_PATH}/{vendor}"
            try:
                files = sftp.listdir(remote_dir)
            except IOError:
                files = []

            # create local tmp dir to stage files
            local_tmp_dir = tempfile.mkdtemp(prefix=f"sftp_dl_{vendor}_")
            try:
                for fname in files:
                    if fname.startswith("."):
                        continue
                    remote_path = f"{remote_dir}/{fname}"
                    local_path = os.path.join(local_tmp_dir, fname)
                    # download
                    sftp.get(remote_path, local_path)
                    # upload to GCS
                    destination_path = f"{GCS_OBJECT_PREFIX}/{vendor}/{fname}"
                    gcs.upload(bucket_name=GCS_BUCKET, object_name=destination_path, filename=local_path)
                    # attempt to remove remote file to emulate move
                    try:
                        sftp.remove(remote_path)
                    except Exception:
                        # not fatal; means copy instead of move
                        pass
            finally:
                # cleanup local temp dir
                try:
                    shutil.rmtree(local_tmp_dir)
                except Exception:
                    pass
        finally:
            # close sftp/ssh
            try:
                if sftp:
                    sftp.close()
            except Exception:
                pass
            try:
                if ssh:
                    ssh.close()
            except Exception:
                pass
    finally:
        # always remove key file
        try:
            os.remove(tmp_key_path)
        except Exception:
            pass

# ---------- Build tasks ----------
if not VENDORS:
    placeholder = EmptyOperator(task_id='no_VENDORS_configured', dag=dag)
    start >> placeholder >> end
else:
    for vendor in VENDORS:
        safe_vendor = sanitize_for_task_id(vendor)
        task_id = f"transfer_sftp_to_gcs_{safe_vendor}"
        transfer_task = PythonOperator(
            task_id=task_id,
            python_callable=transfer_vendor_using_openssh,
            op_kwargs={"vendor": vendor},
            dag=dag,
        )
        start >> transfer_task >> end

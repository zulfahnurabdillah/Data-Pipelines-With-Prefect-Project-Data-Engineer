# transform.py
from prefect import flow, task
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run, wait_for_dbt_cloud_job_run

# --- ⚠️ HARDCODE KREDENSIAL (JANGAN SAMPAI SALAH) ⚠️ ---

# 1. Masukkan Token dbtc_ Anda di sini (Pastikan tidak ada spasi!)
MY_TOKEN = "dbtc_ZF0-iF8TzfvRRjLmIkd2mMYlmszQbTHC3O1r9-j3KU-jrvuSGM"

# 2. Data Akun Anda
MY_ACCOUNT_ID = 70471823510118
MY_JOB_ID = 70471823530095

# 3. HOST KHUSUS (INI KUNCI SUKSESNYA!)
# Jangan pakai https://, cukup domain saja
MY_HOST = "at088.us1.dbt.com"

@task(name="Get Hardcoded Credentials")
def get_dbt_creds_direct() -> DbtCloudCredentials:
    """
    Menggunakan kredensial langsung (Hardcoded) dengan HOST yang benar.
    """
    print(f"--- DEBUG: Target Host: {MY_HOST} ---")
    print(f"--- DEBUG: Account ID: {MY_ACCOUNT_ID} ---")
    
    return DbtCloudCredentials(
        api_key=MY_TOKEN,
        account_id=MY_ACCOUNT_ID,
        host=MY_HOST  # <--- KITA PAKSA PAKA HOST INI
    )

@flow(name="Trigger dbt Cloud Flow")
async def dbt_transform_flow():
    """Memicu job dbt Cloud."""
    
    # Panggil task (sinkron)
    creds = get_dbt_creds_direct()
    
    print(f"Memicu Job ID: {MY_JOB_ID} di server {MY_HOST}...")
    
    # Trigger
    job_run = await trigger_dbt_cloud_job_run(
        dbt_cloud_credentials=creds,
        job_id=MY_JOB_ID
    )
    
    run_id = job_run.id
    print(f"✅ Job berhasil dipicu! Run ID: {run_id}")
    print("2. Menunggu job selesai...")

    # Wait
    await wait_for_dbt_cloud_job_run(
        dbt_cloud_credentials=creds,
        job_run_id=run_id
    )
    
    print(f"🎉 dbt Cloud Job {run_id} selesai dengan sukses!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(dbt_transform_flow())


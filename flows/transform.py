# transform.py
from prefect import flow, task
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run, wait_for_dbt_cloud_job_run

# --- ⚠️ HARDCODE KREDENSIAL (BYPASS BLOCKS) ⚠️ ---

# 1. Masukkan Token dbtc_ Anda di sini (PASTIKAN TIDAK ADA SPASI)
MY_TOKEN = "dbtc_ZF0-iF8TzfvRRjLmIkd2mMYlmszQbTHC3O1r9-j3KU-jrvuSGM"

# 2. Data yang sudah terbukti benar dari tes lokal Anda
MY_ACCOUNT_ID = 70471823510118
MY_JOB_ID = 70471823530095
MY_HOST = "at088.us1.dbt.com"

@task(name="Get Hardcoded Credentials")
def get_dbt_creds_direct() -> DbtCloudCredentials:
    """
    Menggunakan kredensial langsung (Hardcoded) untuk menghindari
    masalah pembacaan Prefect Block.
    """
    print(f"--- DEBUG: Menggunakan Host: {MY_HOST} ---")
    print(f"--- DEBUG: Menggunakan Account: {MY_ACCOUNT_ID} ---")
    
    return DbtCloudCredentials(
        api_key=MY_TOKEN,          # Token langsung
        account_id=MY_ACCOUNT_ID,  # ID langsung
        host=MY_HOST               # Host langsung
    )

@flow(name="Trigger dbt Cloud Flow")
async def dbt_transform_flow():
    """Memicu job dbt Cloud."""
    
    # Panggil task sinkron biasa (tanpa await karena hardcode)
    creds = get_dbt_creds_direct()
    
    print(f"Memicu Job ID: {MY_JOB_ID}...")
    
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

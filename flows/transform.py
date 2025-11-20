# transform.py
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run, wait_for_dbt_cloud_job_run

# --- Konfigurasi ---
BLOCK_NAME_API_TOKEN = "dbt-cloud-api-token"
BLOCK_NAME_ACCOUNT_ID = "dbt-cloud-account-id"

# Masukkan Job ID Anda (Angka)
DBT_CLOUD_JOB_ID = 70471823530095 

# --- ⚠️ KUNCI RAHASIA KITA: HOST KHUSUS ---
# Jangan pakai https://, cukup domain saja
MY_DBT_HOST = "at088.us1.dbt.com"

@task(name="Get dbt Cloud Credentials")
async def get_dbt_creds_block() -> DbtCloudCredentials:
    """Mengambil kredensial dbt Cloud dari Prefect Blocks."""
    
    print(f"--- DEBUG: Menggunakan Host Khusus: {MY_DBT_HOST} ---")

    # 1. Load API Token dari Block
    api_token_block = await Secret.load(BLOCK_NAME_API_TOKEN)
    api_key_val = api_token_block.get()

    # 2. Load Account ID dari Block
    account_id_block = await Secret.load(BLOCK_NAME_ACCOUNT_ID)
    account_id_val = int(account_id_block.get())

    print(f"--- DEBUG: Menggunakan Account ID: {account_id_val} ---")

    # 3. Return Kredensial dengan HOST yang benar
    return DbtCloudCredentials(
        api_key=api_key_val, 
        account_id=account_id_val,
        host=MY_DBT_HOST  # <--- INI YANG MEMBUATNYA BERHASIL
    )

@flow(name="Trigger dbt Cloud Flow")
async def dbt_transform_flow():
    """Memicu job dbt Cloud."""
    
    creds = await get_dbt_creds_block()
    
    print(f"Memicu Job ID: {DBT_CLOUD_JOB_ID} di host {MY_DBT_HOST}...")
    
    # Trigger
    job_run = await trigger_dbt_cloud_job_run(
        dbt_cloud_credentials=creds,
        job_id=DBT_CLOUD_JOB_ID
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

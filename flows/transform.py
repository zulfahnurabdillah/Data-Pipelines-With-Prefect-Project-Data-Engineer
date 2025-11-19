# transform.py
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect_dbt.cloud import DbtCloudCredentials
# KITA IMPORT DUA FUNGSI SEKARANG: TRIGGER DAN WAIT
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run, wait_for_dbt_cloud_job_run

# --- Konfigurasi ---
DBT_CLOUD_API_TOKEN_BLOCK = "dbt-cloud-api-token"
DBT_CLOUD_ACCOUNT_ID_BLOCK = "dbt-cloud-account-id"
DBT_CLOUD_JOB_ID = 70471823530095

@task(name="Get dbt Cloud Credentials")
async def get_dbt_creds_block() -> DbtCloudCredentials:
    """Mengambil kredensial dbt Cloud dari Prefect Blocks."""
    
    # Load secret
    api_token_block = await Secret.load(DBT_CLOUD_API_TOKEN_BLOCK)
    account_id_block = await Secret.load(DBT_CLOUD_ACCOUNT_ID_BLOCK)
    
    # Menggunakan .get() untuk mengambil nilai string
    return DbtCloudCredentials(
        api_key=api_token_block.get(),      # Menggunakan 'api_key' (sesuai perbaikan sebelumnya)
        account_id=int(account_id_block.get())
    )

@flow(name="Trigger dbt Cloud Flow")
async def dbt_transform_flow():
    """Memicu job dbt Cloud dan menunggunya selesai."""
    
    print("Mengambil kredensial...")
    creds = await get_dbt_creds_block()
    
    print(f"1. Memicu Job ID: {DBT_CLOUD_JOB_ID}...")
    
    # LANGKAH 1: TRIGGER (Tanpa menunggu)
    # Kita hapus 'wait_for_completion' yang menyebabkan error
    job_run = await trigger_dbt_cloud_job_run(
        dbt_cloud_credentials=creds,
        job_id=DBT_CLOUD_JOB_ID
    )
    
    run_id = job_run.id
    print(f"✅ Job berhasil dipicu! Run ID: {run_id}")
    print("2. Menunggu job selesai (ini mungkin memakan waktu)...")

    # LANGKAH 2: WAIT (Menunggu secara eksplisit)
    # Kita gunakan fungsi khusus untuk menunggu
    await wait_for_dbt_cloud_job_run(
        dbt_cloud_credentials=creds,
        job_run_id=run_id
    )
    
    print(f"🎉 dbt Cloud Job {run_id} selesai dengan sukses!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(dbt_transform_flow())

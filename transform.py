# flows/transform.py
from prefect import flow, task
from prefect.blocks.system import Secret, String
# Import baru dari paket 'prefect-dbt'
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run

# --- Konfigurasi ---
# Pastikan nama-nama ini cocok dengan Blocks Anda
DBT_CLOUD_API_TOKEN_BLOCK = "dbt-cloud-api-token" 
DBT_CLOUD_ACCOUNT_ID_BLOCK = "dbt-cloud-account-id"

# GANTI DENGAN JOB ID ANDA DARI DBT CLOUD
DBT_CLOUD_JOB_ID = 70471823530095 

@task(name="Get dbt Cloud Credentials")
async def get_dbt_creds_block() -> DbtCloudCredentials:
    """Mengambil kredensial dbt Cloud dari Prefect Blocks."""
    api_token = await Secret.load(DBT_CLOUD_API_TOKEN_BLOCK)
    account_id = await String.load(DBT_CLOUD_ACCOUNT_ID_BLOCK)
    
    # Membuat objek kredensial yang akan digunakan oleh task
    return DbtCloudCredentials(
        api_token=api_token.get_value(),
        account_id=int(account_id.value)
    )

@flow(name="Trigger dbt Cloud Flow")
async def dbt_transform_flow():
    """Memicu job dbt Cloud dan menunggunya selesai."""
    
    creds = await get_dbt_creds_block()
    
    print(f"Memicu dbt Cloud Job ID: {DBT_CLOUD_JOB_ID}...")
    
    dbt_run = await trigger_dbt_cloud_job_run(
        dbt_cloud_credentials=creds,
        job_id=DBT_CLOUD_JOB_ID,
        wait_for_completion=True # Penting: Flow akan menunggu dbt selesai
    )
    
    print(f"dbt Cloud run {dbt_run.id} selesai dengan status: {dbt_run.status}")
    
    # Otomatis GAGAL-kan flow jika dbt run tidak sukses
    if dbt_run.status != "Success":
        raise Exception("dbt Cloud job run failed.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(dbt_transform_flow())
# transform.py
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run

# --- Konfigurasi ---
DBT_CLOUD_API_TOKEN_BLOCK = "dbt-cloud-api-token"
DBT_CLOUD_ACCOUNT_ID_BLOCK = "dbt-cloud-account-id"
# Ganti dengan Job ID Anda yang benar!
DBT_CLOUD_JOB_ID = 70471823530095 

@task(name="Get dbt Cloud Credentials")
async def get_dbt_creds_block() -> DbtCloudCredentials:
    """Mengambil kredensial dbt Cloud dari Prefect Blocks."""
    
    print("--- DEBUGGING CREDENTIALS ---")
    
    # 1. Load API Token
    try:
        api_token_block = await Secret.load(DBT_CLOUD_API_TOKEN_BLOCK)
        api_token_value = api_token_block.get()
        
        # Cek panjang token
        if not api_token_value:
            print("❌ ERROR: Block API Token KOSONG!")
            raise ValueError("API Token kosong di Prefect Block.")
        else:
            # Tampilkan 4 karakter pertama saja untuk konfirmasi
            print(f"✅ API Token ditemukan. Panjang: {len(api_token_value)} karakter.")
            print(f"✅ Depan: {api_token_value[:4]}...")
            
    except Exception as e:
        print(f"❌ GAGAL load API Token Block: {e}")
        raise

    # 2. Load Account ID
    try:
        account_id_block = await Secret.load(DBT_CLOUD_ACCOUNT_ID_BLOCK)
        account_id_value = account_id_block.get()
        print(f"✅ Account ID ditemukan: {account_id_value}")
    except Exception as e:
        print(f"❌ GAGAL load Account ID Block: {e}")
        raise

    print("--- END DEBUGGING ---")

    return DbtCloudCredentials(
        api_token=api_token_value,
        account_id=int(account_id_value)
    )

@flow(name="Trigger dbt Cloud Flow")
async def dbt_transform_flow():
    """Memicu job dbt Cloud dan menunggunya selesai."""
    
    creds = await get_dbt_creds_block()
    
    print(f"Memicu dbt Cloud Job ID: {DBT_CLOUD_JOB_ID}...")
    
    dbt_run = await trigger_dbt_cloud_job_run(
        dbt_cloud_credentials=creds,
        job_id=DBT_CLOUD_JOB_ID,
        wait_for_completion=True
    )
    
    print(f"dbt Cloud run {dbt_run.id} selesai dengan status: {dbt_run.status}")
    
    if dbt_run.status != "Success":
        raise Exception("dbt Cloud job run failed.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(dbt_transform_flow())


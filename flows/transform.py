# transform.py
from prefect import flow, task
from prefect.blocks.system import Secret 
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run, wait_for_dbt_cloud_job_run

# --- ⚠️ ISI DATA ASLI ANDA DI SINI (HARDCODE) ⚠️ ---
# Kita akan memaksa update Block menggunakan nilai ini setiap kali flow berjalan.

# 1. Token Service (dbtc_...)
MY_REAL_TOKEN = "dbtc_ZF0-iF8TzfvRRjLmIkd2mMYlmszQbTHC3O1r9-j3KU-jrvuSGM" 

# 2. Account ID (Angka dalam string)
MY_REAL_ACCOUNT_ID = "70471823510118"

# 3. Job ID (Angka Integer)
DBT_CLOUD_JOB_ID = 70471823530095

# Nama Block (Jangan diubah)
BLOCK_NAME_API_TOKEN = "dbt-cloud-api-token"
BLOCK_NAME_ACCOUNT_ID = "dbt-cloud-account-id"


@task(name="Get dbt Cloud Credentials")
async def get_dbt_creds_block() -> DbtCloudCredentials:
    """
    Task ini akan:
    1. MEMAKSA simpan token hardcoded ke Block Prefect (Overwriting).
    2. Memuat kembali Block tersebut untuk digunakan.
    """
    print("--- 🔄 FORCE UPDATING BLOCKS ---")

    # ---------------------------------------------------------
    # LANGKAH 1: PAKSA SIMPAN (SAVE)
    # Ini akan memperbaiki Block di Cloud jika sebelumnya rusak/salah
    # ---------------------------------------------------------
    print(f"Menyimpan Token ke Block '{BLOCK_NAME_API_TOKEN}'...")
    # Kita gunakan await karena .save() bersifat async di dalam flow
    await Secret(value=MY_REAL_TOKEN).save(name=BLOCK_NAME_API_TOKEN, overwrite=True)
    
    print(f"Menyimpan Account ID ke Block '{BLOCK_NAME_ACCOUNT_ID}'...")
    await Secret(value=MY_REAL_ACCOUNT_ID).save(name=BLOCK_NAME_ACCOUNT_ID, overwrite=True)
    
    print("✅ Blocks berhasil diperbarui dengan data hardcoded.")


    # ---------------------------------------------------------
    # LANGKAH 2: MUAT KEMBALI (LOAD & GET)
    # Sesuai permintaan Anda
    # ---------------------------------------------------------
    print("--- MEMUAT KREDENSIAL ---")
    
    # Load Token
    secret_block_token = await Secret.load(BLOCK_NAME_API_TOKEN)
    api_key_value = secret_block_token.get()
    
    # Load Account ID
    secret_block_account = await Secret.load(BLOCK_NAME_ACCOUNT_ID)
    account_id_value = secret_block_account.get()
    
    print(f"✅ Kredensial siap digunakan for Account: {account_id_value}")

    return DbtCloudCredentials(
        api_key=api_key_value,
        account_id=int(account_id_value)
    )

@flow(name="Trigger dbt Cloud Flow")
async def dbt_transform_flow():
    """Memicu job dbt Cloud."""
    
    creds = await get_dbt_creds_block()
    
    print(f"Memicu Job ID: {DBT_CLOUD_JOB_ID}...")
    
    job_run = await trigger_dbt_cloud_job_run(
        dbt_cloud_credentials=creds,
        job_id=DBT_CLOUD_JOB_ID
    )
    
    run_id = job_run.id
    print(f"✅ Job dipicu! Run ID: {run_id}. Menunggu selesai...")

    await wait_for_dbt_cloud_job_run(
        dbt_cloud_credentials=creds,
        job_run_id=run_id
    )
    
    print(f"🎉 Selesai!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(dbt_transform_flow())

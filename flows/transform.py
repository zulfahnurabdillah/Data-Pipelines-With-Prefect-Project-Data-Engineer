# transform.py
from prefect import flow, task
# 1. Import Library Block System (Sesuai snippet Anda)
from prefect.blocks.system import Secret 
from prefect_dbt.cloud import DbtCloudCredentials
from prefect_dbt.cloud.jobs import trigger_dbt_cloud_job_run, wait_for_dbt_cloud_job_run

# --- Konfigurasi Nama Block ---
# Pastikan nama ini SAMA PERSIS dengan yang ada di UI Prefect Cloud Anda
BLOCK_NAME_API_TOKEN = "dbt-cloud-api-token"
BLOCK_NAME_ACCOUNT_ID = "dbt-cloud-account-id"

# Ganti dengan Job ID Anda
DBT_CLOUD_JOB_ID = 70471823530095 

@task(name="Get dbt Cloud Credentials")
async def get_dbt_creds_block() -> DbtCloudCredentials:
    """
    Mengambil kredensial menggunakan pola:
    secret_block = Secret.load("BLOCK_NAME")
    value = secret_block.get()
    """
    print("--- MEMUAT KREDENSIAL DARI BLOCK ---")

    # ---------------------------------------------------------
    # BAGIAN 1: API TOKEN
    # ---------------------------------------------------------
    print(f"1. Loading Block: {BLOCK_NAME_API_TOKEN}")
    
    # Menggunakan kode snippet Anda (tambah await karena ini async flow)
    secret_block_token = await Secret.load(BLOCK_NAME_API_TOKEN)
    
    # Mengakses value yang tersimpan (bagian .get())
    api_key_value = secret_block_token.get()
    
    # Validasi sederhana
    if not api_key_value:
        raise ValueError("Block API Token kosong!")
    print("   ✅ API Token berhasil diambil.")


    # ---------------------------------------------------------
    # BAGIAN 2: ACCOUNT ID
    # ---------------------------------------------------------
    print(f"2. Loading Block: {BLOCK_NAME_ACCOUNT_ID}")
    
    # Menggunakan kode snippet Anda
    secret_block_account = await Secret.load(BLOCK_NAME_ACCOUNT_ID)
    
    # Mengakses value
    account_id_value = secret_block_account.get()
    
    print(f"   ✅ Account ID berhasil diambil: {account_id_value}")

    # ---------------------------------------------------------
    # RETURN KREDENSIAL
    # ---------------------------------------------------------
    return DbtCloudCredentials(
        api_key=api_key_value,           # Masukkan hasil .get() ke sini
        account_id=int(account_id_value) # Pastikan jadi angka (integer)
    )

@flow(name="Trigger dbt Cloud Flow")
async def dbt_transform_flow():
    """Memicu job dbt Cloud."""
    
    # Panggil task di atas
    creds = await get_dbt_creds_block()
    
    print(f"Memicu Job ID: {DBT_CLOUD_JOB_ID}...")
    
    # Trigger Job
    job_run = await trigger_dbt_cloud_job_run(
        dbt_cloud_credentials=creds,
        job_id=DBT_CLOUD_JOB_ID
    )
    
    run_id = job_run.id
    print(f"✅ Job dipicu! Run ID: {run_id}. Menunggu selesai...")

    # Wait Job
    await wait_for_dbt_cloud_job_run(
        dbt_cloud_credentials=creds,
        job_run_id=run_id
    )
    
    print(f"🎉 Selesai!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(dbt_transform_flow())

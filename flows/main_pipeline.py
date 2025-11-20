import requests
import asyncio
import json
from prefect import flow, task

# --- ⚠️ ISI DATA ASLI ANDA DI SINI ⚠️ ---
# Token dbtc_... (Pastikan tidak ada spasi)
MY_TOKEN = "dbtc_ZF0-iF8TzfvRRjLmIkd2mMYlmszQbTHC3O1r9-j3KU-jrvuSGM" 

# Data Akun Anda
MY_ACCOUNT_ID = 70471823510118
MY_JOB_ID = 70471823530095

# HOST KHUSUS ANDA (Kita tulis manual di sini)
MY_HOST = "at088.us1.dbt.com"

@task(name="Manual Trigger dbt Cloud")
def trigger_dbt_manual():
    """
    Mengirim request HTTP langsung ke dbt Cloud tanpa library prefect-dbt.
    Ini memaksa penggunaan HOST yang benar.
    """
    print(f"🚀 Menembak API dbt Cloud secara manual ke: {MY_HOST}")
    
    # KITA RAKIT URL SENDIRI (Dijamin tidak salah alamat)
    url = f"https://{MY_HOST}/api/v2/accounts/{MY_ACCOUNT_ID}/jobs/{MY_JOB_ID}/run/"
    
    headers = {
        "Authorization": f"Token {MY_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "cause": "Triggered by Prefect (Manual Mode)"
    }

    # Kirim Request
    try:
        response = requests.post(url, headers=headers, json=payload)
        print(f"📡 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            run_id = data['data']['id']
            print(f"✅ SUKSES! Job berhasil berjalan.")
            print(f"🆔 Run ID: {run_id}")
            print(f"🔗 Link: {data['data']['href']}")
            return run_id
        else:
            print(f"❌ GAGAL! Response: {response.text}")
            raise Exception(f"Gagal trigger dbt job: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error Koneksi: {e}")
        raise

@task(name="Monitor Job Status")
async def wait_for_job(run_id):
    """
    Mengecek status job setiap 10 detik sampai selesai.
    """
    url = f"https://{MY_HOST}/api/v2/accounts/{MY_ACCOUNT_ID}/runs/{run_id}/"
    headers = {"Authorization": f"Token {MY_TOKEN}"}
    
    while True:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            status = data['data']['status_human'] # Contoh: 'Queued', 'Running', 'Success'
            
            print(f"⏳ Status Job {run_id}: {status}")
            
            if status == "Success":
                print("🎉 Job Selesai dengan Sukses!")
                break
            elif status in ["Error", "Cancelled", "Failed"]:
                raise Exception(f"Job Gagal dengan status: {status}")
            
            # Tunggu 10 detik sebelum cek lagi
            await asyncio.sleep(10)
        else:
            print(f"⚠️ Gagal cek status: {response.status_code}")
            await asyncio.sleep(10)

@flow(name="E-commerce dbt Transformation")
async def e2e_dbt_pipeline():
    print("--- Memulai Pipeline Manual ---")
    
    # 1. Trigger
    run_id = trigger_dbt_manual()
    
    # 2. Wait (Hanya jalan jika trigger sukses)
    if run_id:
        await wait_for_job(run_id)

if __name__ == "__main__":
    asyncio.run(e2e_dbt_pipeline())

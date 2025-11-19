# flows/main_pipeline.py
import subprocess
import sys
from prefect import flow

# --- JURUS PAMUNGKAS: FORCE INSTALL ---
# Kode ini memaksa server untuk menginstall paket sebelum lanjut
def install_and_import():
    try:
        import prefect_dbt
        print("prefect-dbt sudah terinstall.")
    except ImportError:
        print("prefect-dbt tidak ditemukan. Menginstall sekarang...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "prefect-dbt"])
        print("Instalasi selesai.")

# Jalankan fungsi install INI DULU sebelum import file lain
install_and_import()

# --- BARU IMPORT FILE LAIN SETELAH INSTALL SELESAI ---
# Kita taruh import ini di sini agar tidak error di awal
from transform import dbt_transform_flow

@flow(name="E-commerce dbt Transformation")
async def e2e_dbt_pipeline():
    """
    Flow E2E Cloud Utama:
    Hanya memicu dbt Cloud job untuk transformasi.
    """
    print("Memulai Fase Transformasi (dbt Cloud)...")
    await dbt_transform_flow()
    print("Pipeline dbt Cloud berhasil dijalankan!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(e2e_dbt_pipeline())

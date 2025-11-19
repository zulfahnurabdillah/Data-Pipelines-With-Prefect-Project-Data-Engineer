# flows/main_pipeline.py
from prefect import flow
from flows.transform import dbt_transform_flow

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
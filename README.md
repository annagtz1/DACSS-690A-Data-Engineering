#E-commerce ETL — Local Prefect Run

This repo contains a minimal Prefect ETL pipeline and supporting files for the course final project.

What I ran (local, reproducible)
- I created the pipeline files (prefect_olist_pipeline.py and helpers) using the project setup cell.
- I installed the required packages in the notebook environment and restarted the kernel.
- I executed a quick sample run and then a full run of the Prefect flow (data_processing_flow) locally.
- The pipeline read the Olist orders CSV (local path), attempted to enrich rows with historical BRL→USD exchange rates, and saved outputs to `pipeline_outputs/`.

Important reproducibility note
- The exchange-rate API returned nulls for the enrichment during my run (see `pipeline_outputs/rates_cache.json`), so for reproducibility I produced monthly totals using a fixed conversion rate (1 BRL = 0.25 USD). Files to inspect:
  - `pipeline_outputs/enriched_orders.csv` — enriched dataset from the flow (brl_to_usd_rate and payment_usd columns may be null in this run)
  - `pipeline_outputs/monthly_sales_brl_minimal.csv` — monthly totals in BRL
  - `pipeline_outputs/monthly_sales_usd_fixedrate_minimal.csv` — monthly USD totals computed using the fixed rate (0.25 BRL→USD)

Notes and how to reproduce locally
1. Place your Olist CSV in a local path and confirm its location (or use the default path set in `prefect_olist_pipeline.py`).
2. Install requirements:
   pip install -r requirements.txt
3. Run the pipeline from a notebook (sample first, then full run) or:
   python prefect_olist_pipeline.py
4. Check outputs in `pipeline_outputs/`.

Data policy
- The large CSV is intentionally excluded from the repository (see `.gitignore`) to avoid committing big files.

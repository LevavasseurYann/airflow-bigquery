# ============================================================================
# Custom Airflow image for the airflow-bigquery data platform.
#
# Why a custom image instead of `_PIP_ADDITIONAL_REQUIREMENTS`?
#   Installing dependencies at container startup (the quick-start shortcut) runs
#   on EVERY boot and is not reproducible. Baking them into an image is the
#   production-grade approach recommended by the Airflow project.
#
# The image is shared by every service (api-server, scheduler, dag-processor,
# worker, triggerer) so dbt, Cosmos and the hr_pipeline package are available
# uniformly across the cluster.
# ============================================================================
FROM apache/airflow:3.2.2-python3.12

# ─── System packages (root) ─────────────────────────────────────────────────
# build-essential: some wheels still compile from source on first install.
# git: lets dbt resolve git-based packages if the project ever adds any.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# ─── Python dependencies ────────────────────────────────────────────────────
# Copied and installed first so Docker layer caching is not invalidated by
# unrelated source changes.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ─── hr_pipeline package (editable install) ─────────────────────────────────
# Installed editable (-e) so the bind-mounted ./src is picked up live in dev.
# In a real deployment you would build a versioned wheel instead.
COPY --chown=airflow:0 pyproject.toml README.md /opt/airflow/
COPY --chown=airflow:0 src /opt/airflow/src
RUN pip install --no-cache-dir -e /opt/airflow

# ─── Build-time sanity check ────────────────────────────────────────────────
# Fail the build early if the core imports are broken.
RUN python -c "import cosmos, duckdb, pandas, hr_pipeline; print('image dependencies OK')"

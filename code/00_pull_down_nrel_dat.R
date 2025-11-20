library(DBI)
library(duckdb)
library(tidyverse)
library(glue)
library(arrow)
library(ggiraph)

pums_08_path <-
  c(
    'C:/Users/js5466/OneDrive - Drexel University/r_master/new_projects/HEAT-Housing/recs_pums_master/workflow_dat/clean'
  )


pums_08 <-
  open_dataset(glue('{pums_08_path}/pums_clim_reg_cl.parquet'))


# Create a DuckDB connection
con <- dbConnect(duckdb::duckdb(), here::here('data/nrel.duckdb'))


## Pre-reqs to connect to NREL's S3 bucket
dbExecute(con, "INSTALL httpfs;")
dbExecute(con, "LOAD httpfs;")
dbExecute(con, 'INSTALL zipfs FROM community')
dbExecute(con, 'LOAD zipfs')


## Where data lives on the bucket...
path_to_annual_results <-
  c(
    's3://oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2025/resstock_amy2018_release_1/metadata_and_annual_results/national/full/parquet/upgrade28.parquet'
  )

## Where individual building info lives on the bucket...
path_to_bld_model <-
  c(
    "s3://oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2025/resstock_amy2018_release_1/building_energy_models/upgrade=28/bldg0000001-up28.zip"
  )


## ----------------------------------------------------------------------------=
# Let's connect & create tables  ----
## ----------------------------------------------------------------------------=
dbExecute(
  con,
  glue_sql(
    "CREATE TABLE ann_results_upgrade_28 AS
          SELECT * FROM {path_to_annual_results}",
    .con = con
  )
)

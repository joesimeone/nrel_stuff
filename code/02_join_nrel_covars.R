library(tidyverse)
library(duckdb)


## ----------------------------------------------------------------------------=
# Imports  ----
## ----------------------------------------------------------------------------=

## Where clean NREL data lives
con <- dbConnect(duckdb(), 'data/nrel.duckdb')
dbListTables(con)


## Where (currently) cdd days for select states and 2010 pumas covariates live
broad_covars_files <-
  list.files(
    here::here('data', 'workflow_dat'),
    pattern = '.rds',
    full.names = TRUE
  )

## Degree day covariates
cdd_covars <-
  readRDS(
    here::here('data', 'workflow_dat', 'cdd_avg_state_sample.rds')
  )

## PUMs covariates
pums_covars <-
  readRDS(
    here::here('data', 'workflow_dat', 'puma_2019_5yr_covars.rds')
  )


## ---------------------------------------------------------------------------=
# Join Covars w/ nrel data ----
## ---------------------------------------------------------------------------=

## Ingest covars data temporarily into db to facilitate join
dbWriteTable(
  con,
  'pums_covars',
  pums_covars,
  temporary = TRUE,
  overwrite = TRUE
)

dbWriteTable(con, 'cdd_covars', cdd_covars, temporary = TRUE, overwrite = TRUE)

## db lazy talbes
nrel_cl <-
  tbl(con, 'nrel_clean')

pums_db <-
  tbl(con, 'pums_covars')

cdd_db <-
  tbl(con, 'cdd_covars')

## Join data on geoid codes
nrel_joined_dat <-
  nrel_cl |>
  left_join(pums_db, by = c('in_puma' = 'GISJOIN')) |>
  left_join(cdd_db, by = c('in_county' = 'GISJOIN')) |>
  collect() |>
  janitor::clean_names()


if (!c('nrel_covars_fin') %in% dbListTables(con)) {
  dbWriteTable(con, 'nrel_covars_fin', nrel_joined_dat)
} else {
  print('Table already written')
}

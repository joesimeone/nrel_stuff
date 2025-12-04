library(tidyverse)
library(duckdb)
library(glue)


con <- dbConnect(duckdb(), 'data/nrel.duckdb')

dbListTables(con)


pums_hh_paths <-
  list.files(
    here::here(
      'data',
      'pums'
    ),
    pattern = 'psam_h',
    full.names = TRUE
  )

pums_ppl_paths <-
  list.files(
    here::here(
      'data',
      'pums'
    ),
    pattern = 'psam_p',
    full.names = TRUE
  )


# Households --------------------------------------------------------------
if (!c('pums_19_5yr') %in% dbListTables(con)) {
  ## Not sure why, but needed to
  dbExecute(
    con,
    "CREATE TABLE pums_19_5yr AS 
  SELECT * FROM read_csv('data/pums/*.csv', 
    types={'SERIALNO': 'VARCHAR'})"
  )
} else {
  cat(
    'PUMs household files written to db. Deleting csvs. 
        \n If something weird happened / happens, you can retrieve them from the zip file at data/pums path'
  )

  if (any(file.exists(pums_hh_paths))) {
    fs::file_delete(pums_hh_paths)
  }
}


# People ------------------------------------------------------------------
if (!c('pums_19_ppl_5yr') %in% dbListTables(con)) {
  ## Not sure why, but needed to
  dbExecute(
    con,
    "CREATE TABLE pums_19_ppl_5yr AS 
   SELECT * FROM read_csv('data/pums/*.csv', 
    types={'SERIALNO': 'VARCHAR'})"
  )
} else {
  cat(
    'PUMs household files written to db. Deleting csvs. 
        \n If something weird happened / happens, you can retrieve them from the zip file at data/pums path'
  )

  if (any(file.exists(pums_ppl_paths))) {
    fs::file_delete(pums_ppl_paths)
  }
}

fs::file_delete(pums_hh_paths)

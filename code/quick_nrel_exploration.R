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
con <- dbConnect(duckdb::duckdb())


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


dbExecute(
  con,
  glue::glue(
    "CREATE TABLE bld1_samp AS 
  SELECT * 
  FROM read_csv_auto('zip://{path_to_bld_model}/*.csv')"
  )
)


## ----------------------------------------------------------------------------=
# Let's look at NREL tables ----
## ----------------------------------------------------------------------------=

annual_dat <-
  tbl(con, 'ann_results_upgrade_28')


bld_1_dat <-
  tbl(con, 'bld1_samp')

## For validation, here's what I don't get -----

## Let's see what's available in delaware
de_ann_dat <-
  annual_dat %>%
  filter(in.state == 'DE') %>%
  collect() %>%
  janitor::clean_names()

## There are a few hundred buildings in each puma, but in actuality,
## there are quite a lot many more...
de_ann_dat %>%
  summarise(n = n(), .by = c(in_puma))


## Here's the same for households in pums
de_households <-
  pums_08 %>%
  filter(ST == '10') %>%
  summarise(n = n(), n_pop = sum(WGTP), .by = unique_puma) %>%
  collect()


## That's actually... not as different as I thought lol, although
## I don't know how to deal with weights right

## Let's look everything real quick
## Counts
library(tibble)

state_fips_tbl <- tibble::tibble(
  state = c(
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC"
  ),
  fips = c(
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
    "11"
  )
)

state_fips_tbl


nrel_hh <-
  annual_dat %>%
  filter(in.vacancy_status == 'Occupied') %>%
  summarise(
    n_rel = n(),
    .by = c(in.state)
  ) %>%
  arrange(in.state) %>%
  collect() %>%
  left_join(state_fips_tbl, by = c('in.state' = 'state')) %>%
  arrange(in.state)


pums_hh <-
  pums_08 %>%
  summarise(
    n_pums = n(),
    n_pop_pums = sum(WGTP),
    .by = c('ST')
  ) %>%
  arrange(ST) %>%
  collect() %>%
  left_join(state_fips_tbl, by = c("ST" = "fips")) %>%
  arrange(state)


nrel_plot <-
  ggplot(nrel_hh, aes(in.state, n_rel)) +
  geom_col_interactive(
    aes(tooltip = n_rel),
    fill = '#394165'
  ) +
  scale_y_continuous(limits = c(0, 90000)) +
  coord_flip() +
  theme_minimal()


pums_plot <-
  ggplot(pums_hh, aes(state, n_pums)) +
  geom_col_interactive(
    aes(tooltip = n_pums),
    fill = "#A65141"
  ) +
  coord_flip() +
  theme_minimal()


combo_plots_int <-
  patchwork::wrap_plots(nrel_plot, pums_plot)


girafe(ggobj = combo_plots_int, height = 8, width = 8)

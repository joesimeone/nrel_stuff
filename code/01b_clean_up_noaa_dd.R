library(tidyverse)
library(sf)

## ----------------------------------------------------------------------------=
# Imports ----
## ----------------------------------------------------------------------------=

ipums_co <-
  read_sf(
    here::here(
      'data',
      'ipums',
      'nhgis0005_shape',
      'nhgis0005_shapefile_tl2010_us_county_2010',
      'US_county_2010.shp'
    )
  ) |>
  st_drop_geometry()

## IPUMS 2010 File for GISJOIN Field

tst_states <-
  c(
    'IL',
    'IN',
    'CA',
    'AZ',
    'NM',
    'TX',
    'FL',
    'CA',
    'WA',
    'NY',
    'PA',
    'DE',
    'MN',
    'ND'
  )
## Regex to match up with our test states for the moment
#file_pattern <- paste('cdd_', sep = "", tst_states, collapse = '|')

noaa_cdd_files <-
  list.files(
    here::here('data', 'noaa', 'intermediate'),
    full.names = TRUE,
    pattern = 'cdd_'
  )

noaa_hdd_files <-
  list.files(
    here::here('data', 'noaa', 'intermediate'),
    full.names = TRUE,
    pattern = 'hdd_'
  )


## States that we've pulled data for
state_abbrev <- sub(".*_([A-Z]{2})-.*", "\\1", basename(noaa_hdd_files))
length(unique(state_abbrev))

noaa_cdd_csvs <-
  map(noaa_cdd_files, read_csv, show_col_types = FALSE) |>
  list_rbind()

noaa_hdd_csvs <-
  map(noaa_hdd_files, read_csv, show_col_types = FALSE) |>
  list_rbind()

## In X county in year Y, there were Z total degree days
noaa_cdd_summaries <-
  noaa_cdd_csvs |>
  mutate(year = year(date_formatted)) |>
  summarise(ann_cdd_days = sum(value), .by = c(year, county, metric))


## Join w/ hdds on left hand side will return only counties that are in 
## cleaned NREL data 
noaa_hdd_summaries <-
  noaa_hdd_csvs |>
  mutate(year = year(date_formatted)) |>
  summarise(ann_hdd_days = sum(value), .by = c(year, county, metric))

noaa_summaries <-
  noaa_cdd_summaries |> 
  left_join(noaa_hdd_summaries, by = c('year', 'county'))

## In X county, there was an average of Z total degree days between 1970 - 2019
degree_day_avgs <-
  noaa_summaries |>
  summarise(
    mean_cdd = mean(ann_cdd_days),
    mean_hdd = mean(ann_cdd_days),
    .by = c(county)
  )

## ----------------------------------------------------------------------------=
# Clean up the summarized data  ----
## ----------------------------------------------------------------------------=
## Get State abbs
state_fips_tbl <- tibble(
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

## Clean up data and add in some cleaner geographic identifers
degree_day_avgs_cl <-
  degree_day_avgs |>
  separate_wider_delim(
    c(county),
    delim = '-',
    names = c('st_abb', 'co_fips')
  ) |>
  left_join(state_fips_tbl, by = c('st_abb' = 'state')) |>
  mutate(geoid = glue::glue('{fips}{co_fips}')) |>
  left_join(
    ipums_co,
    by = c('geoid' = 'GEOID10')
  )


saveRDS(
  degree_day_avgs_cl,
  here::here('data', 'workflow_dat', 'dd_avg_state_sample.rds')
)
library(tidyverse)
library(httr2)
library(glue)
#library(tigris)
library(sf)
library(duckdb)

## Database connection, where clean NREL data lives
con <- dbConnect(duckdb(), here::here('data', 'nrel.duckdb'), read_only = TRUE)

dbListTables(con)

## Create db lazy table
nrel_clean <- tbl(con, 'nrel_clean')

## Helps us line up GEOID and GISGEOID down stream
ipums_co_2010 <-
  read_sf(
    here::here('data', 'ipums', 'us_county_2010.shp')
  ) |>
  st_drop_geometry() |>
  select(GEOID10, GISJOIN, ALAND10) |>
  as_tibble()


### NOTE: This is set up theorteically to get all cdds and hdds for every county at
## once (from 1970 - 2019), but I think it's a lot too request at once for NOAA.
## So I wound up doing a lot of filtering and writing a quick thing that chekcs if
## a file is already present in the the directory where I've stuck the... bajillion
## csvs

build_degree_day_call <- function(metric, county) {
  ## Path to NOAA API
  url_glue_string <-
    glue(
      "https://www.ncei.noaa.gov/access/monitoring/climate-at-a-glance/county/time-series/{county}/{metric}/12/0/1970-2019/data.json"
    )

  ## Asks website to get their data | Adds some additional arguments to try not to be a jerk
  api_query <-
    request(url_glue_string) |>
    req_throttle(rate = 3 / 10) |> ## 3 requests per 10 seconds (slower)
    req_retry(
      max_tries = 5, ## More attempts
      is_transient = ~ httr2::resp_status(.x) == 429 ||
        httr2::resp_status(.x) >= 500,
      backoff = ~ runif(1, 3, 10) ## Random 3-10 second wait between retries
    ) |>
    req_timeout(120) ## Double the timeout to 2 minutes

  return(api_query)
}

# Query Arguments --------------------------------------------------------

co_cdd_formatted <-
  nrel_clean %>%
  mutate(
    sample_group = case_when(
      in_ashrae_iecc_climate_zone_2004 == '7B' ~ 'Cold, Very Cold Dry',
      in_ashrae_iecc_climate_zone_2004 == '7A' ~ 'Cold, Very Cold Humid',
      in_ashrae_iecc_climate_zone_2004 == "1A" ~ "Hot, Very Hot Humid",
      in_ashrae_iecc_climate_zone_2004 == "2A" ~ "Hot, Very Hot Humid",
      in_ashrae_iecc_climate_zone_2004 == "2B" ~ "Hot Dry",
      in_ashrae_iecc_climate_zone_2004 == "3A" ~ "Warm Humid",
      in_ashrae_iecc_climate_zone_2004 == "3B" ~ "Warm Dry",
      in_ashrae_iecc_climate_zone_2004 == "3C" ~ "Warm Marine",
      in_ashrae_iecc_climate_zone_2004 == "4A" ~ "Mixed Humid",
      in_ashrae_iecc_climate_zone_2004 == "4B" ~ "Mixed Dry",
      in_ashrae_iecc_climate_zone_2004 == "4C" ~ "Mixed Marine",
      in_ashrae_iecc_climate_zone_2004 == "5A" ~ "Cool Humid",
      in_ashrae_iecc_climate_zone_2004 == "5B" ~ "Cool Dry",
      in_ashrae_iecc_climate_zone_2004 == "5C" ~ "Cool Marine",
      in_ashrae_iecc_climate_zone_2004 == "6A" ~ "Cold, Very Cold Humid",
      in_ashrae_iecc_climate_zone_2004 == "6B" ~ "Cold, Very Cold Dry"
    )
  ) |>
  distinct(sample_group, in_state, in_county) |>
  collect() |>
  left_join(ipums_co_2010, by = c('in_county' = 'GISJOIN')) |>
  mutate(
    county_arg = str_sub(GEOID10, 3, 5),
    county_arg = str_glue('{in_state}-{county_arg}'),
    metric = 'cdd'
  )


co_hdd_formatted <-
  nrel_clean %>%
  mutate(
    sample_group = case_when(
      in_ashrae_iecc_climate_zone_2004 == '7B' ~ 'Cold, Very Cold Dry',
      in_ashrae_iecc_climate_zone_2004 == '7A' ~ 'Cold, Very Cold Humid',
      in_ashrae_iecc_climate_zone_2004 == "1A" ~ "Hot, Very Hot Humid",
      in_ashrae_iecc_climate_zone_2004 == "2A" ~ "Hot, Very Hot Humid",
      in_ashrae_iecc_climate_zone_2004 == "2B" ~ "Hot Dry",
      in_ashrae_iecc_climate_zone_2004 == "3A" ~ "Warm Humid",
      in_ashrae_iecc_climate_zone_2004 == "3B" ~ "Warm Dry",
      in_ashrae_iecc_climate_zone_2004 == "3C" ~ "Warm Marine",
      in_ashrae_iecc_climate_zone_2004 == "4A" ~ "Mixed Humid",
      in_ashrae_iecc_climate_zone_2004 == "4B" ~ "Mixed Dry",
      in_ashrae_iecc_climate_zone_2004 == "4C" ~ "Mixed Marine",
      in_ashrae_iecc_climate_zone_2004 == "5A" ~ "Cool Humid",
      in_ashrae_iecc_climate_zone_2004 == "5B" ~ "Cool Dry",
      in_ashrae_iecc_climate_zone_2004 == "5C" ~ "Cool Marine",
      in_ashrae_iecc_climate_zone_2004 == "6A" ~ "Cold, Very Cold Humid",
      in_ashrae_iecc_climate_zone_2004 == "6B" ~ "Cold, Very Cold Dry"
    )
  ) |>
  distinct(sample_group, in_state, in_county) |>
  collect() |>
  left_join(ipums_co_2010, by = c('in_county' = 'GISJOIN')) |>
  mutate(
    county_arg = str_sub(GEOID10, 3, 5),
    county_arg = str_glue('{in_state}-{county_arg}'),
    metric = 'hdd'
  )

# Finish up argument prep -------------------------------------------------

## Takes a metric, and county argument, but we don't want to re-pull
## files from the API, and we want to go little by little because the API
## keeps timing me out.

get_redund_files <-
  list.files(
    here::here('data', 'noaa', 'intermediate')
  )

redund_files <-
  str_remove(get_redund_files, '.csv')

cdd_args <-
  co_cdd_formatted |>
  mutate(exists_filter = glue::glue('{metric}_{county_arg}')) |>
  filter(!exists_filter %in% redund_files) |>
  filter(sample_group == 'Cold, Very Cold Dry', county_arg != 'HI-NA') |>
  select(metric, county_arg)

hdd_args <-
  co_hdd_formatted |>
  mutate(exists_filter = glue::glue('{metric}_{county_arg}')) |>
  filter(!exists_filter %in% redund_files) |>
  filter(
    sample_group == 'Cool Dry',
    county_arg != 'SD-NA'
  ) |>
  select(metric, county_arg)


tx_args <-
    co_hdd_formatted |> 
  filter(in_state == 'TX')
# chima_args <-
#   c(
#     'IL-031',
#     'IN-097',
#     'CA-037',
#     'AZ-013',
#     'NM-001',
#     'TX-029',
#     'TX-201',
#     'CA-075',
#     'CA-085',
#     'WA-033',
#     'NY-047',
#     'NY-061',
#     'NY-081',
#     'MN-053',
#     'MN-137',
#     'ND-017'
#   )

# cdd_chima <-
#   co_cdd_formatted |>
#   filter(county_arg %in% chima_args) |>
#   select(county_arg, metric)
#
#
# hdd_chima <-
#   co_hdd_formatted |>
#   filter(county_arg %in% chima_args) |>
#   select(county_arg, metric)

# Call functions  --------------------------------------------------------

pwalk(cdd_args, function(county_arg, metric) {
  cli::cli_alert('Trying noaa for {county_arg}:')

  noaa_json <-
    build_degree_day_call(county = county_arg, metric = metric) |>
    req_perform() |>
    resp_body_json(simplifyVector = FALSE)

  cli::cli_alert('Cleaning up the output {county_arg}:')

  noaa_coerced <-
    noaa_json$data |>
    imap_dfr(~ data.frame(date = .y, value = .x, stringsAsFactors = FALSE)) |>
    mutate(date_formatted = ym(date), county = county_arg) |>
    rename(!!sym(glue('{metric}')) := value) |>
    mutate(metric = 'cdd') |>
    rename(value = cdd)

  write_csv(
    noaa_coerced,
    here::here(
      'data',
      'noaa',
      'intermediate',
      glue::glue('cdd_{county_arg}.csv')
    )
  )

  Sys.sleep(runif(1, 1, 3))
})


pwalk(hdd_args, function(county_arg, metric) {
  cli::cli_alert('Trying noaa for {county_arg}:')

  noaa_json <-
    build_degree_day_call(county = county_arg, metric = metric) |>
    req_perform() |>
    resp_body_json(simplifyVector = FALSE)

  cli::cli_alert('Cleaning up the output {county_arg}:')

  noaa_coerced <-
    noaa_json$data |>
    imap_dfr(~ data.frame(date = .y, value = .x, stringsAsFactors = FALSE)) |>
    mutate(date_formatted = ym(date), county = county_arg) |> # Add this line!
    rename(!!sym(glue('{metric}')) := value) |>
    mutate(metric = 'hdd') |>
    rename(value = hdd)

  write_csv(
    noaa_coerced,
    here::here(
      'data',
      'noaa',
      'intermediate',
      glue::glue('hdd_{county_arg}.csv')
    )
  )

  Sys.sleep(runif(1, 1, 3))
})

# cdd_data_fin <- list_rbind(cdd_data) |>
#   mutate(metric = 'cdd') |>
#   rename(value = cdd)
#
# hdd_data_fin <- list_rbind(hdd_data) |>
#   mutate(metric = 'hdd') |>
#   rename(value = hdd)

# degree_days_fin <-
#   rbind(
#     cdd_data_fin,
#     hdd_data_fin
#   )
#
# write_csv(degree_days_fin, 'data/noaa/degree_days_1970_2019.csv')

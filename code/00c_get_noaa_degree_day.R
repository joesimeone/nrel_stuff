library(tidyverse)
library(httr2)
library(glue)
library(tigris)

options(tigris_use_cache = TRUE)
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

## To help us make the api call more iterative

## Get county info
co_info <- counties(year = 2010) |>
  sf::st_drop_geometry() |>
  as_tibble()

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

co_cdd_formatted <-
  co_info |>
  left_join(state_fips_tbl, by = c('STATEFP10' = 'fips')) |>
  select(state, COUNTYFP10) |>
  mutate(county_arg = glue('{state}-{COUNTYFP10}'), metric = 'cdd') |>
  filter(!state %in% c('AK', 'HI')) |>
  select(state, county_arg, metric) |>
  left_join(state_fips_tbl, by = c('state')) |>
  arrange(state, county_arg)

co_hdd_formatted <-
  co_info |>
  left_join(state_fips_tbl, by = c('STATEFP10' = 'fips')) |>
  select(state, COUNTYFP10) |>
  mutate(county_arg = glue('{state}-{COUNTYFP10}'), metric = 'hdd') |>
  filter(!state %in% c('AK', 'HI')) |>
  select(state, county_arg, metric) |>
  arrange(county_arg) |>
  select(state, county_arg, metric) |>
  left_join(state_fips_tbl, by = c('state')) |>
  arrange(state, county_arg)


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
  filter(fips == '48' & !exists_filter %in% redund_files) |>
  select(metric, county_arg)

hdd_args <-
  co_hdd_formatted |>
  mutate(exists_filter = glue::glue('{metric}_{county_arg}')) |>
  filter(fips == '18' & !exists_filter %in% redund_files) |>
  select(metric, county_arg)

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


pwalk(hdd_chima, function(county_arg, metric) {
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

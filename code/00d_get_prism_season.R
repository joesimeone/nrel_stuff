library(arrow)
library(tidyverse)
library(sf)


## Where county data lives | 1 big parquet files
prism_co_path <-
  "//files.drexel.edu/colleges/SOPH/Shared/UHC/Projects/CCUH_read_only_access/PRISM/prism_zonal_stats_v1/clean/county10.parquet"


## Open dataset to prism county UHC resource
prism_co <-
  open_dataset(prism_co_path)


## For eventual join w/ nrel GISJOIN Field
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


## ----------------------------------------------------------------------------=
# Derive seasonal metrics ----
## ----------------------------------------------------------------------------=

prism_county_szn <-
  prism_co |>
  filter(year %in% c(2008:2019), measure %in% c('tmean', 'tmax')) |>
  mutate(
    day = day(date),
    month = month(date),
    astro_season = case_when(
      (month == 1) | (month == 2) | (month == 3 & day <= 19) ~ "WINTER",
      (month == 12 & day >= 21) ~ "WINTER",
      (month == 3 & day >= 20) |
        (month == 4) |
        (month == 5) |
        (month == 6 & day <= 19) ~
        "SPRING",
      (month == 6 & day >= 20) |
        (month == 7) |
        (month == 8) |
        (month == 9 & day <= 21) ~
        "SUMMER",
      (month == 9 & day >= 22) |
        (month == 10) |
        (month == 11) |
        (month == 12 & day <= 20) ~
        "FALL",
      TRUE ~ "WHY"
    )
  ) |>
  summarise(
    mean = mean(value),
    .by = c('state', 'geoid', 'astro_season', 'measure')
  ) |>
  collect()


prism_county_fin <-
  prism_county_szn |>
  pivot_wider(
    names_from = c('measure'),
    values_from = c('mean')
  ) |>
  left_join(
    ipums_co,
    by = c('geoid' = 'GEOID10')
  )


write_csv(
  prism_county_fin,
  here::here(
    'data',
    'prism',
    'prism_county_szn_08_19.csv'
  )
)

fs::dir_create('data', 'prism')

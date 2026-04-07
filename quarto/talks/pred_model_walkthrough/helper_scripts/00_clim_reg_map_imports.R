options(tigris_use_cache = TRUE)

clim_zones <-
  read_csv(
    "C:/Users/js5466/OneDrive - Drexel University/heat_study_prism_data/resources/climate_zones.csv"
  ) %>%
  janitor::clean_names() %>%
  mutate(co_geoid = glue::glue('{state_fips}{county_fips}'))

co_2010 <-
  read_sf(
    here::here('data', 'tigris', 'tl_2010_us_county10.shp')
  )

territories <-
  clim_zones |>
  anti_join(co_2010, by = c('co_geoid' = 'GEOID10')) |>
  select(co_geoid) |>
  pull()


ipums_pums_2010 <-
  read_sf(
    here::here('data', 'ipums', 'ipums_puma_2010', 'ipums_puma_2010.shp')
  ) |>
  st_drop_geometry() |>
  select(GEOID, GISJOIN) |>
  as_tibble()

ipums_co_2010 <-
  read_sf(
    here::here('data', 'ipums', 'us_county_2010.shp')
  ) |>
  st_drop_geometry() |>
  select(GEOID10, GISJOIN, ALAND10) |>
  as_tibble()


clim_zone_classes_iecc <-
  read_csv(here::here(
    'quarto',
    'spend_sample_exploration',
    'data',
    'iecc_clim_zone_classifications.txt'
  ))

chima_tbl <-
  readxl::read_xlsx(here::here(
    'quarto',
    'spend_sample_exploration',
    'data',
    'chima_dis_table .xlsx'
  )) |>
  janitor::clean_names()

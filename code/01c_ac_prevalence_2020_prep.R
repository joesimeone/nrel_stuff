library(tidyverse)
library(sf)


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


ac_uno_estimates <-
  read_csv(
    here::here(
      'data',
      'summary_tract_adjusted_2020.csv'
    ),
    col_types = cols(GEOIDCN = 'character', GEOID = 'character')
  )


ac_est <-
  ac_uno_estimates |>
  mutate(
    ac_binary = if_else(ACtype %in% c('Central', 'Other'), 'AC', 'NO AC')
  ) |>
  summarise(
    mean_ac_pct_prev = mean(Adjusted_Percentage, na.rm = TRUE),
    .by = c(GEOIDCN, ac_binary)
  ) |>
  filter(
    ac_binary == 'AC'
  )


ac_use_tst <-
  ac_est |>
  left_join(
    ipums_co,
    by = c('GEOIDCN' = 'GEOID10')
  )

ac_use_tst <-
  ac_use_tst |>
  select(GISJOIN, mean_ac_pct_prev)


write_csv(
  ac_use_tst,
  here::here(
    'data',
    'ac_prev_est',
    'ac_county_estimates_cl_2020.csv'
  )
)

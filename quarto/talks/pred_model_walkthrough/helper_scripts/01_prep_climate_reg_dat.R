## Calcualte Housing Density per square mile ----
co_housing_density <-
  nrel_main |>
  filter(!in_state %in% c('AK', 'HI')) |>
  summarise(
    n_homes = n(),
    n_wgt_homes = sum(weight),
    .by = in_county
  ) |>
  collect() |>
  left_join(
    ipums_co_2010,
    by = c('in_county' = 'GISJOIN')
  ) |>
  mutate(house_density_per_sqmi = n_wgt_homes / (ALAND10 / 2589988.11))


## Prepare Climate region data for mapping
clim_reg_sf <-
  clim_zones |>
  filter(!co_geoid %in% territories) |>
  filter(!state_fips %in% c('02', '15')) |>
  left_join(co_2010, by = c('co_geoid' = 'GEOID10')) |>
  left_join(
    co_housing_density |>
      select(GEOID10, n_homes, n_wgt_homes, house_density_per_sqmi),
    by = c('co_geoid' = 'GEOID10')
  ) |>
  mutate(across(where(is.character), ~ iconv(., "UTF-8", "UTF-8", sub = ""))) |> ## Maplibre doesn't like one of the columns
  mutate(
    climate_region = case_when(
      ba_climate_zone %in% c("Cold", "Very Cold") ~ "Cold & Very Cold",
      ba_climate_zone %in% c("Hot-Dry", "Mixed-Dry") ~ "Hot-Dry & Mixed Dry",
      TRUE ~ ba_climate_zone
    ),
    iecc_climate_region = str_glue('{iecc_climate_zone}{iecc_moisture_regime}'),
    iecc_climate_region_str = case_when(
      iecc_climate_region == "1A" ~ "Very Hot Humid (1A)",
      iecc_climate_region == "2A" ~ "Hot Humid (2A)",
      iecc_climate_region == "2B" ~ "Hot Dry (2B)",
      iecc_climate_region == "3A" ~ "Warm Humid (3A)",
      iecc_climate_region == "3B" ~ "Warm Dry (3B)",
      iecc_climate_region == "3C" ~ "Warm Marine (3C)",
      iecc_climate_region == "4A" ~ "Mixed Humid (4A)",
      iecc_climate_region == "4B" ~ "Mixed Dry (4B)",
      iecc_climate_region == "4C" ~ "Mixed Marine (4C)",
      iecc_climate_region == "5A" ~ "Cool Humid (5A)",
      iecc_climate_region == "5B" ~ "Cool Dry (5B)",
      iecc_climate_region == "5C" ~ "Cool Marine (5C)",
      iecc_climate_region == "6A" ~ "Cold Humid (6A)",
      iecc_climate_region == "6B" ~ "Cold Dry (6B)",
      iecc_climate_region == "7N/A" ~ "Very Cold (7)",
      iecc_climate_region == "8" ~ "Subarctic (8)",
      TRUE ~ iecc_climate_region
    )
  ) |>
  mutate(
    climate_region = as.factor(climate_region),
    iecc_climate_region = as.factor(iecc_climate_region),
    iecc_climate_region_fac = as.factor(iecc_climate_region_str)
  ) |>
  select(
    state,
    co_geoid,
    NAMELSAD10,
    climate_region,
    iecc_climate_region,
    iecc_climate_region_fac,
    n_homes,
    n_wgt_homes,
    house_density_per_sqmi,
    geometry
  ) |>
  st_as_sf()


clim_reg_sf <-
  clim_reg_sf |>
  mutate(
    ip_units = case_when(
      iecc_climate_region == '1A' ~
        'More than 9,000 Cooling Degree Days (Base 50F)',
      iecc_climate_region %in% c('2A', '2B') ~
        'Between 6,300 & 9,000 Cooling Degree Days (Base 50F)',
      iecc_climate_region %in% c('3A', '3B') ~
        'Between 4,500 & 6,300 Cooling Degree Days Base (50F) | No more than 5,400 Heating Degree Days (Base 65F)',
      iecc_climate_region %in% c('4A', '4B') ~
        'At least 4,500 Cooling Degree Days (Base 50F) & N more than 5,400 Heating Degree Days (Base 65F)',
      iecc_climate_region == '3C' ~
        'At least 3,600 Heating Degree Days (Base 65F)',
      iecc_climate_region == '4C' ~
        'Between 3,600 & 5,400 Heating Degree Days (Base 65F)',
      iecc_climate_region %in% c('5A', '5B') ~
        'Between 5,400 & 7,200 Heating Degree Days (Base 65F)',
      iecc_climate_region %in% c('6A', '6B') ~
        'Between 7,200 & 9,000 Heating Degree Days (Base 65F)',
      iecc_climate_region == '7N/A' ~
        'Between 9,000 & 12,600 Heating Degree Days (Base 65F)',
      TRUE ~ 'Something Wrong'
    )
  )


## Formatting numeric variables for map popup
clim_reg_sf <-
  clim_reg_sf |>
  mutate(
    n_homes = round(n_homes, 3),
    n_wgt_homes = round(n_wgt_homes),
    house_density_per_sqmi_format = round(house_density_per_sqmi, 3),
    n_homes_format = format(n_homes, big.mark = ","),
    n_wgt_homes_format = format(n_wgt_homes, big.mark = ",")
  )

## Hopefully makes this less of a memory hog...
clim_reg_sf <- st_simplify(clim_reg_sf, dTolerance = 25)

st_broad_boundaries <-
  clim_reg_sf |>
  group_by(state) |>
  summarise(new_geom = st_union(geometry))

library(tidyverse)
library(duckdb)


con <- dbConnect(duckdb(), 'data/nrel.duckdb')

dbListTables(con)


nrel_dat <- tbl(con, 'ann_results_upgrade_28')

nrel_narrowed <-
  nrel_dat %>%
  filter(
    #   in.state %in% c('IL', 'IN', 'CA', 'AZ', 'NM', 'TX', 'NY', 'MN', 'ND'),
    in.vacancy_status == "Occupied",
    in.building_america_climate_zone != 'Subarctic'
  ) %>%

  ## Predictors
  select(
    bldg_id,
    weight,
    in.occupants,
    in.building_america_climate_zone,
    in.state,
    in.county_name,
    in.puma,
    in.bedrooms,
    in.geometry_building_type_acs,
    in.federal_poverty_level,
    in.heating_fuel,
    in.income,
    in.income_recs_2015,
    in.income_recs_2020,
    in.vintage,
    in.vintage_acs,
    in.tenure,

    ## Is using estimates in the model cool??
    out.utility_bills.electricity_bill..usd,
    out.utility_bills.natural_gas_bill..usd,
    out.utility_bills.total_bill..usd,

    ## Target Variables
    out.site_energy.total.energy_consumption..kwh,
    out.load.cooling.energy_delivered..kbtu,
    out.load.heating.energy_delivered..kbtu,
    in.windows,
    in.insulation_roof,
    in.geometry_attic_type,
    in.insulation_ceiling,
    in.insulation_wall,
    in.cooling_setpoint,
    in.heating_setpoint,
    in.air_leakage_to_outside_ach50,
    in.infiltration,
    in.roof_material,
    in.geometry_wall_exterior_finish,
    in.hvac_cooling_type,
    in.hvac_heating_type
  ) %>%
  collect() %>%
  janitor::clean_names()


nrel_dat %>%

  glimpse()


## ----------------------------------------------------------------------------=
# Predictors ----
## ----------------------------------------------------------------------------=
nrel_dat_miss <-
  nrel_narrowed %>%
  summarise(across(everything(), ~ sum(is.na(.)))) %>%
  pivot_longer(cols = everything(), names_to = 'miss', values_to = 'miss2')

nrel_dat_char <-
  nrel_narrowed %>%
  select(where(is.character)) %>%
  select(-in_occupants, -in_county_name, -in_puma, -in_state, -in_bedrooms) %>%
  names()


map(nrel_dat_char, ~ janitor::tabyl(nrel_narrowed, .x))

glimpse(nrel_narrowed)
janitor::tabyl(nrel_narrowed, in_geometry_building_type_acs)


nrel_cleaning <-
  nrel_narrowed %>%
  filter(
    !in_geometry_building_type_acs %in%
      c('2 Unit', '3 or 4 unit', 'Mobile Home')
  ) %>%
  mutate(
    x_year_broad = case_when(
      in_vintage_acs == '<1940' ~ ' Before 1940',
      in_vintage_acs %in% c('1940-59', '1960-79') ~ ' 1940 - 1980',
      TRUE ~ ' After 1980'
    ),
    x_hhtype = case_when(
      in_geometry_building_type_acs == 'Single-Family Detached' ~
        " single family detached",
      in_geometry_building_type_acs == 'Single-Family Attached' ~
        " single family attached",
      TRUE ~ " apartment in building with 5+ units"
    ),
    x_hhincome = case_when(
      in_income == "<10000" ~ " less than $10,000",

      in_income == "10000-14999" ~ " $10,000 to $19,999",
      in_income == "15000-19999" ~ " $10,000 to $19,999",

      in_income == "20000-24999" ~ " $20,000 to $29,999",
      in_income == "25000-29999" ~ " $20,000 to $29,999",

      in_income == "30000-34999" ~ " $30,000 to $39,999",
      in_income == "35000-39999" ~ " $30,000 to $39,999",

      in_income == "40000-44999" ~ " $40,000 to $49,999",
      in_income == "45000-49999" ~ " $40,000 to $49,999",

      in_income == "50000-59999" ~ " $50,000 to $59,999",
      in_income == "60000-69999" ~ " $60,000 to $69,999",
      in_income == "70000-79999" ~ " $70,000 to $79,999",

      # Combined 80k–100k category in your data → lowest matching PUMS bin
      in_income == "80000-99999" ~ " $80,000 to $89,999",

      in_income == "100000-119999" ~ " $100,000 to $119,999",

      # All 120k+ map cleanly to cut() top bin
      in_income %in%
        c(
          "120000-139999",
          "140000-159999",
          "160000-179999",
          "180000-199999",
          "200000+"
        ) ~
        " $120,000 or more",

      TRUE ~ NA_character_
    ),
    clim_reg = case_when(
      in_building_america_climate_zone %in% c('Very Cold', 'Cold') ~
        'Very Cold & Cold',
      in_building_america_climate_zone %in% c('Hot-Dry', 'Mixed-Dry') ~
        'Hot-dry & mixed-dry',
      TRUE ~ in_building_america_climate_zone
    ),
    x_heatfuel = case_when(
      in_heating_fuel %in% c('Other Fuel', 'Propane', "Wood") ~ ' other fuel',
      in_heating_fuel == 'None' ~ ' none',
      in_heating_fuel == 'Electricity' ~ ' electricity',
      in_heating_fuel == 'Natural Gas' ~ ' natural gas'
    )
  ) %>%
  mutate(
    in_bedrooms = as.numeric(in_bedrooms),
    in_occupants = parse_number(in_occupants)
  ) ## 10+ is a question here, but very few obs

## ----------------------------------------------------------------------------=
# Targets ----
## ----------------------------------------------------------------------------=
nrel_cleaning <-
  nrel_cleaning %>%
  mutate(
    wall_r_values = case_when(
      str_detect(in_insulation_wall, 'R-') ~
        str_extract(in_insulation_wall, 'R-\\d{1,2}'),
      str_detect(in_insulation_wall, 'Uninsulated') ~ 'Uninsulated'
    ),
    roof_r_values = case_when(
      str_detect(in_insulation_roof, 'R-') ~
        str_extract(in_insulation_roof, 'R-\\d{1,2}'),
      str_detect(in_insulation_roof, 'Uninsulated') ~ 'Uninsulated'
    ),
    window_panes = case_when(
      str_detect(in_windows, 'Double') ~ 'double-pane',
      str_detect(in_windows, 'Single') ~ 'single-pane',
      str_detect(in_windows, 'Triple') ~ 'tripe_pane'
    ),
    infiltration_cats = case_when(
      in_infiltration %in% c('10 ACH50', '15 ACH50') ~ '10 or 15 AHC',
      in_infiltration %in% c('15 ACH50', '20 ACH50', '25 ACH5O') ~
        '15, 20, 25 AHC',
      in_infiltration %in% c('30 ACH50', '40 ACH50') ~ '30 or 40 AHC',
      in_infiltration == '50 ACH50' ~ '50 ACH',
      TRUE ~ 'Less than 10'
    ),
    roof_albedo = case_when(
      in_roof_material %in% c('Tile, Clay or Ceramic', 'Tile, Concrete') ~
        'High',
      TRUE ~ 'Low'
    ),
    wall_albedo = case_when(
      str_detect(in_geometry_wall_exterior_finish, 'Light') ~ 'High',
      str_detect(in_geometry_wall_exterior_finish, 'Dark|Medium') ~ 'Low',
      TRUE ~ 'NONE'
    ),
    infiltration_numeric = parse_number(in_infiltration),
    cool_setpoint_numeric = parse_number(in_cooling_setpoint),
    heat_setpoint_numeric = parse_number(in_heating_setpoint),
    r_wall = case_when(
      is.na(in_insulation_wall) ~ NA_real_,
      str_detect(in_insulation_wall, "Uninsulated") ~ 1,
      TRUE ~ as.numeric(str_extract(in_insulation_wall, "(?<=R-)[0-9]+"))
    ),

    # Roof R-value (assembly), uninsulated = 0.5
    r_roof_assembly = case_when(
      is.na(in_insulation_roof) ~ NA_real_,
      str_detect(in_insulation_roof, "Uninsulated") ~ 0.5,
      str_detect(in_insulation_roof, "R-7") ~ 7,
      str_detect(in_insulation_roof, "R-13") ~ 13,
      str_detect(in_insulation_roof, "R-19") ~ 19,
      str_detect(in_insulation_roof, "R-30") ~ 30,
      str_detect(in_insulation_roof, "R-38") ~ 38,
      str_detect(in_insulation_roof, "R-49") ~ 49,
      TRUE ~ NA_real_
    ),

    # Ceiling R-value, None/Uninsulated = 0.5
    r_ceiling = case_when(
      is.na(in_insulation_ceiling) ~ NA_real_,
      in_insulation_ceiling %in% c("None", "Uninsulated") ~ 0.5,
      str_detect(in_insulation_ceiling, "R-7") ~ 7,
      str_detect(in_insulation_ceiling, "R-13") ~ 13,
      str_detect(in_insulation_ceiling, "R-19") ~ 19,
      str_detect(in_insulation_ceiling, "R-30") ~ 30,
      str_detect(in_insulation_ceiling, "R-38") ~ 38,
      str_detect(in_insulation_ceiling, "R-49") ~ 49,
      TRUE ~ NA_real_
    ),

    attic_type = factor(in_geometry_attic_type),

    # Combined Roof/Ceiling R-value at thermal boundary
    r_roof_ceiling = case_when(
      attic_type == "Vented Attic" ~ r_ceiling,
      attic_type %in%
        c("Unvented Attic", "Finished Attic or Cathedral Ceilings") ~
        r_roof_assembly,
      attic_type == "None" ~ r_roof_assembly,
      TRUE ~ r_roof_assembly
    )
  )


window_map <- tibble(
  in.windows = c(
    "Double, Low-E, Non-metal, Air, M-Gain",
    "Double, Clear, Metal, Air",
    "Double, Clear, Non-metal, Air",
    "Single, Clear, Non-metal",
    "Single, Clear, Metal",
    "Double, Clear, Non-metal, Air, Exterior Clear Storm",
    "Triple, Low-E, Non-metal, Air, L-Gain",
    "Double, Clear, Metal, Air, Exterior Clear Storm",
    "Single, Clear, Non-metal, Exterior Clear Storm",
    "Single, Clear, Metal, Exterior Clear Storm"
  ),
  u_window_ip = c(
    0.30,
    0.50,
    0.50,
    1.00,
    1.00,
    0.40,
    0.30,
    0.40,
    0.60,
    0.60
  ),
  shgc = c(
    0.40,
    0.65,
    0.65,
    0.75,
    0.75,
    0.55,
    0.35,
    0.55,
    0.60,
    0.60
  )
)

nrel_cleaning <-
  nrel_cleaning |>
  left_join(window_map, c('in_windows' = 'in.windows')) |>
  mutate(
    wall_alb = case_when(
      str_detect(in_geometry_wall_exterior_finish, "Light") ~ 0.60,
      str_detect(in_geometry_wall_exterior_finish, "Medium/Dark") ~ 0.30,
      TRUE ~ 0.40
    ),
    roof_alb = case_when(
      str_detect(in_roof_material, "Metal") ~ 0.35,
      str_detect(in_roof_material, "Tile") ~ 0.40,
      str_detect(in_roof_material, "Slate") ~ 0.30,
      str_detect(in_roof_material, "Wood Shingles") ~ 0.25,
      str_detect(in_roof_material, "Asphalt") ~ 0.20,
      str_detect(in_roof_material, "Composition") ~ 0.20,
      TRUE ~ 0.25
    )
  )


if (!c('nrel_clean') %in% dbListTables(con)) {
  dbWriteTable(con, 'nrel_clean', nrel_cleaning)
} else {
  print('table written | Lets hope no more changes lol')
}

# get_ptt_counts <- function(grp_vars) {
#   lovely <-
#     tst %>%
#     summarise(
#       n = n(),
#       n_pop = sum(weight),
#       .by = c(clim_reg, x_hhtype, x_year_broad, all_of(grp_vars))
#     ) %>%
#     group_by(clim_reg, x_hhtype, x_year_broad) %>%
#     mutate(
#       pct = n / sum(n),
#       pct_pop = n_pop / sum(n_pop)
#     ) %>%
#     ungroup()
#
#   return(lovely)
# }

# ptt_cat_vars <-
#   c('wall_r_values', 'roof_r_values', 'window_panes')
#
# tight <-
#   get_ptt_counts(c('infiltration_cats'))
#
#
#
# ggplot(tight, aes(x_hhtype, pct_pop, fill = infiltration_cats)) +
#   geom_col(position = 'dodge') +
#   facet_grid(clim_reg~x_year_broad) +
#   coord_flip()
#
# ggplot(tst, aes(infiltration_numeric, fill = x_hhtype)) +
#   geom_histogram(color = 'white', bins = 15) +
#   facet_grid(clim_reg~x_year_broad)

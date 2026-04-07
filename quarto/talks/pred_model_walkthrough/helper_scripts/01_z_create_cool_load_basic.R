cool_load_viz <-
  map(
    train_tbl_names,
    ~ dbGetQuery(con, str_glue('SELECT * FROM test."{.x}"'))
  ) |>
  list_rbind() |>
  mutate(
    iecc_climate_region_str = case_when(
      in_ashrae_iecc_climate_zone_2004 == "1A_or_2A" ~
        "Hot Very Hot Humid (1|2A)",
      in_ashrae_iecc_climate_zone_2004 == "2A" ~ "Hot Humid (2A)",
      in_ashrae_iecc_climate_zone_2004 == "2B" ~ "Hot Dry (2B)",
      in_ashrae_iecc_climate_zone_2004 == "3A" ~ "Warm Humid (3A)",
      in_ashrae_iecc_climate_zone_2004 == "3B" ~ "Warm Dry (3B)",
      in_ashrae_iecc_climate_zone_2004 == "3C" ~ "Warm Marine (3C)",
      in_ashrae_iecc_climate_zone_2004 == "4A" ~ "Mixed Humid (4A)",
      in_ashrae_iecc_climate_zone_2004 == "4B" ~ "Mixed Dry (4B)",
      in_ashrae_iecc_climate_zone_2004 == "4C" ~ "Mixed Marine (4C)",
      in_ashrae_iecc_climate_zone_2004 == "5A" ~ "Cool Humid (5A)",
      in_ashrae_iecc_climate_zone_2004 == "5B" ~ "Cool Dry (5B)",
      in_ashrae_iecc_climate_zone_2004 == "5C" ~ "Cool Marine (5C)",
      in_ashrae_iecc_climate_zone_2004 == "7A_or_6A" ~
        "Cold | Very Cold Humid (6|7A)",
      in_ashrae_iecc_climate_zone_2004 == "7B_or_6B" ~
        "Cold | Very Cold Dry (6|7B)",
      in_ashrae_iecc_climate_zone_2004 == "7AK" ~ "Subarctic",
      TRUE ~ 'hmm'
    )
  ) |>
  select(
    iecc_climate_region_str,
    out_load_cooling_energy_delivered_kbtu,
    x_hhtype,
    x_year_broad
  ) |>
  collect()

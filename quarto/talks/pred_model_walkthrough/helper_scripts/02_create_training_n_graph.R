## All Counts No Transformations
clim_reg_all_counts <-
  tbl(con, 'ann_results_upgrade_28 ') %>%
  summarise(
    n_all = n(),
    .by = c(in.ashrae_iecc_climate_zone_2004)
  ) |>
  collect() |>
  mutate(
    in.ashrae_iecc_climate_zone_2004 = case_when(
      in.ashrae_iecc_climate_zone_2004 %in% c('1A', '2A') ~ '1A_or_2A',
      in.ashrae_iecc_climate_zone_2004 %in% c('6B', '7B') ~ '7B_or_6B',
      in.ashrae_iecc_climate_zone_2004 %in% c('6A', '7A') ~ '7A_or_6A',
      TRUE ~ in.ashrae_iecc_climate_zone_2004
    )
  ) |>
  summarise(
    n_all = sum(n_all),
    .by = in.ashrae_iecc_climate_zone_2004
  )


mixed_humid_exp <-
  dbGetQuery(con, 'SELECT * FROM train."4A"') |>
  as_tibble()


nrel_tst_clim_reg_counts <-
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
  summarise(
    n_test = n(),
    .by = c(in_ashrae_iecc_climate_zone_2004, iecc_climate_region_str)
  )


nrel_iecc_clim_reg_counts <-
  map(
    train_tbl_names,
    ~ dbGetQuery(con, str_glue('SELECT * FROM train."{.x}"'))
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
  summarise(
    n_train = n(),
    .by = c(in_ashrae_iecc_climate_zone_2004, iecc_climate_region_str)
  ) |>
  left_join(
    clim_reg_all_counts,
    by = c(
      'in_ashrae_iecc_climate_zone_2004' = 'in.ashrae_iecc_climate_zone_2004'
    )
  ) |>
  left_join(nrel_tst_clim_reg_counts) |>
  mutate(
    iecc_clim_reg_fac = as.factor(iecc_climate_region_str),
    n_diff = n_all - n_train,
    n_filtered_out = n_diff - n_test
  ) |>
  arrange(iecc_clim_reg_fac, desc(n_train)) |>
  filter(iecc_clim_reg_fac != 'Subarctic')

train_tbl_names <-
  dbGetQuery(con, "SELECT * FROM information_schema.tables") |>
  filter(table_schema == 'train') |>
  select(table_name) |>
  pull()


## Want to get some statsitics to help add some detail to the plot...
nrel_train_ac_counts <-
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
      TRUE ~ 'PROBLEM'
    )
  ) |>
  summarise(
    mean_cool = mean(out_load_cooling_energy_delivered_kbtu),
    sd_cool = sd(out_load_cooling_energy_delivered_kbtu),
    .by = c('iecc_climate_region_str')
  )


rsq_viz <-
  tbl(con, 'model_comps.ashrae_climate_region_metrics') |>
  filter(.metric == 'rsq') |>
  mutate(
    iecc_climate_region_str = case_when(
      clim_reg == "1A_or_2A" ~ "Hot Very Hot Humid (1|2A)",
      clim_reg == "2A" ~ "Hot Humid (2A)",
      clim_reg == "2B" ~ "Hot Dry (2B)",
      clim_reg == "3A" ~ "Warm Humid (3A)",
      clim_reg == "3B" ~ "Warm Dry (3B)",
      clim_reg == "3C" ~ "Warm Marine (3C)",
      clim_reg == "4A" ~ "Mixed Humid (4A)",
      clim_reg == "4B" ~ "Mixed Dry (4B)",
      clim_reg == "4C" ~ "Mixed Marine (4C)",
      clim_reg == "5A" ~ "Cool Humid (5A)",
      clim_reg == "5B" ~ "Cool Dry (5B)",
      clim_reg == "5C" ~ "Cool Marine (5C)",
      clim_reg == "7A_or_6A" ~ "Cold | Very Cold Humid (6|7A)",
      clim_reg == "7B_or_6B" ~ "Cold | Very Cold Dry (6|7B)",
      TRUE ~ 'PROBLEM'
    ),
    ci_lower = mean - 1.96 * std_err,
    ci_upper = mean + 1.96 * std_err
  ) |>
  collect() |>
  mutate(
    across(where(is.numeric), ~ round(., 3)),
    iecc_climate_region_fac = as.factor(iecc_climate_region_str)
  ) |>
  mutate(
    wflow_id = fct_reorder(wflow_id, mean),
    wflow_id = str_remove(wflow_id, 'cooling_pred_'),
    wflow_id = snakecase::to_title_case(wflow_id)
  ) |>
  group_by(clim_reg) |>
  mutate(color_helper = if_else(mean == max(mean), 'Best', 'Not So Best')) |>
  ungroup()


rsq_viz <-
  rsq_viz |>
  left_join(nrel_train_ac_counts, by = c('iecc_climate_region_str')) |>
  left_join(nrel_iecc_clim_reg_counts, by = c('iecc_climate_region_str')) |> ## From other source script... probably not ideal...
  group_by(clim_reg) |>
  mutate(max_r2 = max(mean)) |>
  ungroup() |>
  mutate(
    label = str_glue(
      "**{iecc_climate_region_fac}:** (N = {format(n_train, big.mark = ',')}) <br> Mean = {format(round(mean_cool, 2), big.mark = ',')} | SD = {format(round(sd_cool, 2), big.mark = ',')} <br> Max R2 = **{round(max_r2, 2)}**"
    )
  )

rmse_viz <-
  tbl(con, 'model_comps.ashrae_climate_region_metrics') |>
  filter(.metric == 'rmse') |>
  mutate(
    iecc_climate_region_str = case_when(
      clim_reg == "1A_or_2A" ~ "Hot Very Hot Humid (1|2A)",
      clim_reg == "2A" ~ "Hot Humid (2A)",
      clim_reg == "2B" ~ "Hot Dry (2B)",
      clim_reg == "3A" ~ "Warm Humid (3A)",
      clim_reg == "3B" ~ "Warm Dry (3B)",
      clim_reg == "3C" ~ "Warm Marine (3C)",
      clim_reg == "4A" ~ "Mixed Humid (4A)",
      clim_reg == "4B" ~ "Mixed Dry (4B)",
      clim_reg == "4C" ~ "Mixed Marine (4C)",
      clim_reg == "5A" ~ "Cool Humid (5A)",
      clim_reg == "5B" ~ "Cool Dry (5B)",
      clim_reg == "5C" ~ "Cool Marine (5C)",
      clim_reg == "7A_or_6A" ~ "Cold | Very Cold Humid (6|7A)",
      clim_reg == "7B_or_6B" ~ "Cold | Very Cold Dry (6|7B)",
      TRUE ~ 'PROBLEM'
    ),
    ci_lower = mean - 1.96 * std_err,
    ci_upper = mean + 1.96 * std_err
  ) |>
  collect() |>
  mutate(
    across(where(is.numeric), ~ round(., 3)),
    iecc_climate_region_fac = as.factor(iecc_climate_region_str)
  ) |>
  mutate(
    wflow_id = fct_reorder(wflow_id, mean),
    wflow_id = str_remove(wflow_id, 'cooling_pred_'),
    wflow_id = snakecase::to_title_case(wflow_id)
  ) |>
  group_by(clim_reg) |>
  mutate(color_helper = if_else(mean == min(mean), 'Best', 'Not So Best')) |>
  ungroup()


rmse_viz <-
  rmse_viz |>
  left_join(nrel_train_ac_counts, by = c('iecc_climate_region_str')) |>
  left_join(nrel_iecc_clim_reg_counts, by = c('iecc_climate_region_str')) |> ## From other source script... probably not ideal...
  group_by(clim_reg) |>
  mutate(min_rmse = min(mean)) |>
  ungroup() |>
  mutate(
    label = str_glue(
      "**{iecc_climate_region_fac}:** (N = {format(n_train, big.mark = ',')}) <br> Mean = {format(round(mean_cool, 2), big.mark = ',')} | SD = {format(round(sd_cool, 2), big.mark = ',')} <br> Max R2 = **{round(min_rmse, 2)}**"
    )
  )

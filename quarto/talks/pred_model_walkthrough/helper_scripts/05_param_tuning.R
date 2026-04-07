default_r2_benchmark <-
  rsq_viz |>
  filter(
    clim_reg == '4A'
  ) |>
  distinct(max_r2) |>
  pull()


default_rmse_benchmark <-
  rmse_viz |>
  filter(
    clim_reg == '4A'
  ) |>
  distinct(min_rmse) |>
  pull()


r2_params_viz <-
  mh_exp_metrics |>
  filter(
    .metric == 'rsq'
  ) |>
  mutate(
    param_combos = paste0('param_combo', sprintf('%02d', 1:30)),
    param_combos = as.factor(param_combos),
    benchmark_helper = if_else(mean >= default_r2_benchmark, 'Yay', "Nay")
  )


rmse_params_viz <-
  mh_exp_metrics |>
  filter(
    .metric == 'rmse'
  ) |>
  mutate(
    param_combos = paste0('param_combo', sprintf('%02d', 1:30)),
    param_combos = as.factor(param_combos),
    benchmark_helper = if_else(mean <= default_rmse_benchmark, 'Yay', "Nay")
  )

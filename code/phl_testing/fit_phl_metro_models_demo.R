library(tidymodels)
library(here)
library(duckdb)
library(tidyverse)
library(xgboost)
library(ranger)
library(glmnet)
library(poissonreg)

## Imports  ----------------------------------------------------------------
con <- dbConnect(duckdb(), 'data/nrel.duckdb')

phl_con <- dbConnect(duckdb(), 'data/phl_testing/phl_nrel_modeling.duckdb')
dbListTables(con)


prism_co_szn <-
  read_csv(
    here::here(
      'data',
      'prism',
      'prism_county_szn_08_19.csv'
    )
  )

# prism_summer <-
#   filter(prism_co_szn, astro_season == 'SUMMER') |>
#   select(geoid, GISJOIN, tmean)

hdds_phl_metro <-
  readRDS(here::here('data', 'workflow_dat', 'hdd_phl_metro_sample.rds'))

## ----------------------------------------------------------------------------=
# Apply filters & selection for pred task on backend ----
## ----------------------------------------------------------------------------=

nrel_tbl <-
  tbl(con, 'nrel_covars_fin') |>
  filter(
    in_metropolitan_and_micropolitan_statistical_area ==
      'Philadelphia-Camden-Wilmington, PA-NJ-DE-MD MSA',
    # in_ashrae_iecc_climate_zone_2004 == ashrae_clim_code, ## Mixed humid climate region
    out_load_cooling_energy_delivered_kbtu != 0,
    out_load_heating_energy_delivered_kbtu != 0
  ) |>
  select(
    bldg_id,
    #  weight,
    in_county,
    in_puma,
    in_occupants,
    in_ashrae_iecc_climate_zone_2004,
    in_bedrooms,
    in_tenure,
    in_heating_fuel,
    out_utility_bills_total_bill_usd,
    in_metropolitan_and_micropolitan_statistical_area,
    x_hhtype,
    x_hhincome,
    x_year_broad,
    pop_density_per_sq_mi,
    at_least_hs,
    black,
    hispanic_or_latino,
    wgt_med_income,
    wgt_med_elep_expend,
    mean_cdd,
    out_load_cooling_energy_delivered_kbtu,
    out_load_heating_energy_delivered_kbtu
  ) |>
  collect() |>
  mutate(
    across(where(is.character), ~ as.factor(.)),
    tot_system_load_delivered = out_load_cooling_energy_delivered_kbtu +
      out_load_heating_energy_delivered_kbtu
  ) |>
  select(
    -out_load_cooling_energy_delivered_kbtu,
    -out_load_heating_energy_delivered_kbtu
  )

nrel_tbl <-
  nrel_tbl |>
  mutate(
    in_heating_fuel = if_else(
      in_heating_fuel %in% c('Propane', 'Other Fuel', 'Wood'),
      'Other',
      in_heating_fuel
    )
  )

# nrel_tbl <-
#   nrel_tbl |>
#   left_join(
#     prism_summer,
#     by = c('in_county' = 'GISJOIN')
#   ) |>
#   select(-geoid)

hdds_phl_metro <-
  hdds_phl_metro |>
  select(GISJOIN, mean_hdd)

nrel_tbl <-
  nrel_tbl |>
  left_join(
    hdds_phl_metro,
    by = c('in_county' = 'GISJOIN')
  )

# nrel_tbl <-
#   nrel_tbl |>
#   left_join(
#     ac_use_tst,
#     by = c('in_county' = 'GISJOIN')
#   )
## Any missings (?)
nrel_tbl |>
  filter(if_any(everything(), is.na))


# Split Data --------------------------------------------------------------

set.seed(1 - 9 - 2025)

test_proportion <- .2

nrel_tbl <- nrel_tbl |>
  mutate(
    split = if_else(
      row_number() %in% sample(n(), size = round(n() * test_proportion)),
      "test",
      "train"
    )
  )

## Remove .2 percent of obs randomly to test our tuned models
nrel_train <-
  nrel_tbl |>
  filter(split == "train")
#mutate(weight = frequency_weights(weight)) ## Going to ignore because all weights are the same

nrel_test <-
  nrel_tbl |>
  filter(split == "test")
#mutate(weight = frequency_weights(weight)) ## Going to ignore because all weights are the same

nrel_folds <-
  vfold_cv(nrel_train, v = 10)

# Write sets to db so I can reaccess them ---------------------------------

dbExecute(
  phl_con,
  "CREATE SCHEMA IF NOT EXISTS train;
  CREATE SCHEMA IF NOT EXISTS test;
  CREATE SCHEMA IF NOT EXISTS model_comps;"
)


DBI::dbWriteTable(
  phl_con,
  DBI::Id(schema = "train", table = 'phl_metro'),
  value = nrel_train,
  overwrite = TRUE
)


DBI::dbWriteTable(
  phl_con,
  DBI::Id(schema = "test", table = 'phl_metro'),
  value = nrel_test,
  overwrite = TRUE
)


# Write folds -------------------------------------------------------------
set.seed(1 - 30 - 2026)

nrel_folds <-
  vfold_cv(nrel_train, v = 10, repeats = 5)


# Recipe ------------------------------------------------------------------
nrel_pre_spec <-
  recipe(tot_system_load_delivered ~ ., data = nrel_train) |>
  update_role(
    bldg_id,
    in_county,
    in_puma,
    in_metropolitan_and_micropolitan_statistical_area,
    in_ashrae_iecc_climate_zone_2004,
    new_role = "id"
  ) |>
  # update_role(weight, new_role = "case_weights") |>  # Add this line
  step_zv(all_nominal_predictors()) |>
  step_zv(all_numeric_predictors()) |>
  step_normalize(all_numeric_predictors()) |>
  step_dummy(all_nominal_predictors())


# Models ------------------------------------------------------------------

## Models that we're testing
models_to_fit <-
  list(
    lm_model = linear_reg() |>
      set_engine("lm"),

    lasso = linear_reg(
      penalty = .1,
      mixture = 1
    ) |>
      set_mode("regression") |>
      set_engine("glmnet"),

    # Ridge regression
    ridge = linear_reg(
      penalty = .1,
      mixture = 0
    ) |>
      set_mode("regression") |>
      set_engine("glmnet"),

    # elastic_net =
    #    linear_reg(
    #      penalty = tune(), mixture = tune()) |>
    #      set_mode("regression") |>
    #      set_engine("glmnet"),

    xgboost = boost_tree(
      mode = "regression",
      engine = "xgboost",
      mtry = NULL,
      trees = NULL,
      min_n = NULL,
      tree_depth = NULL,
      learn_rate = NULL,
      loss_reduction = NULL,
      sample_size = NULL,
      stop_iter = NULL
    ),

    if_a_tree_falls = rand_forest(
      mode = "regression",
      engine = "ranger",
      mtry = NULL,
      trees = NULL,
      min_n = NULL
    )
  )


# Combine model & spec ----------------------------------------------------

## Combine models and recipe
temp_wflow_set <-
  workflow_set(
    preproc = list(cooling_pred = nrel_pre_spec),

    models = list(
      simple = models_to_fit$lm_model,
      lasso = models_to_fit$lasso,
      ridge = models_to_fit$ridge,
      # elastic = models_to_fit$elastic_net,
      xgboost = models_to_fit$xgboost,
      rand_forest = models_to_fit$if_a_tree_falls
    ),

    cross = FALSE
  )


# Fit Models --------------------------------------------------------------
metrics <- metric_set(rmse, mae, rsq, mape) # Changed variable name to avoid confusion


keep_pred <-
  control_resamples(
    save_pred = TRUE,
    save_workflow = TRUE
  )

temp_model_outputs <-
  temp_wflow_set |>
  workflow_map(
    "fit_resamples",
    # Options to `workflow_map()`:
    seed = 1 - 30 - 2025,
    verbose = TRUE,
    # Options to `fit_resamples()`:
    resamples = nrel_folds,
    control = keep_pred,
    metrics = metrics
  )


# Extract metrics predictors ----------------------------------------------
temp_metrics <-
  collect_metrics(temp_model_outputs)

temp_pred <-
  collect_predictions(temp_model_outputs)


# Write results for further analysis --------------------------------------

DBI::dbWriteTable(
  phl_con,
  DBI::Id(schema = "model_comps", table = 'phl_metro_metrics'),
  value = temp_metrics,
  overwrite = TRUE
)


DBI::dbWriteTable(
  phl_con,
  DBI::Id(schema = "model_comps", table = 'temp_pred'),
  value = temp_pred,
  overwrite = TRUE
)

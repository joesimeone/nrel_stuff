library(tidymodels)
library(here)
library(duckdb)
library(tidyverse)
library(xgboost)
library(ranger)
library(glmnet)
library(mirai)

## Imports  ----------------------------------------------------------------
con <- dbConnect(duckdb(), 'data/nrel.duckdb')

dbListTables(con)


## ----------------------------------------------------------------------------=
# Getting Everything together ----
## ---------------------------------------------------------------------------=

nrel_tbl <-
  tbl(con, 'nrel_covars_fin') %>%
  filter(
    out_load_cooling_energy_delivered_kbtu > 500, ## People not really using their system
    !in_state %in% c('DC', 'AK', 'HI'), ## Don't think that I can get cdd info for DC... (?)
    !in_county %in% c('G5106780', 'G4601020') ## There was also a weird cdd issue with a VA county
  ) |>
  select(
    bldg_id,
    #  weight,
    in_county,
    in_puma,
    in_occupants,
    in_ashrae_iecc_climate_zone_2004,
    in_bedrooms,
    in_heating_fuel,
    in_tenure,
    out_utility_bills_electricity_bill_usd,
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
    out_load_cooling_energy_delivered_kbtu
  ) |>
  mutate(
    in_heating_fuel = if_else(
      in_heating_fuel %in% c('Propane', 'Other Fuel', 'Wood'),
      'Other',
      in_heating_fuel
    )
  ) |>
  collect() |>
  mutate(
    across(where(is.character), ~ as.factor(.))
  )

dbListTables(con)


## ----------------------------------------------------------------------------=
# Some basics about how sample is constructed ----
## ----------------------------------------------------------------------------=

## Any missings left??
nrel_tbl %>%
  filter(if_any(everything(), is.na))


## How many individual counties and where? (163, not spread evenely across clim reg)
nrel_tbl |>
  select(in_ashrae_iecc_climate_zone_2004, in_county) |>
  distinct(in_ashrae_iecc_climate_zone_2004, in_county) |>
  arrange(in_ashrae_iecc_climate_zone_2004, in_county) |>
  mutate(
    n = n(),
    .by = c(in_ashrae_iecc_climate_zone_2004)
  ) |>
  arrange(desc(n))

## How many does the AC low outlier get rid of???
no_ac_filter <-
  tbl(con, 'nrel_covars_fin') |>
  summarise(
    n_nrel_nonvac = n(),
    .by = c(in_ashrae_iecc_climate_zone_2004)
  ) |>
  collect()


nrel_tbl |>
  summarise(
    n_sample = n(),
    .by = c(in_ashrae_iecc_climate_zone_2004)
  ) |>
  left_join(
    no_ac_filter
  ) |>
  mutate(
    excluded_obs = n_nrel_nonvac - n_sample
  )


## FINISHED SAMPLE OBJECT
nrel_tbl_fin <-
  nrel_tbl |>
  mutate(
    in_ashrae_iecc_climate_zone_2004 = case_when(
      in_ashrae_iecc_climate_zone_2004 %in% c('7A', '6A') ~ '7A_or_6A',
      in_ashrae_iecc_climate_zone_2004 %in% c('7B', '6B') ~ '7B_or_6B',
      in_ashrae_iecc_climate_zone_2004 %in% c('1A', '2A') ~ '1A_or_2A',
      TRUE ~ in_ashrae_iecc_climate_zone_2004
    )
  )


## Split this bad boy up so that we can iterate
nrel_tbl_split <-
  split(nrel_tbl_fin, nrel_tbl_fin$in_ashrae_iecc_climate_zone_2004)

names(nrel_tbl_split) <- glue::glue('ashrae_{names(nrel_tbl_split)}')
## ---------------------------------------------------------------------------=
# Test Train Split -----
## ---------------------------------------------------------------------------=
set.seed(1 - 23 - 2025)

test_proportion <- .2

nrel_samp_split <-
  map(nrel_tbl_split, function(dat) {
    dat |>
      mutate(
        split = if_else(
          row_number() %in% sample(n(), size = round(n() * test_proportion)),
          "test",
          "train"
        )
      )
  }) |>
  set_names(names(nrel_tbl_split))

## Remove .2 percent of obs randomly to test our tuned models
nrel_train <-
  map(nrel_samp_split, function(dat) {
    dat |>
      filter(split == "train")
  }) |>
  set_names(names(nrel_samp_split))
#mutate(weight = frequency_weights(weight)) ## Going to ignore because all weights are the same

nrel_test <-
  map(nrel_samp_split, function(dat) {
    dat |>
      filter(split == "test")
  }) |>
  set_names(names(nrel_samp_split))
#mutate(weight = frequency_weights(weight)) ## Going to ignore because all weights are the same

dbExecute(
  con,
  "CREATE SCHEMA IF NOT EXISTS cooling_train;"
)


dbExecute(
  con,
  "CREATE SCHEMA IF NOT EXISTS cooling_test;"
)

## Write training and test sets to database for posterity and further analyses
map2(
  nrel_train,
  names(nrel_train),
  ~ DBI::dbWriteTable(
    con,
    DBI::Id(schema = "cooling_train", table = str_glue('{.y}')),
    value = .x,
    overwrite = TRUE
  )
)

map2(
  nrel_test,
  names(nrel_test),
  ~ DBI::dbWriteTable(
    con,
    DBI::Id(schema = "cooling_test", table = str_glue('{.y}')),
    value = .x,
    overwrite = TRUE
  )
)

## Split each climate region into Folds for cross fold validation
nrel_folds <-
  map(
    nrel_train,
    function(dat) {
      vfold_cv(dat, v = 10)
    }
  ) |>
  set_names(names(nrel_train))

names(nrel_train)


### Recipe ------------------------------------------------------------------

## Little more expensive than I thought...

tictoc::tic()
nrel_pre_spec <-
  map(nrel_train, function(dat) {
    recipe(out_load_cooling_energy_delivered_kbtu ~ ., data = dat) |>
      update_role(
        bldg_id,
        in_county,
        in_puma,
        in_ashrae_iecc_climate_zone_2004,
        new_role = "id"
      ) %>%
      # update_role(weight, new_role = "case_weights") %>%  # Add this line
      step_zv(all_nominal_predictors()) %>%
      step_zv(all_numeric_predictors()) %>%
      step_normalize(all_numeric_predictors()) %>%
      step_dummy(all_nominal_predictors())
  }) |>
  set_names(names(nrel_train))
tictoc::toc()
## ----------------------------------------------------------------------------=
# Models ---
## ----------------------------------------------------------------------------=

## Models that we're testing
models_to_fit <-
  list(
    lm_model = linear_reg() %>%
      set_engine("lm"),

    lasso = linear_reg(
      penalty = .1,
      mixture = 1
    ) %>%
      set_mode("regression") %>%
      set_engine("glmnet"),

    # Ridge regression
    ridge = linear_reg(
      penalty = .1,
      mixture = 0
    ) %>%
      set_mode("regression") %>%
      set_engine("glmnet"),

    # elastic_net =
    #    linear_reg(
    #      penalty = tune(), mixture = tune()) %>%
    #      set_mode("regression") %>%
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
cool_wflow_set <-
  map(
    nrel_pre_spec,
    function(dat) {
      workflow_set(
        preproc = list(cooling_pred = dat),

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
    }
  ) |>
  set_names(names(nrel_pre_spec))


# Fit Models --------------------------------------------------------------
# my_metrics <- metric_set(rmse, mae, rsq, mape) # Changed variable name to avoid confusion
#
# keep_pred <-
#   control_resamples(
#     save_pred = TRUE
#   )
#

daemons(6, output = TRUE)

cool_model_outputs <- pmap(
  list(
    dat = cool_wflow_set,
    folds = nrel_folds,
    name = names(cool_wflow_set)
  ),
  in_parallel(
    function(dat, folds, name) {
      library(tidymodels)
      library(cli)

      # Define metrics inside the worker
      my_metrics <- metric_set(rmse, rsq, mae) # adjust to your actual metrics

      # Define control inside the worker too
      keep_pred <- control_resamples(save_pred = TRUE)

      result <- dat %>%
        workflow_map(
          "fit_resamples",
          seed = 1122025,
          verbose = FALSE,
          resamples = folds,
          control = keep_pred,
          metrics = my_metrics
        )

      cli_alert_success("Finished: {name}")

      result
    }
  )
) |>
  set_names(names(cool_wflow_set))

daemons(0)

## ----------------------------------------------------------------------------=
# Write results for further analysis -----
## ----------------------------------------------------------------------------=

# Extract metrics predictors ----------------------------------------------
cool_metrics <-
  map(cool_model_outputs, collect_metrics)

cool_metrics_fin <-
  imap(cool_metrics, ~ .x |> mutate(clim_reg = .y)) |>
  list_rbind()


cool_pred <-
  map(cool_model_outputs, collect_predictions)

## So we can re-id buildings in pred for visuals and analysis
nrel_ids <-
  map(nrel_train, ~ .x |> mutate(.row = row_number()) |> select(bldg_id, .row))

cool_pred_ids <-
  map2(
    cool_pred,
    nrel_ids,
    ~ .x |>
      left_join(.y, by = c('.row'))
  )

cool_pred_fin <-
  imap(
    cool_pred_ids,
    ~ .x |>
      mutate(clim_reg = .y)
  ) |>
  list_rbind()


## ----------------------------------------------------------------------------=
# Write to duckdb schema -----
## ----------------------------------------------------------------------------=

dbExecute(
  con,
  "CREATE SCHEMA IF NOT EXISTS cool_model_comps;"
)


DBI::dbWriteTable(
  con,
  DBI::Id(schema = "cool_model_comps", table = "ashrae_climate_region_metrics"),
  cool_metrics_fin
)

DBI::dbWriteTable(
  con,
  DBI::Id(
    schema = "cool_model_comps",
    table = "ashrae_climate_region_predictions"
  ),
  cool_pred_fin
)

dbListTables(con, schema = 'main')

dbDisconnect(con, TRUE)

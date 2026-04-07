library(xgboost)
library(tidymodels)
library(tidyverse)
library(duckdb)
library(vip)


con <- dbConnect(duckdb(), 'data/phl_testing/phl_nrel_modeling.duckdb')


dbListTables(con)
dbGetQuery(con, "SHOW TABLES") # shows tables in current schema
dbGetQuery(con, "SELECT * FROM information_schema.tables")


phl_exp <-
  dbGetQuery(con, 'SELECT * FROM train.phl_metro') |>
  as_tibble()


set.seed(1 - 30 - 2026)

phl_folds <-
  vfold_cv(phl_exp, v = 10, repeats = 5)


# Recipe prep -------------------------------------------------------------

phl_recipe <-
  recipe(tot_system_load_delivered ~ ., data = phl_exp) |>
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


# Model & Grid prep --------------------------------------------------------------

## The xgboost model that we can tune
xgb_spec <-
  boost_tree(
    mode = "regression",
    engine = "xgboost",
    mtry = tune(),
    trees = 1000,
    min_n = tune(),
    tree_depth = tune(),
    learn_rate = tune(),
    loss_reduction = tune(),
    sample_size = tune(),
    stop_iter = 10
  )

## The grid of hyper paraemeters that we'll test...
xgb_grid <-
  grid_space_filling(
    finalize(mtry(), phl_exp),
    min_n(),
    tree_depth(),
    learn_rate(),
    loss_reduction(),
    sample_size = sample_prop(),
    size = 30,
    type = "latin_hypercube",
    original = TRUE
  )


## Finalized model
xgb_tune_spec <-
  workflow() |>
  add_recipe(phl_recipe) |>
  add_model(xgb_spec)


# Perform tuning ----------------------------------------------------------

my_metrics <- metric_set(rmse, rsq, mae, mape) # adjust to your actual metrics


set.seed(1 - 30 - 2026)

tictoc::tic()
xgb_res <-
  tune_grid(
    xgb_tune_spec,
    resamples = phl_folds,
    grid = xgb_grid,
    control = control_grid(save_pred = TRUE),
    metrics = my_metrics
  )
tictoc::toc()

xgb_res


# Assessment Basics  ------------------------------------------------------

## We should use the wide data just to make a dot plots of r2 and rmse
phl_example_metrics <-
  collect_metrics(xgb_res)

phl_example_metrics |>
  filter(.metric == 'rsq') |>
  ggplot(aes(.config, mean)) +
  geom_point() +
  coord_flip() +
  theme_classic()

## Extract Best performing models, but make sure that they're the same...
best_model_rsq <-
  select_best(xgb_res, metric = 'rsq')

best_mode_rmse <-
  select_best(xgb_res, metric = 'rmse')

best_model_mae <-
  select_best(xgb_res, metric = 'rmse')

xgb_spec_fin <-
  finalize_workflow(
    xgb_tune_spec,
    best_model_rsq
  )


# Generate feature importance (vip package) -------------------------------
temp_load_imp_plot <-
  xgb_spec_fin |>
  fit(data = phl_exp) |>
  extract_fit_parsnip() |>
  vip(geom = 'point') +
  theme_classic()


## ---------------------------------------------------------------------------=
# Apply to test set and finalize metrics  ----
## ---------------------------------------------------------------------------=

phl_hold_out <-
  dbGetQuery(con, 'SELECT * FROM test.phl_metro') |>
  as_tibble()


phl_final_fit <-
  fit(xgb_spec_fin, data = phl_exp)

# Step 2: Predict on your test set
phl_test_predictions <-
  predict(phl_final_fit, new_data = phl_hold_out) |>
  bind_cols(phl_hold_out) # attach actuals

# Step 3: Calculate metrics
phl_test_metrics <-
  phl_test_predictions |>
  metrics(truth = tot_system_load_delivered, estimate = .pred)

## ----------------------------------------------------------------------------=
# If happy.... save models for oos in PUMs ----
## ----------------------------------------------------------------------------=

saveRDS(xgb_spec_fin, "tuned_models/phl_post_tuning_extraction.rds")
saveRDS(phl_final_fit, "tuned_models/phl_example_fit.rds")


saveRDS(phl_example_metrics, 'data/phl_testing/phl_example_metrics.rds')
saveRDS(phl_test_predictions, 'data/phl_testing/phl_test_predictions.rds')
saveRDS(phl_test_metrics, 'data/phl_testing/phl_test_metrics.rds')
saveRDS(xgb_grid, 'data/phl_testing/phl_sample_grid.rds')

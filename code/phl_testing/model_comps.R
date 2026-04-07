library(tidymodels)
library(here)
library(duckdb)
library(tidyverse)
library(xgboost)
library(ranger)
library(glmnet)
library(ggtext)
library(ggiraph)

helper_files <-
  list.files(
    here::here(
      'data',
      'phl_testing'
    ),
    full.names = TRUE
  )


phl_sample_metrics <-
  readRDS('C:/git/nrel_stuff/data/phl_testing/phl_example_metrics.rds')

theme_scribble <-
  theme(
    plot.background = element_rect(fill = 'ivory'),
    panel.background = element_rect(fill = 'ivory'),
    panel.grid.major.x = element_line(
      color = 'lightgray',
      linetype = 'dashed',
      linewidth = .55
    ),
    panel.grid.minor.x = element_blank(),
    panel.grid.major.y = element_blank(),
    panel.grid.minor.y = element_blank(),
    legend.position = 'none'
  )

## Imports  ----------------------------------------------------------------

phl_con <- dbConnect(duckdb(), 'data/phl_testing/phl_nrel_modeling.duckdb')
dbListTables(phl_con)


temp_summaries <-
  tbl(phl_con, 'train.phl_metro') |>
  summarise(
    mean_temp_load = mean(tot_system_load_delivered),
    sd_temp_load = sd(tot_system_load_delivered),
    n_train = n()
  ) |>
  collect()


phl_rsq_viz <-
  tbl(phl_con, 'model_comps.phl_metro_metrics') |>
  filter(.metric == 'rsq') |>
  mutate(
    ci_lower = mean - 1.96 * std_err,
    ci_upper = mean + 1.96 * std_err
  ) |>
  collect() |>
  mutate(
    across(where(is.numeric), ~ round(., 3))
  ) |>
  mutate(
    wflow_id = fct_reorder(wflow_id, mean),
    wflow_id = str_remove(wflow_id, 'cooling_pred_'),
    wflow_id = snakecase::to_title_case(wflow_id)
  ) |>
  mutate(color_helper = if_else(mean == max(mean), 'Best', 'Not So Best')) |>
  ungroup()


phl_rmse_viz <-
  tbl(phl_con, 'model_comps.phl_metro_metrics') |>
  filter(.metric == 'rmse') |>
  mutate(
    ci_lower = mean - 1.96 * std_err,
    ci_upper = mean + 1.96 * std_err
  ) |>
  collect() |>
  mutate(
    across(where(is.numeric), ~ round(., 3))
  ) |>
  mutate(
    wflow_id = fct_reorder(wflow_id, mean),
    wflow_id = str_remove(wflow_id, 'cooling_pred_'),
    wflow_id = snakecase::to_title_case(wflow_id)
  ) |>
  mutate(color_helper = if_else(mean == min(mean), 'Best', 'Not So Best')) |>
  ungroup()


phl_r2_plot <-
  phl_rsq_viz |>
  ggplot(aes(
    x = wflow_id,
    y = mean,
    color = color_helper,
    alpha = color_helper
  )) +
  geom_point(size = 3) +
  geom_errorbar(aes(ymin = ci_lower, ymax = ci_upper), width = 0.3) +
  coord_flip() +
  scale_y_continuous(limits = c(0, 0.9), breaks = seq(0, 0.9, 0.2)) +
  theme(legend.position = 'none', strip.text = element_markdown(size = 10)) +
  scale_alpha_manual(values = c(1.25, .85)) +
  scale_color_manual(values = c('#807DBA', 'darkgray')) +
  labs(x = NULL, y = 'Mean R2 Over 10-Fold, 5-Repeat CFV') +
  theme_scribble


phl_rmse_plot <-
  phl_rmse_viz |>
  ggplot(aes(
    x = wflow_id,
    y = mean,
    color = color_helper,
    alpha = color_helper
  )) +
  geom_point(size = 3) +
  geom_errorbar(aes(ymin = ci_lower, ymax = ci_upper), width = 0.3) +
  coord_flip() +
  scale_y_continuous(labels = label_number(scale = 1 / 1000, suffix = "k")) +
  #scale_color_brewer(palette = 'Set1') +
  scale_color_manual(values = c('#7D9DBA', 'darkgray')) +
  scale_alpha_manual(values = c(1.25, .85)) +
  labs(x = NULL, y = 'Mean RMSE Over 10-Fold CFV') +
  theme_scribble


# Results from param tuning  ----------------------------------------------

phl_default_r2_benchmark <-
  phl_rsq_viz |>
  summarise(max_r2 = max(mean)) |>
  pull()


phl_default_rmse_benchmark <-
  phl_rmse_viz |>
  summarise(min_rmse = min(mean)) |>
  pull()


phl_r2_params_viz <-
  phl_sample_metrics |>
  filter(
    .metric == 'rsq'
  ) |>
  mutate(
    param_combos = paste0('param_combo', sprintf('%02d', 1:30)),
    param_combos = as.factor(param_combos),
    benchmark_helper = if_else(mean >= phl_default_r2_benchmark, 'Yay', "Nay")
  )


phl_rmse_params_viz <-
  phl_sample_metrics |>
  filter(
    .metric == 'rmse'
  ) |>
  mutate(
    param_combos = paste0('param_combo', sprintf('%02d', 1:30)),
    param_combos = as.factor(param_combos),
    benchmark_helper = if_else(mean <= phl_default_rmse_benchmark, 'Yay', "Nay")
  )


phl_r2_params_plots <-
  phl_r2_params_viz |>
  ggplot(aes(
    param_combos,
    mean,
    color = benchmark_helper,
    alpha = benchmark_helper
  )) +
  geom_point_interactive(
    aes(
      tooltip = str_glue(
        "mtry: {mtry}
min_n: {min_n}
tree_depth: {tree_depth}
learn_rate: {learn_rate}
loss_reduction: {loss_reduction}
sample_size: {sample_size}
Mean R²: {round(mean, 3)}"
      )
    ),
    size = 3
  ) +
  geom_hline(
    yintercept = phl_default_r2_benchmark,
    linetype = 'dashed',
    color = 'red'
  ) +
  annotate(
    'text',
    x = 30,
    y = phl_default_r2_benchmark - .05,
    label = str_glue('Default Benchmark: {round(phl_default_r2_benchmark, 2)}'),
    hjust = .1,
    size = 3.5
  ) +
  scale_alpha_manual(values = c(.55, 1.25)) +
  scale_color_manual(values = c('darkgray', '#807DBA')) +
  coord_flip() +
  # r2_param_labs +
  theme(
    panel.background = element_rect(fill = "transparent", colour = NA),
    plot.background = element_rect(fill = "transparent", colour = NA),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(),
    legend.background = element_rect(fill = "transparent"),
    legend.key = element_rect(fill = "transparent"),
    plot.title = element_markdown(hjust = .5),
    plot.subtitel = element_markdown(hjust = .5),
    legend.position = 'none',
    text = element_text(size = 14),
    axis.text.y = element_text(margin = margin(t = 10))
  )


phl_rmse_params_plots <-
  phl_rmse_params_viz |>
  ggplot(aes(
    param_combos,
    mean,
    color = benchmark_helper,
    alpha = benchmark_helper
  )) +
  geom_point_interactive(
    aes(
      tooltip = str_glue(
        "mtry: {mtry}
min_n: {min_n}
tree_depth: {tree_depth}
learn_rate: {learn_rate}
loss_reduction: {loss_reduction}
sample_size: {sample_size}
Mean R²: {round(mean, 3)}"
      )
    ),
    size = 3
  ) + # Removed color = '#807DBA' to respect the aes mapping
  geom_hline(
    yintercept = phl_default_rmse_benchmark,
    linetype = 'dashed',
    color = 'red'
  ) +
  annotate(
    'text',
    x = 30,
    y = phl_default_rmse_benchmark,
    label = str_glue(
      "Default Benchmark: {format(round(phl_default_rmse_benchmark, 2), big.mark = ',')}"
    ),
    hjust = -0.1,
    size = 3.5
  ) +
  scale_alpha_manual(values = c(.25, 1)) + # Changed 1.25 to 1 (max alpha is 1)
  scale_color_manual(values = c('gray', '#7D9DBA')) +
  scale_y_continuous(labels = scales::comma) +
  coord_flip() +
  # rmse_param_labs +
  theme(
    panel.background = element_rect(fill = "transparent", colour = NA),
    plot.background = element_rect(fill = "transparent", colour = NA),
    plot.title = element_markdown(hjust = .5),
    plot.subtitel = element_markdown(hjust = .5),
    panel.grid.major = element_blank(),
    panel.grid.minor = element_blank(),
    legend.background = element_rect(fill = "transparent"),
    legend.key = element_rect(fill = "transparent"),
    legend.position = 'none',
    text = element_text(size = 14),
    axis.text.y = element_text(margin = margin(t = 10))
  )

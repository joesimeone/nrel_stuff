library(duckdb)
library(tidyverse)
library(glue)
library(tigris)
library(sf)


options(tigris_use_cache = TRUE)
## ----------------------------------------------------------------------------=
# Imports ---
## ----------------------------------------------------------------------------=

con <- dbConnect(duckdb(), 'data/nrel.duckdb')

dbListTables(con)


pums_ppl <- tbl(con, 'pums_19_ppl_5yr')
pums_hh <- tbl(con, 'pums_19_5yr')


## ---------------------------------------------------------------------------=
# Get tigris file for population density calcs ----
## ---------------------------------------------------------------------------=

puma_shp <-
  pumas(state = NULL, year = 2019, cb = TRUE) |>
  st_drop_geometry()

puma_shp <-
  puma_shp |>
  mutate(unique_puma = glue::glue('{STATEFP10}{PUMACE10}'))

# Common Function ---------------------------------------------------------
get_pums_pcts <- function(dat, demo_var) {
  ## Calculate the percentage of stuff in a given PUMA
  dat |>
    summarise(
      n = n(),
      pop = sum(PWGTP),
      .by = c('unique_puma', all_of(demo_var))
    ) |>
    group_by(unique_puma) |>
    mutate(
      pct = n / sum(n),
      pop_pct = pop / sum(pop)
    ) |>
    ungroup() |>
    collect()
}

## ---------------------------------------------------------------------------=
# Prep Data w/out pulling into R -----
## ---------------------------------------------------------------------------=

## Cleans our predictor candidates so that they're more interpretable when
## we start xgboosting.
## For people file:
## AGE - over or under 65
## Race - Black or non black
## High School - People at least finished high school

pums_ppl_19_cl <-
  pums_ppl |>
  mutate(
    unique_puma = paste0(ST, PUMA),
    o_u_65 = if_else(AGEP >= 65, 'over_65', 'under_65'),
    school_hs_attain = if_else(
      SCHL %in% c(16, 18:24),
      'at_least_hs',
      'max_12th_grade_or_ged'
    ),
    race_black_base = if_else(RAC1P == 2, 'Black', 'non-black'),
    hisp_base = if_else(HISP == '01', 'non-hispanic', 'hispanic_or_latino')
  )

## Prepare household data
## Year energy expenditures (electric, gas, other / fulp)

pums_hh_19_cl <-
  pums_hh |>
  mutate(
    unique_puma = paste0(ST, PUMA),
    yearel = ELEP * 12,
    yearng = GASP * 12,
    yearoth = FULP * 12,
    tot_ann_expend = yearel + yearng + yearoth
  )


## ----------------------------------------------------------------------------=
# Derive socio-demographic covariates ----
## ----------------------------------------------------------------------------=

## We want the percentage of select demographic characteristics for each PUMA
## expressed as a percentage.

## Variables of interest
demo_vars <-
  c('school_hs_attain', 'race_black_base', 'hisp_base', 'o_u_65')

## List with my percentages || Leave this in memory so we can use population
## estimates in a second...
puma_pcts <-
  map(demo_vars, ~ get_pums_pcts(pums_ppl_19_cl, .x))


## Swing percentages wide as columns
puma_pcts_wide <-
  map(
    demo_vars,
    ~ get_pums_pcts(pums_ppl_19_cl, .x) |>
      select(-n, -pop) |>
      pivot_wider(
        id_cols = c('unique_puma'),
        names_from = !!sym(.x),
        values_from = 'pct'
      ) |>
      janitor::clean_names()
  ) |>
  set_names(demo_vars)

## Population Density stuff

pumas_pop <-
  puma_pcts[[1]] |>
  select(unique_puma, pop) |>
  left_join(puma_shp, by = c('unique_puma')) |>
  mutate(pop_density_per_sq_mi = pop / (ALAND10 / 2589988.11)) |>
  select(unique_puma, NAME10, pop, ALAND10, pop_density_per_sq_mi) |>
  janitor::clean_names()


## ----------------------------------------------------------------------------=
# Get some household variable medians just in case -----
## ----------------------------------------------------------------------------=

## This guy will take a little longer because
pums_hh_med_covars <-
  pums_hh_19_cl |>
  select(unique_puma, WGTP, HINCP, yearel, yearng, yearoth, tot_ann_expend) |>
  collect() |>
  summarise(
    wgt_med_income = Hmisc::wtd.quantile(HINCP, weights = WGTP, probs = .5),
    wgt_med_elep_expend = Hmisc::wtd.quantile(
      yearel,
      weights = WGTP,
      probs = .5
    ),
    wgt_med_ng_expend = Hmisc::wtd.quantile(yearng, weights = WGTP, probs = .5),
    wgt_med_oth_expend = Hmisc::wtd.quantile(
      yearoth,
      weights = WGTP,
      probs = .5
    ),
    wgt_med_tot_expend = Hmisc::wtd.quantile(
      tot_ann_expend,
      weights = WGTP,
      probs = .5
    ),
    .by = c('unique_puma')
  )


## ---------------------------------------------------------------------------=
# Combine Covariates Data -----
## ---------------------------------------------------------------------------=

puma_pcts_fin <-
  puma_pcts_wide |>
  reduce(left_join)

puma_covars_fin <-
  pumas_pop |>
  left_join(
    puma_pcts_fin,
    by = c('unique_puma')
  ) |>
  left_join(
    pums_hh_med_covars,
    by = c('unique_puma')
  )

write_csv(
  puma_covars_fin,
  'data/workflow_dat/puma_2019_5yr_covars.csv'
)

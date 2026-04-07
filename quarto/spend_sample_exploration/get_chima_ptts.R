get_chima_ptts <- function(
  htype,
  ybl,
  ba_clim_reg,
  wall_val,
  roof_val,
  #  u_val,
  # shgc_val,
  ach50_val,
  cool_point,
  heat_point
) {
  # roof_alb,
  #wall_alb){

  nrel_tst <-
    nrel_clean |>

    filter(
      x_hhtype == htype,
      x_year_broad == ybl,
      clim_reg == ba_clim_reg,
      r_wall == wall_val,
      r_roof_ceiling == roof_val,
      #   u_window_ip == u_val,
      #  shgc == shgc_val,
      infiltration_numeric == ach50_val,
      cool_setpoint_numeric == cool_point,
      heat_setpoint_numeric == heat_point
      #   roof_albedo == roof_alb,
      #  wall_albedo == wall_alb
    ) |>
    collect()

  return(nrel_tst)
}


chima_tbl_cl <-
  chima_tbl |>
  mutate(
    clim_reg = if_else(
      !vintage %in% c('pre-1940', '1940-79', 'post-1980'),
      vintage,
      NA_character_
    )
  ) |>
  fill(clim_reg) |>
  filter(vintage != clim_reg) |>
  mutate(
    x_year_broad = case_when(
      vintage == 'pre-1940' ~ ' Before 1940',
      vintage == '1940-79' ~ ' 1940 - 1980',
      vintage == 'post-1980' ~ ' After 1980'
    ),
    x_hhtype = case_when(
      building_type == 'MF5+' ~ ' apartment in building with 5+ units',
      building_type == 'SFA' ~ ' single family attached',
      building_type == 'SFD' ~ ' single family detached'
    ),
    clim_reg = case_when(
      clim_reg == "Hot-Dry / Mixed-Dry Climate Region" ~ "Hot-dry & mixed-dry",
      clim_reg == "Hot-Humid Climate Region" ~ "Hot-Humid",
      clim_reg == "Marine Climate Region" ~ "Marine",
      clim_reg == "Mixed-Humid Climate Region" ~ "Mixed-Humid",
      clim_reg == "Very Cold / Cold Climate Region" ~ "Very Cold & Cold",
      TRUE ~ clim_reg
    ),
    r_wall_ft2_h_f_btu = replace_na(r_wall_ft2_h_f_btu, '1'),
    r_wall_ft2_h_f_btu = if_else(
      r_wall_ft2_h_f_btu == 'NA',
      '1',
      r_wall_ft2_h_f_btu
    ),
    r_wall_ft2_h_f_btu = parse_number(r_wall_ft2_h_f_btu)
  ) |>
  select(-stock_106, -stock_share_percent, -vintage, -building_type) |>
  rename(
    r_wall = r_wall_ft2_h_f_btu,
    r_roof_ceiling = r_roof_ft2_h_f_btu,
    u_window_ip = u_window_btu_ft2_h_f,
    infiltration_numeric = ach50,
    cool_setpoint_numeric = cool_setpoint_f,
    heat_setpoint_numeric = heat_setpoint_f,
    roof_alb = roof_albedo,
    wall_alb = wall_albedo
  ) |>
  arrange(clim_reg, x_hhtype, x_year_broad)


chimas_ptts <-
  pmap(
    .l = list(
      htype = chima_tbl_cl$x_hhtype,
      ybl = chima_tbl_cl$x_year_broad,
      ba_clim_reg = chima_tbl_cl$clim_reg,
      wall_val = chima_tbl_cl$r_wall,
      roof_val = chima_tbl_cl$r_roof_ceiling,
      #     u_val       = chima_tbl_cl$u_window_ip,
      #    shgc_val    = chima_tbl_cl$shgc,
      ach50_val = chima_tbl_cl$infiltration_numeric,
      cool_point = chima_tbl_cl$cool_setpoint_numeric,
      heat_point = chima_tbl_cl$heat_setpoint_numeric
      #   roof_alb    = chima_tbl_cl$roof_alb,
      #  wall_alb    = chima_tbl_cl$wall_alb
    ),
    .f = get_chima_ptts
  ) |>
  list_rbind()


nrel_tst <-
  nrel_clean |>

  filter(
    x_hhtype == ' apartment in building with 5+ units',
    x_year_broad == ' 1940 - 1980',
    clim_reg == 'Hot-Humid',
    r_wall == 7,
    r_roof_ceiling == 30,
    #   u_window_ip == u_val,
    #  shgc == shgc_val,
    infiltration_numeric == 25,
    cool_setpoint_numeric == 75,
    heat_setpoint_numeric == 70
    #   roof_albedo == roof_alb,
    #  wall_albedo == wall_alb
  ) |>
  collect()

chimas_ptts |>
  distinct(in_state)

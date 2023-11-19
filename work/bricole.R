ff <- list.files("/space_mounts/data/marseille/distances/src/car_dgr2", full.names = TRUE)
walk(ff, ~{
  ppqt <- arrow::read_parquet(glue::glue("{.x}/ttm.parquet"))
  ppqt[, COMMUNE := NULL]
  arrow::write_parquet(ppqt, glue::glue("{.x}/ttm.parquet"))})

arrow::open_dataset("/space_mounts/data/marseille/distances/src/car_dgr2")

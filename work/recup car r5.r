# récup d'un long calcul

library(tidyverse)
library(fs)
library(qs)
library(data.table)

recup_rep <- "/home/datahub/larochelle/temp_cr5"

temps <- fs::dir_ls(recup_rep)

data_temp <- data.table::rbindlist(map(temps, ~qs::qread(.x)))


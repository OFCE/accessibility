library(tidyverse)
library(dodgr)

load("zones.rda")
load("../larochelle/zones.rda")

set_overpass_url("https://overpass.kumi.systems/api/interpreter")

sc_scot3 <- map(names(scot3.epci), ~dodgr_streetnet_sc(str_c(.x, " France")))

names(sc_scot3) <- names(scot3.epci)

joined_sc <- map(purrr::transpose(sc_scot3), ~bind_rows(.x))

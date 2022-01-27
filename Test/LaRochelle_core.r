library(sf)
library(stars)
library(glue)
library(tictoc)
library(devtools)

ville <- "La Rochelle"
localdata <- "~/files/la rochelle/r5"
DVFdata <- "~/files/DVFdata"

# choix d'un petit secteur de test. Core plutôt que commuting
FUA <- st_read("{DVFdata}/sources/FUA/FRA core/FRA_core.shp" |> glue(),
               stringsAsFactors=FALSE) |> st_transform(3035)

fua <- FUA |> dplyr::filter(fuaname == ville) |> dplyr::pull(geometry)

# choix de la résolution
resol <- 200

jour_du_transit <- as.POSIXct("2022-03-09 08:00:00 UTC")

jMem <- "8G"
options(java.parameters = '-Xmx12G')

#moteur_r5 <- r5r::setup_r5(localdata, verbose = TRUE, overwrite = FALSE)

larochelle_c200 <- qs::qread("~/files/la rochelle/la_rochelle_c200.rda", nthreads = 4)
larochelle_emp <- qs::qread(file = "~/files/la rochelle/emp_LaRochelle.rda", nthreads = 4)

# Pour l'instant, test sur core et non commuting
larochelle_emp <- larochelle_emp[fua]
st_dimensions(larochelle_c200) <- st_dimensions(larochelle_emp)

opportunites_stars <- c(larochelle_emp, dplyr::select(larochelle_c200, Ind))
rm(larochelle_c200, larochelle_emp)

opportunites <- st_as_sf(opportunites_stars, as_points = TRUE) |>
  dplyr::transmute(emplois = ifelse(is.na(emplois_total), 0, emplois_total)) |>
  dplyr::filter(emplois != 0)

rJava::.jinit()

# load_all()
moteur_r5_wparam <- routing_setup_r5(path = localdata, date = jour_du_transit, jMem = jMem, n_threads = 4)

tic()
iso_transit_r5 <- iso_accessibilite(quoi = opportunites,
                                    ou = fua |> st_zm(drop=TRUE),
                                    resolution = resol,
                                    tmax = 30,
                                    pdt = 5,
                                    dir = "r5_temp_larochelle" |> glue(),
                                    routing = moteur_r5_wparam,
                                    future=TRUE)
toc()

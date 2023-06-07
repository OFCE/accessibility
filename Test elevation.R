elevation_file = "~/files/la rochelle/r5_base/elevation.tif"
test_var_file = "~/larochelle/v2/mobscol/marges_mobsco.rda"
localr5 = "~/files/la rochelle/r5_base"

source("~/accessibility/R/iso_accessibilite.r")
source("~/accessibility/R/iso2time.r")
source("~/accessibility/R/iso_utils.r")
source("~/accessibility/R/map_utils.r")
source("~/accessibility/R/routing_system.r")
source("~/accessibility/R/gtfs_plot.R")
source("~/accessibility/R/gtfs_utils.r")

load(file = test_var_file)

library(stringr)
library(sf)
library(r5r)
library(glue)
library(tictoc)
library(dplyr)
library(terra)
library(data.table)
library(arrow)
library(conflicted)
conflict_prefer("first", "dplyr")
conflict_prefer("idINS2square", "accessibility")
conflict_prefer("idINS2square", "accessibility")
conflict_prefer("select", "dplyr")
conflict_prefer("filter", "dplyr")
conflict_prefer("shift", "terra")
conflict_prefer("wday", "lubridate")

progressr::handlers(global = TRUE)
progressr::handlers(progressr::handler_progress(format = ":bar :percent :eta", width = 80))
data.table::setDTthreads(8)
arrow::set_cpu_count(8)

conflict_prefer("select", "dplyr", quiet=TRUE)
conflict_prefer("filter", "dplyr", quiet=TRUE)

## globals --------------------
load("~/larochelle/v2/baselayer.rda")

resol <- 200

elevation <- terra::rast("{elevation}" |> glue())

# les carreaux de résidence
c200ze <- qs::qread("{c200ze_file}" |> glue())
c200.scot3 <- c200ze |>
  filter(ind>0&scot) |> 
  select(ind) |>
  st_centroid() 

message(str_c("Nombre de carreaux sur la zone",
              "résidents = {nrow(c200.scot3)}",
              "opportunités emploi = {c200ze |> filter(emp>0) |> nrow()}",
              "opportunités complètes = {c200ze |> filter(emp>0|ind>0) |> nrow()}",
              sep = "\n") |> glue())

file.remove("/files/la rochelle/r5/newlines-gtfs.zip")

# Choix du jour du transit
jour_du_transit <- plage(localr5) |> choisir_jour_transit()
message(
  "jour retenu: \n{lubridate::wday(jour_du_transit, label = TRUE, abbr = FALSE)} {jour_du_transit}" |> glue())

# les opportinités
opportunites <- c200ze |> 
  select(emplois=emp, ind) |> 
  st_centroid() |> 
  st_transform(crs=4326)

# les arguments individus et opportunites ici utilisés donnent une liste d'idINS, leur point central associé (sf_geometry) et le nombre d'individus dans la zone.
# On utilisera ici les variables les plus larges possibles au vu de la taille du calcul (on ne veut surtout pas devoir faire ce calcul pour chaque couple type d'etab)
les_individus <- indiv_ttetab
les_opportunites <- opport_ttetab

rm(list = ls(pattern = "mobsco_"))
rm(list = ls(pattern = "indiv_"))
rm(list = ls(pattern = "opport_"))

les_individus <- les_individus |> mutate(geometry = idINS2square(idINS)) |> st_as_sf(sf_column_name = "geometry", crs = 3035) |> st_transform(crs = 4326) |> st_centroid() |> select(NB, id = idINS)
les_opportunites <- les_opportunites |> mutate(geometry = idINS2square(idINS)) |> st_as_sf(sf_column_name = "geometry", crs = 3035) |> st_transform(crs = 4326) |> st_centroid() |> select(NB, id= idINS)


#file.remove("/files/la rochelle/r5/newlines-gtfs.zip")

# Choix du jour du transit
jour_du_transit <- plage(localr5) |> choisir_jour_transit()
message(
  "jour retenu: \n{lubridate::wday(jour_du_transit, label = TRUE, abbr = FALSE)} {jour_du_transit}" |> glue())

# ---- CALCUL DE L'ACCESSIBILITE ----

future::plan("multisession", workers=1)


r5 <- routing_setup_r5(path = localr5, date=jour_du_transit, n_threads = 16, mode = "CAR",
                          elevation = "NONE",
                          overwrite = TRUE,
                          max_rows = 50000,
                          elevation_tif = "elevation.tif", # calcule les dénivelés si di est true
                          max_rides=1)
r5_di <- routing_setup_r5(path = localr5, date=jour_du_transit, n_threads = 16, mode = "CAR",
                          elevation = "NONE",
                          overwrite = TRUE,
                          di = TRUE, # Nécessaire pour les distances !
                          max_rows = 50000,
                          elevation_tif = "elevation.tif", # calcule les dénivelés si di est true
                          max_rides=1)

street_net <- street_network_to_sf(r5$core)

# a <- r5r::travel_time_matrix(r5r_core = r5$core, origins = les_individus[1:50,], destinations = les_opportunites[1:50,], mode = "CAR")
# a_ext <- r5r::expanded_travel_time_matrix(r5r_core = r5_ext$core, origins = les_individus[1:50,], destinations = les_opportunites[1:50,], mode = "CAR", breakdown = TRUE)
a_di <- r5r::detailed_itineraries(r5r_core = r5_di$core, origins = les_individus[1:50,], destinations = les_opportunites[1:50,], mode = "CAR", all_to_all = TRUE)

# ## --- ACCESS ET LE RESTE ------ -----
# 
# access <- furrr::future_map(workable_ous, function(gs) {
#   logger::log_threshold(lt)
#   logger::log_layout(logger::layout_glue_generator(
#     format = "{level} [{pid}] [{format(time, \"%Y-%m-%d %H:%M:%S\")}] {msg}"))
#   
#   logger::log_appender(logger::appender_file(logfile))
#   routing <- routing$core_init(routing)
#   purrr::map(gs, function(g) {
#     rrouting <- get_routing(routing, g)
#     print(rrouting$ancres)
#     ## Fonction access_to_groupe
#     t2d <- table2disk
#     spid <- get_pid(pids)
#     s_ou <- ou_4326[gr==g, .(id, lon, lat, x, y)]
#     logger::log_debug("aog:{g} {k} {nrow(s_ou)}")
#     
#     if(t2d && is.in.dir(g, dir))
#     {
#       logger::log_success("carreau:{g} dossier:{dir}")
#       return(data.table(file = stringr::str_c(dir, "/", g, ".rda")))
#     }
#     
#     if(is.null(rrouting$ancres))
#     {
#       tictoc::tic()
#       routing_sans_elevation <- rrouting
#       routing_sans_elevation$elevation <- NULL
#       ttm_ou <- select_ancres(s_ou, k, routing_sans_elevation)
#       les_ou_s <- ttm_ou$les_ou_s
#       
#       if(is.null(ttm_ou$error))
#       {
#         if(nrow(ttm_ou$result)>0)
#         {
#           closest <- ttm_ou$result[, .(closest=fromId[which.min(travel_time)],
#                                        tt = min(travel_time)),
#                                    by=toId][, id:=toId][, toId:=NULL]
#           s_ou <- merge(s_ou, closest, by="id")
#           delay <- max(s_ou[["tt"]])
#         }
#         else
#         {
#           delay <- 0
#         }
#         logger::log_debug("ttm_ou:{nrow(ttm_ou$result)}")
#         
#         # on filtre les cibles qui ne sont pas trop loin selon la distance euclidienne
#         quoi_f <- minimax_euclid(from=ttm_ou$les_ou, to=quoi_4326, dist=vmaxmode(rrouting$mode)*(tmax+delay))
#         
#         logger::log_debug("quoi_f:{nrow(quoi_f)}")
#         
#         # distances entre les ancres et les cibles
#         if(any(quoi_f)){
#           ttm_0 <- iso_ttm(o = ttm_ou$les_ou,
#                            d = quoi_4326[quoi_f],
#                            tmax=tmax+delay+3,
#                            routing=rrouting)}
#         else{
#           ttm_0 <- list(error=NULL, result=data.table())}
#         
#         if(!is.null(ttm_0$error))
#         {
#           logger::log_warn("carreau:{g} ou_id:{les_ou_s} erreur ttm_0 {ttm_0$error}")
#           ttm_0 <- data.table()
#         }
#         else{
#           ttm_0 <- ttm_0$result}
#         
#         if(nrow(ttm_0)>0)
#         {
#           pproches <- sort(unique(s_ou$closest))
#           ttm_0[ , npea:=nrow(quoi_4326)] [, npep:=length(unique(toId)), by=fromId]
#           logger::log_debug("toc {nrow(ttm_0)}")
#           
#           if(!is.null(pproches))
#           {
#             # boucle sur les ancres
#             ttm <- rbindlist(
#               purrr::map(pproches,
#                          function(close)
#                          {ttm_on_closest(close, s_ou, quoi_4326, ttm_0, ttm_ou$les_ou, tmax, rrouting, g)}
#               ), use.names = TRUE, fill = TRUE)
#             ttm <- rbind(ttm_0[travel_time<=tmax], ttm[travel_time<=tmax], use.names=TRUE, fill=TRUE)
#           }
#           else{
#             ttm <- ttm_0}
#           
#           time <- tictoc::toc(quiet = TRUE)
#           dtime <- (time$toc - time$tic)
#           
#           if(nrow(ttm)> 0)
#           {
#             paires_fromId <- ttm[, .(npep=npep[[1]], npea=npea[[1]]), by=fromId]
#             npea <- sum(paires_fromId$npea)
#             npep <- sum(paires_fromId$npep)
#             
#             speed_log <- stringr::str_c(
#               length(pproches),
#               " ancres ", f2si2(npea),
#               "@",f2si2(npea/dtime),"p/s demandees, ",
#               f2si2(npep),"@",f2si2(npep/dtime), "p/s retenues")
#             
#             if(!ttm_out)
#             {
#               ttm_d <- merge(ttm, quoi_4326, by.x="toId", by.y="id", all.x=TRUE)
#               ttm_d <- ttm_d[, purrr::map(.SD,~sum(., na.rm=TRUE)),
#                              by=c("fromId", "travel_time"),
#                              .SDcols=(opp_var)]
#               ttm_d <- merge(ttm_d, paires_fromId, by="fromId")
#               ttm_d[, gr:=g]
#             }
#             else
#             {
#               ttm_d <- ttm
#             }
#             logger::log_success("carreau:{g} {speed_log}")
#           }
#           else
#           {
#             ttm_d <- NULL
#             logger::log_warn("carreau:{g} ttm vide")
#           }
#         } # close nrow(ttm_0)>0
#         else # nrow(ttm_0)==0
#         {
#           time <- tictoc::toc(quiet=TRUE)
#           logger::log_warn("carreau:{g} ou_id:{les_ou_s} ttm_0 vide")
#           logger::log_warn("la matrice des distances entre les ancres et les opportunites est vide")
#           ttm_d <- NULL
#         }
#       }
#       else  # nrow(ttm_ou)==0
#       {
#         logger::log_warn("carreau:{g} ou_id:{les_ou_s} ttm_ou vide")
#         logger::log_warn("la matrice des distances interne au carreau est vide")
#         ttm_d <- NULL}
#     }
#     else # ancres=FALSE pas besoin de finasser, on y va brutal puisque c'est déjà calculé
#     {
#       ttm <- iso_ttm(o=s_ou, d=quoi_4326, tmax=tmax+1, routing=rrouting)$result
#       ttm_d1 <- merge(ttm, quoi_4326, by.x="toId", by.y="id", all.x=TRUE)
#       ttm_d <- ttm_d1[, purrr::map(.SD,~sum(., na.rm=TRUE)),
#                       by=c("fromId", "travel_time"),
#                       .SDcols=(opp_var)]
#       ttm_d2 <- ttm_d1[, .(npea=.N, npep=.N), by=fromId] [, gr:=g]
#       ttm_d <- merge(ttm_d, ttm_d2, by="fromId")
#     }
#     
#     if(t2d)
#     {
#       file <- stringr::str_c(dir,"/", g, ".rda")
#       qs::qsave(ttm_d, file, preset="fast", nthreads = 4)
#       ttm_d <- data.table(file=file)
#     }
#     ttm_d
#   })
# }, .options=furrr::furrr_options(seed=TRUE,
#                                  stdout=FALSE ,
#                                  packages=packages)) |>
#   purrr::flatten()
# 
# 
# 
# 
# access_names <- names(access)
# access <- rbindlist(access, use.names = TRUE, fill = TRUE)
# 
# message("...finalisation du routing engine")
# gc()
# pl <- future::plan()
# future::plan(pl) # pour reprendre la mémoire
# if(table2disk) {
#   access <- purrr::map(set_names(access$file, access_names),~{
#     tt <- qs::qread(.x, nthreads=4)
#     if(is.null(tt)) return(NULL)
#     tt[, .(fromId, toId, travel_time)]
#     setkey(tt, fromId)
#     setindex(tt, toId)
#     tt
#   }) |> purrr::compact()
#   
#   # if(length(access)>1)
#   #   access <- rbindlist(access, use.names = TRUE)
# }
# npaires <- sum(map_dbl(access, nrow))
# res <- list(
#   type = "dt",
#   origin = routing$type,
#   origin_string = routing$string,
#   string = glue::glue("matrice de time travel {routing$type} precalculee"),
#   time_table = access,
#   fromId = ou_4326[, .(id, lon, lat, x, y, gr)],
#   toId = quoi_4326[, .(id, lon, lat, x, y)],
#   groupes = ou_gr,
#   resolution = groupes$resINS,
#   res_ou = resolution,
#   res_quoi = res_quoi,
#   ancres = FALSE,
#   future =TRUE,
#   mode = routing$mode)
# 
# 
# dtime <- as.numeric(Sys.time()) - as.numeric(start_time)
# red <- 100 * (npaires_brut - npaires) / npaires_brut
# tmn <- second2str(dtime)
# speed_b <- npaires_brut / dtime
# speed <- npaires / dtime
# mtime <- glue::glue("{tmn} - {f2si2(npaires)} routes - {f2si2(speed_b)} routes(brut)/s - {f2si2(speed)} routes/s - {signif(red,2)}% reduction")
# message(mtime)
# logger::log_success("{routing$string} en {mtime}")
# attr(res, "routing") <- glue::glue("{routing$string} en {mtime}")
# message("...nettoyage")
# future::plan() # remplace plan(plan())
# gc()
#   
# #  res
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# # ## vélo
# # # passer à 8*16 = 128 vCPU
# # future::plan("multisession", workers=1)
# # rJava::.jinit(silent=TRUE)
# # logger::log_threshold("INFO")
# # 
# # r5_bike <- routing_setup_r5(path = localr5, date=jour_du_transit, n_threads = 16, mode = "BICYCLE",
# #                             elevation = elevation,
# #                             overwrite = TRUE,
# #                             di = dic, # Nécessaire pour les distances !
# #                             max_rows = 50000, 
# #                             elevation_tif = "elevation.tif", # calcule les dénivelés si di est true
# #                             max_rides=1) 
# # 
# # # s'il marche pas pour la mémoire de Java, 
# # # aller sur "usethis::edit_r_profile()" 
# # # et insérer "options(java.parameters = '-Xmx32G')", 
# # # après redémarrer pour confirmer le changement
# # 
# # iso_bike_dt <- iso_accessibilite(quoi = les_opportunites, 
# #                                  ou = les_individus, 
# #                                  resolution = resol,
# #                                  tmax = 120, 
# #                                  pdt = 1, chunk=1e+6,
# #                                  dir = "temp_be_test",
# #                                  routing = r5_bike,
# #                                  ttm_out = TRUE,
# #                                  future=TRUE)
# # 
# # 
# # ## marche à pied
# # # passer à 8*16 = 128 vCPU
# # future::plan("multisession", workers=1)
# # rJava::.jinit(silent=TRUE)
# # logger::log_threshold("INFO")
# # 
# # r5_walk <- routing_setup_r5(path = localr5, 
# #                             date=jour_du_transit, 
# #                             n_threads = 16,
# #                             mode = "WALK",
# #                             
# #                             elevation = elevation,
# #                             overwrite = TRUE, 
# #                             di = dic, 
# #                             elevation = elevation,
# #                             elevation_tif = "elevation.tif", 
# #                             max_rows = 50000, 
# #                             max_rides = 1)
# # 
# # iso_walk_dt <- iso_accessibilite(quoi = les_opportunites, 
# #                                  ou = les_individus, 
# #                                  resolution = resol,
# #                                  tmax = 90, 
# #                                  pdt = 1,
# #                                  dir = "temp_we_test",
# #                                  routing = r5_walk,
# #                                  ttm_out = TRUE,
# #                                  future = TRUE)
# # 
# # ## voiture r5
# # # passer à 8*15 = 120 vCPU
# # # ca prend 20h à calculer (20h*120vCPU = 100j vCPU) pour la Rochelle
# # future::plan("multisession", workers=1)
# # rJava::.jinit(silent=TRUE)
# # logger::log_threshold("INFO")
# # 
# # r5_car5 <- routing_setup_r5(path = localr5, 
# #                             date=jour_du_transit, 
# #                             n_threads = 16, 
# #                             mode = "CAR",
# #                             
# #                             elevation = elevation,
# #                             overwrite = TRUE, 
# #                             di = dic,
# #                             max_rows=100000,
# #                             max_rides = 1)
# # 
# # iso_car5_dt <- iso_accessibilite(quoi = les_opportunites, 
# #                                  ou = les_individus, 
# #                                  resolution = resol,
# #                                  tmax = 120, 
# #                                  pdt = 1,
# #                                  chunk = 1e+7,
# #                                  dir = "temp_cr5_test",
# #                                  routing = r5_car5,
# #                                  ttm_out = TRUE,
# #                                  future = FALSE)
# # 
# # 

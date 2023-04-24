elevation_file = "~/files/la rochelle/r5_base/elevation.tif"
test_var_file = "~/larochelle/v2/mobscol/marges_mobsco.rda"
localr5 = "~/files/la rochelle/r5_base"

load(test_var_file)

library(data.table)
library(sf)
library(dplyr)
library(r5r)

o <- indiv_l 
d <- opport_l |> mutate(geometry = idINS2square(idINS)) |> st_as_sf(sf_column_name = "geometry", crs = 3035) |> st_centroid()
tmax <- 90
routing <- routing_setup_r5(path = localr5, n_threads = 16, mode = "BICYCLE",
                            use_elevation=TRUE,
                            overwrite=TRUE,
                            di=TRUE, # Nécessaire pour les distances !
                            max_rows=50000, 
                            max_rides=1) 

rm(list = ls(pattern = "indiv"))
rm(list = ls(pattern = "opport"))
rm(list = ls(pattern = "mobsco"))

quoi <- d
res_quoi <- 200

rf <- 1
qxy <- quoi  |>
  sf::st_transform(3035) |>
  sf::st_coordinates()
qins <- idINS3035(qxy, resolution = res_quoi, resinstr = FALSE)
qag <- quoi|>
  sf::st_drop_geometry()  |>
  as.data.frame() |>
  as.data.table()
qag <- qag[, id:=qins] [, lapply(.SD, function(x) sum(x, na.rm=TRUE)), by=id, .SDcols="NB"]
qag[, geometry:=idINS2square(qag$id, resolution=res_quoi)]
quoi <- sf::st_as_sf(qag)

r = iso_ouetquoi_4326(ou = o, quoi = d, res_quoi = 200)

gc()
o <- o[, .(id=as.character(id),lon,lat)]
d <- d[, .(id=as.character(id),lon,lat)]
od <- ksplit(
  CJ(o = o$id, d=d$id),
  k=max(1,ceiling(nrow(o)*nrow(d)/routing$max_rows)))
tt <- Sys.time()
res <- map(od, function(od_element) {
  oCJ <- data.table(id=od_element$o)
  dCJ <- data.table(id=od_element$d)
  res <- safe_r5_di(
    r5r_core = routing$core,
    origins = o[oCJ, on="id"],
    destinations = d[dCJ, on="id"],
    mode=routing$mode,
    mode_egress="WALK",
    departure_datetime = routing$departure_datetime,
    max_walk_time = routing$max_walk_time,
    max_bike_time = Inf,
    max_trip_duration = tmax+1,
    walk_speed = routing$walk_speed,
    bike_speed = routing$bike_speed,
    max_rides = routing$max_rides,
    max_lts = routing$max_lts,
    shortest_path= TRUE,
    n_threads = routing$n_threads,
    verbose=FALSE,
    progress=FALSE,
    drop_geometry=is.null(routing$elevation_tif))
})
res <- purrr::transpose(res)
res$result <- rbindlist(res$result)
if("geometry"%in%names(res$result))
  res$result <- st_as_sf(res$result)
res$error <- compact(res$error)
if(length(res$error)==0) res$error <- NULL
logger::log_debug("calcul de distances ({round(as.numeric(Sys.time()-tt), 2)} s. {nrow(od)} paires)")

if(is.null(res$error)) {
  if(nrow(res$result)>0) {
    if(!is.null(routing$elevation_tif)) {
      # on discretise par pas de 10m pour le calcul des dénivelés
      # ca va plus vite que la version LINESTRING (x10)
      # avec un zoom à 13 les carreaux font 5x5m
      # mais on n'attrape pas le pont de l'ile de Ré
      tt <- Sys.time()
      pp <- sf::st_coordinates(
        sf::st_cast(
          sf::st_segmentize(
            st_geometry(res$result),
            dfMaxLength = routing$dfMaxLength),
          "MULTIPOINT"))
      elvts <- data.table(id = pp[,3], h = terra::extract(routing$elevation_data, pp[, 1:2]))
      setnames(elvts, "h.layer", "h")
      elvts[, h:= nafill(h, type="locf")]
      elvts[, dh:= h-shift(h, type="lag", fill=NA), by="id"]
      deniv <- elvts[, .(deniv=sum(dh, na.rm=TRUE), deniv_pos=sum(dh[dh>0], na.rm=TRUE)), by="id"]
      deniv[, id:=NULL]
      logger::log_debug("calcul d'élévation ({round(as.numeric(Sys.time()-tt), 2)} s.)")
      resdi <- cbind(as.data.table(st_drop_geometry(res$result)), deniv)
    } else {
      resdi <- as.data.table(res$result)
      resdi[, `:=`(deniv=NA, deniv_pos=NA)]
    }
    setnames(resdi, c("to_id", "from_id"), c("toId", "fromId"))
    resdi <- resdi[ , .(travel_time = as.integer(sum(total_duration)),
                        distance = sum(distance),
                        deniv = sum(deniv),
                        deniv_pos = sum(deniv_pos),
                        legs = .N), by=c("fromId", "toId")]
    resdi[, `:=`(fromId=as.integer(fromId), toId=as.integer(toId))]
    
    res$result <- resdi
  }
} else # quand il y a erreur on renvoie une table nulle
  res$result <- data.table(
    fromId=numeric(),
    toId=numeric(),
    travel_time=numeric(),
    distance=numeric(),
    deniv = numeric(),
    deniv_pos = numeric(),
    legs=numeric())
res
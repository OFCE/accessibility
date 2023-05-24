#' calcul du temps de trajet selon le système de routing choisi.
#' /code{iso_ttm} calcule un temps de trajets
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
iso_ttm <- function(o, d, tmax, routing)
{
  r <- switch(routing$type,
              "r5" = r5_ttm(o, d, tmax, routing),
              "r5_ext" = r5_ettm(o, d, tmax, routing),
              "r5_di" = r5_di(o, d, tmax, routing),
              "otpv1" = otpv1_ttm(o, d, tmax, routing),
              "osrm" = osrm_ttm(o, d, tmax, routing),
              "dt"= dt_ttm(o, d, tmax, routing),
              "euclidean" = euc_ttm(o, d, tmax, routing),
              "dodgr" = dodgr_ttm(o, d, tmax, routing))
  r
}

#' définit le type du routeur
#'
#' @param routing système de routing
safe_ttm <- function(routing)
{
  switch(routing$type,
         "r5" = r5_ttm,
         "otpv1" = otpv1_ttm,
         "osrm" = osrm_ttm,
         "dt"= dt_ttm,
         "euclidean" = euc_ttm,
         "dodgr" = dodgr)
}

#' définit le type du routeur
#'
#' @param delay délai pour l'heure de départ
#' @param routing système de routing
delayRouting <- function(delay, routing)
{
  switch(routing$type,
         "r5" = {
           res <- routing
           res$departure_datetime <-
             as.POSIXct(routing$departure_datetime+delay*60,
                        format = "%d-%m-%Y %H:%M:%S")
           res},
         "otpv1" = {routing}, # to do
         "osrm" = {routing}, # no departure time
         "data.table"= {routing},
         "dodgr"= {routing}) # rien to do
}

#' wrapper pour travel_time_matrix
#'
#' @param ... cf r5r::travel_time_matrix
safe_r5_ttm <- purrr::safely(r5r::travel_time_matrix)

#' fonction de récupération
#'
#' @inheritParams r5r::travel_time_matrix
#'

quiet_r5_ttm <- function(...) {
  utils::capture.output(
    r <- safe_r5_ttm(...),
    file=NULL,
    type=c("output", "message"),
    split = FALSE, append = TRUE)
  r
}

#' calcul du temps de trajet avec r5
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
r5_ttm <- function(o, d, tmax, routing)
{
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  res <- quiet_r5_ttm(
    r5r_core = routing$core,
    origins = o,
    destinations = d,
    mode=routing$mode,
    departure_datetime = routing$departure_datetime,
    max_walk_time = routing$max_walk_time,
    max_trip_duration = tmax+1,
    time_window = as.integer(routing$time_window),
    percentiles = routing$percentile,
    walk_speed = routing$walk_speed,
    bike_speed = routing$bike_speed,
    max_rides = routing$max_rides,
    max_lts = routing$max_lts,
    n_threads = routing$n_threads,
    verbose=FALSE,
    progress=FALSE)
  if(!is.null(res$error))
  {
    gc()
    res <- safe_r5_ttm(
      r5r_core = routing$core,
      origins = o,
      destinations = d,
      mode=routing$mode,
      departure_datetime = routing$departure_datetime,
      max_walk_time = routing$max_walk_time,
      max_trip_duration = tmax+1,
      time_window = as.integer(routing$time_window),
      percentiles = routing$percentile,
      walk_speed = routing$walk_speed,
      bike_speed = routing$bike_speed,
      max_rides = routing$max_rides,
      n_threads = routing$n_threads,
      verbose=FALSE,
      progress=FALSE)
    if(is.null(res$error)) logger::log_warn("second r5::travel_time_matrix ok")
  }

  if (is.null(res$error)&&nrow(res$result)>0)
    res$result[, `:=`(fromId=as.integer(from_id), toId=as.integer(to_id), travel_time=as.integer(travel_time_p50))]
  else
  {
    logger::log_warn("error r5::travel_time_matrix, give an empty matrix after 2 attemps")
    res$result <- data.table(fromId=numeric(), toId=numeric(), travel_time=numeric())
  }
  res
}


#' wrapper pour expanded_travel_time_matrix
#'
#' @param ... cf r5r::travel_time_matrix
safe_r5_ettm <- purrr::safely(r5r::expanded_travel_time_matrix)

#' fonction de récupération
#'
#' @inheritParams r5r::expanded_travel_time_matrix
#'

quiet_r5_ettm <- function(...) {
  utils::capture.output(
    r <- safe_r5_ettm(...),
    file=NULL,
    type=c("output", "message"),
    split = FALSE, append = TRUE)
  r
}

#' calcul du temps de trajet avec r5
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
r5_ettm <- function(o, d, tmax, routing)
{
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  res <- quiet_r5_ettm(
    r5r_core = routing$core,
    origins = o,
    destinations = d,
    mode=routing$mode,
    departure_datetime = routing$departure_datetime,
    max_walk_time = routing$max_walk_time,
    max_trip_duration = tmax+1,
    time_window = as.integer(routing$time_window),
    walk_speed = routing$walk_speed,
    bike_speed = routing$bike_speed,
    max_rides = routing$max_rides,
    max_lts = routing$max_lts,
    n_threads = routing$n_threads,
    breakdown = TRUE,
    verbose=FALSE,
    progress=FALSE)
  if(!is.null(res$error))
  {
    gc()
    res <- safe_r5_ettm(
      r5r_core = routing$core,
      origins = o,
      destinations = d,
      mode=routing$mode,
      departure_datetime = routing$departure_datetime,
      max_walk_time = routing$max_walk_time,
      max_trip_duration = tmax+1,
      time_window = as.integer(routing$time_window),
      percentiles = routing$percentile,
      walk_speed = routing$walk_speed,
      bike_speed = routing$bike_speed,
      max_rides = routing$max_rides,
      n_threads = routing$n_threads,
      breakdown = TRUE,
      verbose=FALSE,
      progress=FALSE)
    if(is.null(res$error)) logger::log_warn("second r5::travel_time_matrix ok")
  }
  
  if (is.null(res$error)&&nrow(res$result)>0)
    res$result[, `:=`(fromId=as.integer(from_id), toId=as.integer(to_id), travel_time=as.integer(total_time), n_rides, access_time, wait_time, ride_time, transfer_time, egress_time)]
  else
  {
    logger::log_warn("error r5r::expanded_travel_time_matrix, give an empty matrix after 2 attemps")
    res$result <- data.table(fromId=numeric(), toId=numeric(), travel_time=numeric())
  }
  res
}

#' wrapper pour detailed_itineraries
#'
#' @inheritParams r5r::detailed_itineraries
safe_r5_di <- purrr::safely(r5r::detailed_itineraries)

#' calcul des itinéraires détaillés avec r5r
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
r5_di <- function(o, d, tmax, routing) {
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
        setnames(elvts, "h.elevation", "h")
        elvts[, h:= nafill(h, type="locf")]
        elvts[, dh:= h-data.table::shift(h, type="lag", fill=NA), by="id"]
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
}

#' calcul du temps de trajet avec une distance euclidienne
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
#' @import sf
euc_ttm <- function(o, d, tmax, routing)
{
  mode <- routing$mode
  vitesse <- routing$speed

  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]

  o_3035 <- sf_project(from=st_crs(4326), to=st_crs(3035), o[, .(lon, lat)])
  d_3035 <- sf_project(from=st_crs(4326), to=st_crs(3035), d[, .(lon, lat)])
  dist <- rdist::cdist(X=o_3035, Y=d_3035, metric="euclidean", p=2)
  colnames(dist) <- d$id
  rownames(dist) <- o$id
  dt <- data.table(dist, keep.rownames=TRUE)
  setnames(dt, "rn", "fromId")
  dt[, fromId:=as.integer(fromId)]
  dt <- melt(dt, id.vars="fromId", variable.name="toId", value.name = "distance", variable.factor = FALSE)
  dt[, travel_time := distance/1000/vitesse*60]
  dt <- dt[travel_time<tmax,]
  dt[, `:=`(toId = as.integer(toId), travel_time = as.integer(ceiling(travel_time)))]
  list(error=NULL,
       result=dt)
}

#' calcul du temps de trajet avec otp. Ne marche pas.
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
otpv1_ttm <- function(o, d, tmax, routing)
{
  # ca marche pas parce que OTP ne renvoie pas de table
  # du coup il faudrait faire ça avec les isochrones
  # ou interroger OTP paire par paire
  # la solution ici est très très lente et donc pas utilisable

  o[, `:=`(k=1, fromId=id, fromlon=lon, fromlat=lat)]
  d[, `:=`(k=1, toId=id, tolon=lon, tolat=lat)]
  paires <- merge(o,d, by="k", allow.cartesian=TRUE)
  temps <- furrr::future_map_dbl(1:nrow(paires), ~{
    x <- paires[.x, ]
    t <- otpr::otp_get_times(
      routing$otpcon,
      fromPlace= c(x$fromlat, x$fromlon),
      toPlace= c(x$tolat, x$tolon),
      mode= routing$mode,
      date= routing$date,
      time= routing$time,
      maxWalkDistance= routing$maxWalkDistance,
      walkReluctance = routing$walkReluctance,
      arriveBy = routing$arriveBy,
      transferPenalty = routing$transferPenalty,
      minTransferTime = routing$minTransferTime,
      detail = FALSE,
      includeLegs = FALSE)
    if(t$errorId=="OK") t[["duration"]]
    else NA
  })
  paires[ , .(fromId, toId)] [, temps:=as.integer(temps)]
}

#' calcul du temps de trajet avec osrm
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
osrm_ttm <- function(o, d, tmax, routing)
{
  options(osrm.server = routing$osrm.server,
          osrm.profile = routing$osrm.profile)
  safe_table <- purrr::safely(osrm::osrmTable)
  l_o <- o[, .(id, lon, lat)]
  l_d <- d[, .(id, lon, lat)]
  tt <- safe_table(
    src = l_o,
    dst= l_d,
    exclude=NULL,
    gepaf=FALSE,
    measure="duration")
  if(!is.null(tt$error))
  {
    gc()
    logger::log_warn("deuxieme essai osrm")
    tt <- safe_table(
      src = l_o,
      dst= l_d,
      exclude=NULL,
      gepaf=FALSE,
      measure="duration")
  }

  if(is.null(tt$error))
  {
    dt <- data.table(tt$result$duration, keep.rownames = TRUE)
    dt[, fromId:=rn |> as.integer()] [, rn:=NULL]
    dt <- melt(dt, id.vars="fromId", variable.name="toId", value.name = "travel_time", variable.factor = FALSE)
    dt <- dt[travel_time < tmax,]
    dt[, `:=`(toId = as.integer(toId), travel_time = as.integer(ceiling(travel_time)))]
    tt$result <- dt
  }
  else
    logger::log_warn("error osrm::osrmTable, give an empty matrix after 2 attemps")
  tt
}

#' calcul du temps de trajet avec data.table (??)
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#'
#' @import data.table
dt_ttm <- function(o, d, tmax, routing)
{
  o_rid <- merge(o[, .(oid=id, x, y)], routing$fromId[, .(rid=id, x, y)], by=c("x", "y"))
  d_rid <- merge(d[, .(did=id, x, y)], routing$toId[, .(rid=id, x, y)], by=c("x", "y"))
  ttm <- routing$time_table[(fromId%in%o_rid$rid), ][(toId%in%d_rid$rid),][(travel_time<tmax), ]
  ttm <- merge(ttm, o_rid[, .(oid, fromId=rid)], by="fromId")
  ttm <- merge(ttm, d_rid[, .(did, toId=rid)], by="toId")
  ttm <- ttm[, `:=`(fromId=NULL, toId=NULL)]
  setnames(ttm,old=c("oid", "did"), new=c("fromId", "toId"))
  list(
    error=NULL,
    result=ttm
  )
}

#' setup du système de routing r5
#'
#' @param data_path path
#' @param verbose par défaut, FALSE
#' @param temp_dir par défaut, FALSE
#' @param use_elevation par défaut, FALSE
#' @param overwrite par défaut, FALSE
#'
#' @export
quick_setup_r5 <- function (data_path, verbose = FALSE, temp_dir = FALSE,
                            use_elevation = FALSE, overwrite = FALSE) {

  rlang::check_installed("rJava", reason = "rJava est nécessaire pour r5r")
  rlang::check_installed("r5r", reason = "r5r est nécessaire pour ce calcul")

  checkmate::assert_directory_exists(data_path)
  checkmate::assert_logical(verbose)
  checkmate::assert_logical(temp_dir)
  checkmate::assert_logical(use_elevation)
  checkmate::assert_logical(overwrite)
  rJave::.jinit()
  data_path <- path.expand(data_path)

  any_network <- length(grep("network.dat", list.files(data_path))) > 0
  if (!(any_network)) stop("\nA network is needed")

  jars <- list.files(path=file.path(.libPaths()[1], "r5r", "jar")) |>
    purrr::keep(~ stringr::str_detect(.x,"^r5-[:graph:]*.jar"))
  jars_date <- stringr::str_extract(jars, pattern="[:digit:]{8}") |> as.numeric()
  jar <- jars[which.max(jars_date)]
  jar_file <- file.path(.libPaths()[1], "r5r", "jar", jar)
  r5r_jar <- file.path(.libPaths()[1], "r5r", "jar", "r5r_0_6_0.jar")
  .jaddClassPath(path = r5r_jar)
  .jaddClassPath(path = jar_file)
  dat_file <- file.path(data_path, "network.dat")
  if (checkmate::test_file_exists(dat_file) && !overwrite) {
    r5r_core <- .jnew("org.ipea.r5r.R5RCore", data_path,
                      verbose)
    message("\nUsing cached network.dat from ", dat_file)
  }
  else {
    return(NULL)
  }
  r5r_core$buildDistanceTables()
  return(r5r_core)
}

#' récupère le setup du système de routing r5
#'
#' @param data_path path
#' @param verbose par défaut, FALSE
#' @param temp_dir par défaut, FALSE
#' @param use_elevation par défaut, FALSE
#' @param overwrite par défaut, FALSE
#'
#'
#' @export
get_setup_r5 <- function (data_path, verbose = FALSE, temp_dir = FALSE,
                          use_elevation = FALSE, overwrite = FALSE) {

  rlang::check_installed("rJava", reason = "rJava est nécessaire pour r5r")
  rlang::check_installed("r5r", reason = "r5r est nécessaire pour ce calcul")

  checkmate::assert_directory_exists(data_path)
  checkmate::assert_logical(verbose)
  checkmate::assert_logical(temp_dir)
  checkmate::assert_logical(use_elevation)
  checkmate::assert_logical(overwrite)
  rJava::.jinit()
  data_path <- path.expand(data_path)
  any_network <- length(grep("network.dat", list.files(data_path))) > 0

  if (!(any_network)) stop("\nAn network is needed")

  jars <- list.files(path=file.path(.libPaths()[1], "r5r", "jar")) |>
    purrr::keep(~ stringr::str_detect(.x,"^r5-[:graph:]*.jar"))
  jars_date <- stringr::str_extract(jars, pattern="[:digit:]{8}") |> as.numeric()
  jar <- jars[which.max(jars_date)]
  jar_file <- file.path(.libPaths()[1], "r5r", "jar", jar)
  r5r_jar <- file.path(.libPaths()[1], "r5r", "jar", "r5r_0_6_0.jar")
  dat_file <- file.path(data_path, "network.dat")

  return(list(r5r_jar = r5r_jar, r5_jar = jar, network = dat_file))
}

#' setup du système de routing de r5
#'
#' Cette fonction est utlisée pour fabriquer le "core" r5 qui est ensuite appelé pour faire le routage.
#' Elle utilise le r5.....jar téléchargé ou le télécharge si nécessaire.
#' Un dossier contenant les informations nécessaires doit être passé en parmètre.
#' Ce dossier contient au moins un pbf (OSM), des fichiers GTFS (zip), des fichiers d'élévation (.tif)
#' Si iol contient un network.dat, celui ci sera utilisé sans reconstruire le réseau
#'
#' @param path path Chemin d'accès au dossier
#' @param overwrite Regénére le network.dat même si il est présent
#' @param date date Date où seront simulées les routes
#' @param mode mode de transport, par défaut c("WALK", "TRANSIT"), voir r5r ou r5 pour les autres modes
#' @param montecarlo nombre de tirages montecarlo par minutes de time_windows par défaut 10
#' @param max_walk_time temps maximal à pied
#' @param time_window par défaut, 1. fenêtre pour l'heure de départ en minutes
#' @param percentiles par défaut, 50., retourne les percentiles de temps de trajet (montecarlo)
#' @param walk_speed vitesse piéton
#' @param bike_speed vitesse vélo
#' @param max_lts stress maximal à vélo (de 1 enfant à 4 toutes routes), 2 par défaut
#' @param max_rides nombre maximal de changements de transport
#' @param max_rows nombre maximale de lignes passées à detailled_itirenaries
#' @param n_threads nombre de threads
#' @param jMem taille mémoire vive, plus le nombre de threads est élevé, plus la mémoire doit être importante
#' @param quick_setup dans le cas où le core existe déjà, il n'est pas recréer, plus rapide donc, par défaut, FALSE
#' @param di renvoie des itinéraires détaillés (distance, nombre de branche) en perdant le montecarlo et au prix d'une plus grande lenteur
#' @param ext renvoie une travel_time_matrix plus détaillée  si di = FALSE (utilise le fonction expanded_travel_time_matrix de r5r au lieu de travel_time_matrix)
#' @param elevation_tif nom du fichier raster (WGS84) des élévations en mètre, en passant ce paramètre, on calcule le dénivelé positif.
#'                  elevatr::get_elev_raster est un bon moyen de le générer. Fonctionne même si on n'utilise pas les élévations dans le routing
#' @param elevation méthode de clcul (NONE, TOBLER, MINETTI )
#' @param dfMaxLength longueur en mètre des segments pour la discrétization
#'
#' @export
routing_setup_r5 <- function(path,
                             date="17-12-2019 8:00:00",
                             mode=c("WALK", "TRANSIT"),
                             montecarlo=10L,
                             max_walk_time= Inf,
                             time_window=1L,
                             percentiles=50L,
                             walk_speed = 5.0,
                             bike_speed = 12.0,
                             max_lts= 2,
                             max_rides= ifelse("TRANSIT"%in%mode, 3L, 1L),
                             elevation_tif = NULL,
                             elevation = "NONE",
                             dfMaxLength = 10,
                             breakdown = FALSE,
                             overwrite = FALSE,
                             n_threads= 4L,
                             max_rows=5000,
                             jMem = "12G",
                             ext = FALSE,
                             di = FALSE,
                             quick_setup = FALSE)
{
  env <- parent.frame()
  path <- glue::glue(path, .envir = env)
  rlang::check_installed("rJava", reason = "rJava est nécessaire pour r5r")
  rlang::check_installed("r5r", reason = "r5r est nécessaire pour ce calcul")
  assertthat::assert_that(
    all(mode%in%c('TRAM', 'SUBWAY', 'RAIL', 'BUS',
                  'FERRY', 'CABLE_CAR', 'GONDOLA', 'FUNICULAR',
                  'TRANSIT', 'WALK', 'BICYCLE', 'CAR', 'BICYCLE_RENT', 'CAR_PARK')),
    msg = "incorrect transport mode")

  mode_string <- stringr::str_c(mode, collapse = "&")
  rJava::.jinit(force.init = TRUE, silent=TRUE) #modif du code ci-dessus (MP)
  r5r::stop_r5()
  #rJava::.jgc(R.gc = TRUE)

  if(quick_setup)
    core <- quick_setup_r5(data_path = path)
  else {
    out <- capture.output(
      core <- r5r::setup_r5(data_path = path, verbose=FALSE,
                            elevation=elevation, overwrite = overwrite)
    )
  }


  options(r5r.montecarlo_draws = montecarlo)
  setup <- get_setup_r5(data_path = path)
  mtnt <- lubridate::now()
  type <- case_when(di ~ "r5_di",
                    ext & !di ~ "r5_ext",
                    TRUE ~ "r5")
  list(
    type = type,
    di = di,
    path = path,
    string = glue::glue("{type} routing {mode_string} sur {path} a {mtnt}"),
    core = core,
    montecarlo = as.integer(montecarlo),
    time_window = as.integer(time_window),
    departure_datetime = as.POSIXct(date, format = "%d-%m-%Y %H:%M:%S", tz=Sys.timezone()),
    mode = mode,
    percentiles = percentiles,
    max_walk_time = max_walk_time,
    walk_speed = walk_speed,
    bike_speed = bike_speed,
    max_rides = max_rides,
    max_lts = max_lts,
    elevation = elevation,
    elevation_tif = elevation_tif,
    pkg = c("r5r", "rJava"),
    dfMaxLength = dfMaxLength,
    breakdown = breakdown,
    elevation_data = if(is.null(elevation_tif)) NULL else terra::rast(str_c(path, "/", elevation_tif)),
    max_rows = max_rows,
    n_threads = as.integer(n_threads),
    future = TRUE,
    jMem = jMem,
    r5r_jar = setup$r5r_jar,
    r5_jar = setup$r5_jar,
    future_routing = function(routing) {
      rout <- routing
      rout$elevation_data <- NULL
      return(rout)},
    core_init = function(routing){
      options(java.parameters = glue::glue('-Xmx{routing$jMem}'))
      rJava::.jinit(silent=TRUE)
      r5r::stop_r5()
      rJava::.jgc(R.gc = TRUE)
      options(r5r.montecarlo_draws = routing$montecarlo)
      core <- r5r::setup_r5(data_path = routing$path, verbose=FALSE,
                            elevation=routing$elevation)
      out <- routing
      out$core <- core
      if(!is.null(routing$elevation_tif))
        out$elevation_data <- terra::rast(str_c(routing$path, "/", routing$elevation_tif))
      return(out)
    })
}

#' setup du système de routing otp
#'
#' @param router info du serveur otp.
#' @param port par défaut, 8000.
#' @param memory par défaut, 8G.
#' @param rep chemin du repertoire.
#' @param date date des trajets.
#' @param mode mode de transit, par défaut c("WALK", "TRANSIT").
#' @param max_walk_time temps maximal à pied
#' @param precisionMeters précision demandée au serveur, par défaut 50m.
#'
#' @export
routing_setup_otpv1 <- function(
    router,
    port=8000,
    memory="8G",
    rep,
    date="12-17-2019 8:00:00",
    mode=c("WALK", "TRANSIT"),
    max_walk_time= 30,
    precisionMeters=50)
{
  s_now <- lubridate::now()
  mode_string <- stringr::str_c(mode, collapse = "&")
  list(
    type = "otpv1",
    string = glue::glue("otpv1 routing {mode_string} sur {router}(:{port}) a {s_now}"),
    otpcon = OTP_server(router=router, port=port, memory = memory, rep=rep),
    date = unlist(stringr::str_split(date, " "))[[1]],
    time = unlist(stringr::str_split(date, " "))[[2]],
    mode = mode,
    batch = FALSE,
    arriveBy = FALSE,
    walkReluctance = 2,
    maxWalkDistance = max_walk_dist,
    transferPenalty = 0,
    minTransferTime = 0,
    clampInitialWait = 0,
    offRoadDistanceMeters = 50,
    precisionMeters = precisionMeters,
    future = FALSE)
}

#' setup du système de routing osrm
#'
#' @param server port du serveur osrm, par défaut 5000.
#' @param profile mode de transport, par défaut "driving".
#' @param future calcul parallele, par défaut TRUE.
#'
#' @export
routing_setup_osrm <- function(
    server=5000,
    profile="driving",
    future=TRUE)
{
  s_now <- lubridate::now()
  list(
    type = "osrm",
    string = glue::glue("osrm routing localhost:{server} profile {profile} a {s_now}"),
    osrm.server = glue::glue("http://localhost:{server}/"),
    osrm.profile = profile,
    future = TRUE,
    pkg = "osrm",
    mode = switch(profile,
                  driving="CAR",
                  walk="WALK",
                  bike="BIKE"))
}

#' setup du système de routing euclidien
#'
#' @param mode mode de transport, par défaut, "WALK".
#' @param speed vitesse de déplacement, par défaut, 5km/h.
#'
#' @export
routing_setup_euc <- function(
    mode="WALK", speed=5)
{
  s_now <- lubridate::now()
  list(
    type = "euclidean",
    string = glue::glue("euclidien a {s_now}"),
    future = TRUE,
    mode = mode,
    speed = speed)
}

#' lancement d'un serveur java otp
#'
#' @param router nom du routeur
#' @param port par défaut, 8000
#' @param memory taille mémoire du serveur, par défaut 8G
#' @param rep répertoire
#' @export

OTP_server <- function(router="IDF1", port=8008, memory="8G", rep)
{
  safe_otp_connect <- purrr::safely(otpr::otp_connect)
  connected <- FALSE
  connection <- safe_otp_connect(router=router, port=port)
  if (!is.null(connection$error))
  {
    secureport <- port+1
    current.wd <- getwd()
    setwd("{rep}/otp_idf" |> glue::glue())
    shell("start java -Xmx{memory} -jar otp-1.4.0-shaded.jar --router {router} --graphs graphs --server --port {port} --securePort {secureport}"|> glue::glue(),
          translate = TRUE, wait = FALSE, mustWork = TRUE)
    setwd(current.wd)
    safe_otp_connect <- purrr::safely(otpr::otp_connect)
    connected <- FALSE
    while(!connected) {
      connection <- safe_otp_connect(router=router, port=port)
      connected <- is.null(connection$error)}
  }
  connection$result
}

#' setup du système de routing de dodgr
#'
#' Cette fonction met en place ce qui est nécessaire pour lancer dodgr
#' A partir d'un fichier de réseau (au format silicate, téléchargé par overpass, voir download_dodgr_osm)
#' le setup fabrique le weighted_streetnetwork à partir d'un profile par mode de transport
#' Ce fichier est enregistré est peut être ensuite utilisé pour calculer les distances ou les temps de parcours
#'
#' @param path string, chemin d'accès au dossier contenant le réseau
#' @param date string, date Date où seront simulées les routes (non utilisé)
#' @param mode string, mode de transport, par défaut "CAR" (possible (BICYCLE, WALK,...))
#' @param turn_penalty booléen, applique une pénalité pour les turns
#' @param distances booléen, calcule les distances en prime
#' @param wt_profile_file string, chemin vers le fichier des profils (écrit avec \code{dodgr::write_dodgr_wt_profile})
#' @param overwrite booléen, Regénére le reseau même si il est présent
#' @param n_threads entier, nombre de threads
#' @param overwrite reconstruit le réseau dodgr à partir de la source OSM
#'
#' @export
routing_setup_dodgr <- function(path,
                                date="17-12-2019 8:00:00",
                                mode="CAR",
                                turn_penalty = FALSE,
                                distances = FALSE,
                                wt_profile_file = NULL,
                                n_threads= 4L,
                                overwrite = FALSE)
{
  env <- parent.frame()
  path <- glue::glue(path, .envir = env)
  rlang::check_installed("dodgr", reason="requis pour le routage")
  assertthat::assert_that(
    mode%in%c("CAR", "BICYCLE", "WALK", "bicycle", "foot", "goods",
              "hgv", "horse", "moped",
              "motorcar", "motorcycle", "psv", "wheelchair"),
    msg = "incorrect transport mode")
  mode <- case_when(mode=="CAR"~"motorcar",
                    mode=="BICYCLE"~"bicycle",
                    mode=="WALK"~"foot")
  RcppParallel::setThreadOptions(numThreads = as.integer(n_threads))
  # dans le path on s'attend à 1 fichier sfosm
  loff <- list.files(path=path, pattern = "*.sfosm")
  if(length(loff)>1)
    message("Attention, il y a plusieurs candidats de réseaux dans le dossier {path}" |> glue::glue())
  graph_name <- stringr::str_c(stringr::str_remove(loff[[1]], "\\.[:alpha:]*$"), ".", mode, ".dodgrnet")
  graph_name <- glue::glue("{path}/{graph_name}")
  if(file.exists(graph_name)&!overwrite) {
    graph <- qs::qread(graph_name, nthreads=4)
    message("dodgr network en cache")
  } else {
    osm_network <- qs::qread(str_c(path, "/", loff[[1]]), nthreads=4)
    graph <- dodgr::weight_streetnet(osm_network, wt_profile=mode, wt_profile_file = wt_profile_file, turn_penalty = turn_penalty)
    qs::qsave(graph, graph_name, nthreads=4)
  }
  mtnt <- lubridate::now()
  type <- "dodgr"
  list(
    type = type,
    path = path,
    graph = graph,
    distances = distances,
    pkg = "dodgr",
    turn_penalty = turn_penalty,
    graph_name = graph_name,
    string = glue::glue("{type} routing {mode} sur {path} a {mtnt}"),
    departure_datetime = as.POSIXct(date, format = "%d-%m-%Y %H:%M:%S", tz=Sys.timezone()),
    mode = mode,
    n_threads = as.integer(n_threads),
    future = TRUE,
    future_routing = function(routing) {
      rout <- routing
      rout$graph <- NULL
      rout$elevation_data <- NULL
      return(rout)
    },
    core_init = function(routing){
      RcppParallel::setThreadOptions(numThreads = routing$n_threads)
      # refresh le graph à partir du disque en cas de multicore
      rout <- routing
      rout$graph <- qs::qread(routing$graph_name, nthreads=4)
      if(!is.null(routing$elevation))
        rout$elevation_data <- terra::rast(str_c(routing$path, "/", routing$elevation))
      return(rout)
    })
}

dodgr_ttm <- function(o, d, tmax, routing)
{
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  temps <- dodgr::dodgr_times(
    graph = routing$graph,
    from = o,
    to = d,
    shortest=FALSE)
  temps <- data.table(temps, keep.rownames = TRUE)
  temps[, fromId:=rn |> as.integer()] [, rn:=NULL]
  temps <- melt(temps, id.vars="fromId", variable.name="toId", value.name = "travel_time", variable.factor = FALSE)
  temps <- temps[travel_time<=tmax*60,]
  if(routing$distances) {
    dist <- dodgr::dodgr_distances(
      graph = routing$graph,
      from = o,
      to = d,
      shortest=FALSE,
      parallel = TRUE)
    dist <- data.table(dist, keep.rownames = TRUE)
    dist[, fromId:=rn |> as.integer()] [, rn:=NULL]
    dist <- melt(dist, id.vars="fromId", variable.name="toId", value.name = "distance", variable.factor = FALSE)
    temps <- merge(temps, dist, by=c("fromId", "toId"), all.x=TRUE, all.y=FALSE)
  }
  erreur <- NULL

  if (nrow(temps)>0)
    temps[, `:=`(fromId=as.integer(fromId), toId=as.integer(toId), travel_time=as.integer(travel_time/60))]
  else
  {
    erreur <- "dodgr::travel_time_matrix empty"
    temps <- data.table(fromId=numeric(), toId=numeric(), travel_time=numeric(), distance=numeric())
    logger::log_warn(erreur)
  }
  return(list(result=temps, error=erreur))
}


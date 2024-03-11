#' wrapper pour travel_time_matrix
#'
#' @param ... cf r5r::travel_time_matrix
safe_r5_ttm <- purrr::safely(r5r::travel_time_matrix)

#' fonction de récupération
#'
#' @inheritParams r5r::travel_time_matrix
#'

safe_r5_ttm_ext <- purrr::safely(r5r::expanded_travel_time_matrix)


#' calcul du temps de trajet avec r5
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#' @param dist_only Ne calcule que les distances
#'
#' @import data.table
r5_ttm <- function(o, d, tmax, routing, dist_only = TRUE)
{
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  if(!routing$extended) {
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
      max_lts = routing$max_lts,
      n_threads = routing$n_threads,
      draws_per_minute = routing$montecarlo,
      verbose=FALSE,
      progress=FALSE)
  } else {
    res <- safe_r5_ttm_ext(
      r5r_core = routing$core,
      origins = o,
      destinations = d,
      mode=routing$mode,
      departure_datetime = routing$departure_datetime,
      max_walk_time = routing$max_walk_time,
      max_trip_duration = tmax+1,
      time_window = as.integer(routing$time_window),
      breakdown = TRUE,
      walk_speed = routing$walk_speed,
      bike_speed = routing$bike_speed,
      max_rides = routing$max_rides,
      max_lts = routing$max_lts,
      n_threads = routing$n_threads,
      draws_per_minute = routing$montecarlo,
      verbose=FALSE,
      progress=FALSE
    )
  }
  
  if (is.null(res$error)&&nrow(res$result)>0)
    if(!routing$extended) {
    res$result[, `:=`(fromId=as.integer(from_id), toId=as.integer(to_id), travel_time=as.integer(travel_time_p50))]
    } else {
      res$result <- res$result[, .(
        access_time = quantile(access_time, routing$percentiles, na.rm=TRUE),
        egress_time = quantile(egress_time, routing$percentiles, na.rm=TRUE),
        travel_time = quantile(total_time, routing$percentiles, na.rm=TRUE),
        n_rides = quantile(n_rides, routing$percentiles, na.rm=TRUE)), by = c("from_id", "to_id")]
      res$result <- res$result[, `:=`(fromId=as.integer(from_id), toId=as.integer(to_id), travel_time=as.integer(travel_time))]
    }
  
  else
  {
    logger::log_warn("error r5::travel_time_matrix, matrice vide")
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
#' @param dist_only Ne calcule que les distances
#'
#' @import data.table
r5_di <- function(o, d, tmax, routing, dist_only = FALSE) {
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
#' @param montecarlo nombre de tirages montecarlo par minutes de time_windows par défaut 5
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
#' @param extended retourne plus d'information
#' @param elevation_tif nom du fichier raster (WGS84) des élévations en mètre, en passant ce paramètre, on calcule le dénivelé positif.
#'                  elevatr::get_elev_raster est un bon moyen de le générer. Fonctionne même si on n'utilise pas les élévations dans le routing
#' @param elevation méthode de calcul (NONE, TOBLER, )
#' @param dfMaxLength longueur en mètre des segments pour la discrétization
#'
#' @export
routing_setup_r5 <- function(path,
                             date="17-12-2019 8:00:00",
                             mode=c("WALK", "TRANSIT"),
                             montecarlo=5L,
                             max_walk_time= Inf,
                             time_window=1L,
                             percentiles=.5 ,
                             walk_speed = 5.0,
                             bike_speed = 12.0,
                             max_lts= 2,
                             max_rides= ifelse("TRANSIT"%in%mode, 3L, 1L),
                             elevation_tif = NULL,
                             elevation = "NONE",
                             dfMaxLength = 10,
                             extended = FALSE,
                             overwrite = FALSE,
                             n_threads= 4L,
                             max_rows=5000,
                             jMem = "12G",
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
  
  
  setup <- get_setup_r5(data_path = path)
  mtnt <- lubridate::now()
  type <-  "r5"
  list(
    type = type,
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
    extended = extended,
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
                            elevation = "NONE", overwrite = FALSE) {
  
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
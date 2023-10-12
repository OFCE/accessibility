#' Télécharge d'OSM pour dodgr en format silicate
#'
#' le format silicate permet à dodgr de pondérer les tournants,
#' les arrêts aux feux rouge ainsi que les restrictions de circulation
#'
#' @param box les limites de la zone à télécharger, au format st_bbox()
#' @param workers le nombre de workers
#' @param .progress affiche un indicateur de progression
#'
#' @return un osmdata_sc
#' @export
#'
#'
#'
download_osmsc <- function(box, workers = 1, .progress = TRUE) {
  
  rlang::check_installed(
    "osmdata", 
    version="0.2.5.005", 
    reason = "pour utiliser download_sc, il faut au moins la version 0.2.5.005 sir github ropensci/osmdata")
  require(osmdata, quietly = TRUE)
  tictoc::tic()
  
  dir.create(glue::glue("logs"), showWarnings = FALSE, recursive = TRUE)
  timestamp <- lubridate::stamp("15-01-20 10h08.05", orders = "dmy HMS", quiet = TRUE) (lubridate::now(tzone = "Europe/Paris"))
  logfile <- glue::glue("logs/download_osmsc.{timestamp}.log")
  logger::log_appender(logger::appender_file(logfile))
  
  split_bbox <- function (bbox, grid = 2, eps = 0.05) {
    xmin <- bbox ["x", "min"]
    ymin <- bbox ["y", "min"]
    dx <- (bbox ["x", "max"] - bbox ["x", "min"]) / grid
    dy <- (bbox ["y", "max"] - bbox ["y", "min"]) / grid
    bboxl <- list ()
    for (i in 1:grid) {
      for (j in 1:grid) {
        b <- matrix (c (xmin + ((i - 1 - eps) * dx),
                        ymin + ((j - 1 - eps) * dy),
                        xmin + ((i + eps) * dx),
                        ymin + ((j + eps) * dy)),
                     nrow = 2, dimnames = dimnames (bbox))
        bboxl <- append (bboxl, list (b))}}
    return(bboxl)
  }
  
  bbox <- box |> matrix(nrow = 2, dimnames = list(list("x","y"), list("min", "max")))
  logger::log_success("bbox {box}")
  
  queue <- split_bbox(bbox, grid=max(1,round(sqrt(2*workers))))
  fts <- c("\"highway\"", "\"restriction\"", "\"access\"",
           "\"bicycle\"", "\"foot\"", "\"motorcar\"", "\"motor_vehicle\"",
           "\"vehicle\"", "\"toll\"")
  
  saved_plan <- future::plan()
  future::plan("multisession", workers = workers)
  osm <- furrr::future_map(queue, ~{
    logger::log_appender(logger::appender_file(logfile))
    local_q <- list(.x)
    
    result <- list()
    split <- 0
    while(length (local_q) > 0) {
      
      opres <- NULL
      opres <- try ({
        opq (bbox = local_q[[1]], timeout = 25) |>
          add_osm_features(features = fts) |>
          osmdata_sc(quiet = TRUE)
      })
      
      if (class(opres)[1] != "try-error") {
        logger::log_success("downloaded après {split} split")
        result <- append(result, list (opres))
        local_q <- local_q[-1]
      } else {
        split <- split + 1
        logger::log_info("{opres} split ({split})")
        
        bboxnew <- split_bbox(local_q[[1]])
        queue <- append(bboxnew, local_q[-1])
      }
    }
    
    final <- do.call (c, result)
    
  },
  .progress=.progress,
  .options = furrr::furrr_options(seed=TRUE))
  future::plan(saved_plan)
  osm <- do.call(c, osm)
  time <- tictoc::toc(log = TRUE, quiet = TRUE)
  dtime <- (time$toc - time$tic)
  cli::cli_alert_success(
    "OSM silicate téléchargé: {dtime%/%60}m {round(dtime-60*dtime%/%60)}s {signif(lobstr::obj_size(osm)/1024/1024, 3)} Mb")
  
  return(osm)
}

#' Télécharge d'OSM pour dodgr en format sf
#'
#' le format silicate permet à dodgr de pondérer les tournants,
#' les arrêts aux feux rouge ainsi que les restrictions de circulation
#'
#' @param box les limites de la zone à télécharger, au format st_bbox()
#' @param workers le nombre de workers
#' @param .progress affiche un indicateur de progression
#'
#' @return un osmdata_sc
#' @export
#'
#'
#'
download_osmsf <- function(box, workers = 1, .progress = TRUE, trim= FALSE) {
  
  rlang::check_installed("osmdata", reason = "pour utiliser download_sf`")
  require(osmdata, quietly = TRUE)
  tictoc::tic()
  split_bbox <- function (bbox, grid = 2, eps = 0.05) {
    xmin <- bbox ["x", "min"]
    ymin <- bbox ["y", "min"]
    dx <- (bbox ["x", "max"] - bbox ["x", "min"]) / grid
    dy <- (bbox ["y", "max"] - bbox ["y", "min"]) / grid
    bboxl <- list ()
    for (i in 1:grid) {
      for (j in 1:grid) {
        b <- matrix (c (xmin + ((i - 1 - eps) * dx),
                        ymin + ((j - 1 - eps) * dy),
                        xmin + ((i + eps) * dx),
                        ymin + ((j + eps) * dy)),
                     nrow = 2, dimnames = dimnames (bbox))
        bboxl <- append (bboxl, list (b))}}
    return(bboxl)}
  bbox <- box |> matrix(nrow = 2, dimnames = list(list("x","y"), list("min", "max")))
  queue <- split_bbox(bbox, grid=max(1,workers%/%2))
  
  saved_plan <- future::plan()
  future::plan("multisession", workers = workers)
  
  osm <- furrr::future_map(queue, ~{
    local_q <- list(.x)
    
    result <- list()
    split <- 0
    while(length (local_q) > 0) {
      
      opres <- NULL
      opres <- try ({
        opq (bbox = local_q[[1]], timeout = 25) |>
          add_osm_feature(key = "highway") |>
          osmdata_sf(quiet = TRUE)})
      
      if (class(opres)[1] != "try-error") {
        opres <- opres |>
          osm_poly2line()
        opres$osm_points <- NULL
        opres$osm_multilines <- NULL
        opres$osm_polygons <- NULL
        opres$osm_multipolygons <- NULL
        result <- append(result, list (opres))
        local_q <- local_q[-1]
      } else {
        bboxnew <- split_bbox(local_q[[1]])
        
        queue <- append(bboxnew, local_q[-1])
      }
    }
    
    final <- do.call (c, result)
    
  }, .progress=.progress, .options = furrr::furrr_options(seed=TRUE))
  
  future::plan(saved_plan)
  osm <- do.call(c, osm)
  if(trim)
    osm <- osm |>
    trim_osmdata(bbox)
  time <- tictoc::toc(log = TRUE, quiet = TRUE)
  dtime <- (time$toc - time$tic)
  cli::cli_alert_success(
    "OSM sf téléchargé: {dtime%/%60}m {round(dtime-60*dtime%/%60)}s {signif(lobstr::obj_size(osm)/1024/1024, 3)} Mb")
  return(osm$osm_lines)
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
#' @param contract applique la fonction de contraction de graphe (défaut FALSE) déconseillé si turn_penalty est employé
#' @param deduplicate applique la fonction de déduplication de graphe (défaut TRUE)
#'
#' @export
routing_setup_dodgr <- function(path,
                                date="17-12-2019 8:00:00",
                                mode="CAR",
                                turn_penalty = FALSE,
                                distances = FALSE,
                                wt_profile_file = NULL,
                                n_threads= 4L,
                                overwrite = FALSE,
                                contract = FALSE,
                                deduplicate = TRUE)
{
  env <- parent.frame()
  path <- glue::glue(path, .envir = env)
  rlang::check_installed("dodgr", reason="requis pour le routage")
  assertthat::assert_that(
    mode%in%c("CAR", "BICYCLE", "WALK", "bicycle", "foot", "goods",
              "hgv", "horse", "moped",
              "motorcar", "motorcycle", "psv",
              "wheelchair"),
    msg = "incorrect transport mode")
  mode <- dplyr::case_when(mode=="CAR"~"motorcar",
                    mode=="BICYCLE"~"bicycle",
                    mode=="WALK"~"foot")
  RcppParallel::setThreadOptions(numThreads = as.integer(n_threads))
  # dans le path on s'attend à 1 fichier sfosm
  loff <- list.files(path=path, pattern = "*.scosm")
  if(length(loff)>1)
    cli::cli_alert_warning(
      "Attention, il y a plusieurs candidats de réseaux dans le dossier {.path {path}}" |> glue::glue())
  graph_name <- stringr::str_c(
    stringr::str_remove(loff[[1]], "\\.[:alpha:]*$"), ".", mode, ".dodgrnet")
  graph_name <- glue::glue("{path}/{graph_name}")
  dodgr_dir <- stringr::str_c(path, '/dodgr_files/')
  
  if(file.exists(graph_name)&!overwrite) {
    
    graph <- load_street_network(graph_name)
    # dodgr_tmp <- list.files(
    #   dodgr_dir,
    #   pattern = "^dodgr",
    #   full.names = TRUE)
    # 
    # file.copy(dodgr_tmp, tempdir())
    
    message("dodgr network en cache")
    
  } else {
    
    dodgr_tmp <- list.files(
      tempdir(),
      pattern = "^dodgr",
      full.names=TRUE)
    file.remove(dodgr_tmp)
    
    osm_network <- qs::qread(str_c(path, "/", loff[[1]]), nthreads=4)
    
    dodgr::dodgr_cache_off()
    
    cli::cli_alert_info("Création du streetnet")
    graph <- dodgr::weight_streetnet(
      osm_network,
      wt_profile = mode,
      wt_profile_file = wt_profile_file,
      turn_penalty = turn_penalty)
    if(contract) {
      cli::cli_alert_info("Contraction")
      graph <- dodgr::dodgr_contract_graph(graph)
    }
    if(deduplicate) {
      cli::cli_alert_info("Déduplication")
      graph <- dodgr::dodgr_deduplicate_graph(graph)
    }
    save_street_network(graph, filename = graph_name)
    # dodgr a besoin des fichiers créés à cette étape
    # dodgr_tmp <- list.files(tempdir(),
    #                         pattern = "^dodgr",
    #                         full.names = TRUE)
    # dir.create(dodgr_dir)
    # file.copy(from  = dodgr_tmp, to = dodgr_dir, overwrite = TRUE)
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
    departure_datetime = as.POSIXct(date,
                                    format = "%d-%m-%Y %H:%M:%S",
                                    tz=Sys.timezone()),
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
      rout$graph <- load_street_network(routing$graph_name)
      # dodgr_dir <- stringr::str_c(rout$path, '/dodgr_files/')
      # dodgr_tmp <- list.files(
      #   dodgr_dir,
      #   pattern = "^dodgr",
      #   full.names = TRUE)
      # file.copy(dodgr_tmp, tempdir())
      if(!is.null(routing$elevation))
        rout$elevation_data <- terra::rast(str_c(routing$path, "/", routing$elevation))
      return(rout)
    })
}

dodgr_ttm <- function(o, d, tmax, routing)
{
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  m_o <- as.matrix(o[, .(lon, lat)])
  m_d <- as.matrix(d[, .(lon, lat)])
  temps <- dodgr::dodgr_times(
    graph = routing$graph,
    from = m_o,
    to = m_d,
    shortest=FALSE)
  o_names <- dimnames(temps)
  names(o_names[[1]]) <- o$id
  names(o_names[[2]]) <- d$id
  dimnames(temps) <- list(o$id, d$id)
  temps <- data.table(temps, keep.rownames = TRUE)
  temps[, fromId:=rn |> as.integer()] [, rn:=NULL]
  temps <- melt(temps,
                id.vars="fromId",
                variable.name="toId",
                value.name = "travel_time",
                variable.factor = FALSE)
  temps <- temps[travel_time<=tmax*60,]
  temps[, `:=`(fromIdalt = o_names[[1]][as.character(fromId)],
               toIdalt = o_names[[2]][as.character(toId)])]
  if(routing$distances) {
    dist <- dodgr::dodgr_distances(
      graph = routing$graph,
      from = m_o,
      to = m_d,
      shortest=FALSE,
      parallel = TRUE)
    dimnames(dist) <- list(o$id, d$id)
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

load_street_network <- function(filename) {
  dodgr_dir <- stringr::str_c(dirname(filename), '/dodgr_files/')
  dodgr_tmp <- list.files(
    dodgr_dir,
    pattern = "^dodgr",
    full.names = TRUE)
  file.copy(dodgr_tmp, tempdir())
  qs::qread(filename, nthreads = 4)
}

save_street_network <- function(graph, filename) {
  qs::qsave(graph, file = filename, nthreads = 4, preset= "fast")
  # dodgr a besoin des fichiers créés à cette étape
  dodgr_tmp <- list.files(tempdir(),
                           pattern = "^dodgr",
                           full.names = TRUE)
  dodgr_dir <- stringr::str_c(dirname(filename), "/dodgr_files")
  dir.create(dodgr_dir)
  file.copy(from  = dodgr_tmp, to = dodgr_dir, overwrite = TRUE)
}
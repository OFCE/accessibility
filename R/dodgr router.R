#' Télécharge d'OSM pour dodgr en format silicate
#'
#' le format silicate permet à dodgr de pondérer les tournants,
#' les arrêts aux feux rouge ainsi que les restrictions de circulation
#'
#' @param zone la zone à télécharger, au format sf
#' @param workers le nombre de workers
#' @param elevation télécharge les altitudes pour alimenter les dénivelés
#' @param .progress affiche un indicateur de progression
#'
#' @return un osmdata_sc
#' @export
#'
#'
#'
download_osmsc <- function(zone, elevation=FALSE, workers = 1, .progress = TRUE) {
  
  rlang::check_installed(
    "osmdata", 
    version="0.2.5.005", 
    reason = "pour utiliser download_sc, il faut au moins la version 0.2.5.005
    sur github ropensci/osmdata")
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
  
  bbox <- sf::st_bbox(zone |> sf::st_transform(4326)) |>
    matrix(nrow = 2, dimnames = list(list("x","y"), list("min", "max")))
  logger::log_success("bbox {bbox}")
  
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
  
  if(elevation) {
    elevation <- elevatr::get_elev_raster(
      locations = zone |> sf::st_transform(4326), src = "aws", 
      z=13, neg_to_na = TRUE)
    progressr::handlers("cli")
    unlink("/tmp/elevation.tif")
    raster::writeRaster(elevation, "/tmp/elevation.tif")
    osm <- osm |> 
      osmdata::osm_elevation("/tmp/elevation.tif")
  }
  
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
#' Dans l'état actuel di ne fonctionne pas, de plus le calcul est fait dans dodgr avec un seul thread
#' 
#'
#' @param path string, chemin d'accès pour sauvegarder le routeur (par défaut il est mis en /tmp)
#' @param osm chemin vers le fichier osm au format silicate
#' @param date string, date Date où seront simulées les routes (non utilisé)
#' @param mode string, mode de transport, par défaut "CAR" (possible (BICYCLE, WALK,...))
#' @param turn_penalty booléen, applique une pénalité pour les turns
#' @param distances booléen, calcule les distances en prime
#' @param denivele calcule le d+ sur le trajet
#' @param wt_profile_file string, chemin vers le fichier des profils (écrit avec \code{dodgr::write_dodgr_wt_profile})
#' @param overwrite booléen, Regénére le reseau même si il est présent
#' @param n_threads entier, nombre de threads
#' @param overwrite reconstruit le réseau dodgr à partir de la source OSM
#' @param contract applique la fonction de contraction de graphe (défaut FALSE) déconseillé si turn_penalty est employé
#' @param deduplicate applique la fonction de déduplication de graphe (défaut TRUE)
#' @param di utilise les itinériaires détaillés, mais ne fonctionne pas pour le moment
#'
#' @export
routing_setup_dodgr <- function(
    path="/tmp/",
    osm,
    date="17-12-2019 8:00:00",
    mode="CAR",
    turn_penalty = FALSE,
    distances = FALSE,
    denivele = FALSE,
    wt_profile_file = NULL,
    n_threads= 4L,
    overwrite = FALSE,
    contract = FALSE,
    deduplicate = TRUE,
    nofuture = TRUE)
{
  env <- parent.frame()
  path <- glue::glue(path, .envir = env)
  
  rlang::check_installed(
    "dodgr",
    reason="requis pour le routage")
  assertthat::assert_that(
    mode%in%c("CAR", "BICYCLE", "WALK", "bicycle", "foot", "goods",
              "hgv", "horse", "moped",
              "motorcar", "motorcycle", "psv",
              "wheelchair"),
    msg = "incorrect transport mode")
  type <- "dodgr"
  mode <- dplyr::case_when(mode=="CAR"~"motorcar",
                           mode=="BICYCLE"~"bicycle",
                           mode=="WALK"~"foot")
  
  RcppParallel::setThreadOptions(
    numThreads = as.integer(n_threads))
  
  graph_name <- stringr::str_c(
    mode,
    ifelse(turn_penalty, "_tp",""),
    ifelse(denivele, "_deniv",""),
    ".dodgrnet")
  
  graph_name <- glue::glue("{path}/{graph_name}")
  dodgr_dir <- stringr::str_c(path, '/dodgr_files/')
  
  if(file.exists(graph_name)&!overwrite) {
    if(nofuture) {
      pg <- load_streetnet(graph_name)
      }
    else {
      pg <- NULL
    }
    message("dodgr network en cache")
    
  } else {
    
    dodgr_tmp <- list.files(
      tempdir(),
      pattern = "^dodgr",
      full.names=TRUE)
    file.remove(dodgr_tmp)
    
    osm_silicate <- qs::qread(osm, nthreads=8L)
    
    dodgr::dodgr_cache_on()
    
    cli::cli_alert_info("Création du streetnet")
    graph <- dodgr::weight_streetnet(
      osm_silicate,
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
    cli::cli_alert_info("Preprocess")
    pg <- dodgr::process_graph(graph)
    out <- save_streetnet(pg, filename = graph_name)
  }
  mtnt <- lubridate::now()
  if("dz"%in% names(pg$graph_compound)) {
    pg$graph_compound$dzplus <- 
      pg$graph_compound$dz * pg$graph_compound$d * (pg$graph_compound$dz >0)
    pg$graph_compound$dzplus[is.na(pg$graph_compound$dzplus)] <- 0
  }
  if(!nofuture) {
    pg <- NULL
  }
  list(
    type = type,
    path = path,
    pg = pg,
    distances = distances,
    denivele = denivele,
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
      rout$pg <- NULL
      return(rout)
    },
    core_init = function(routing){
      RcppParallel::setThreadOptions(numThreads = routing$n_threads)
      rout <- routing
      rout$pg <- load_streetnet(routing$graph_name)
      if("dz"%in% names(rout$pg$graph)) {
        rout$pg$graph$dzplus <-
          rout$pg$graph$dz * rout$pg$graph$d * (rout$pg$graph$dz >0)
        rout$pg$graph_compound$dzplus <- 
          rout$pg$graph_compound$dz * rout$pg$graph_compound$d * (rout$pg$graph_compound$dz >0)
        rout$pg$graph_compound$dzplus[is.na(rout$pg$graph_compound$dzplus)] <- 0
      }
      logger::log_info("router {graph_name} chargé")
      return(rout)
    })
}

#' ttm sur des paires
#'
#' @param od tibble origine destination
#' @param routing routing (voir dodgr_routing_setup)
#' @param chunk nombre de paires demandées d'un coup 
#'
#' @return a tibble
#' @export

dodgr_pairs <- function(od, routing, chunk = Inf) {
  logger::log_info("dodgr_pairs called, {nrow(od)} paires")
  RcppParallel::setThreadOptions(numThreads = routing$n_threads)
  if(is.null(routing$pg))
    routing <- routing$core_init(routing)
  lpg <- routing$pg
  lpg$graph$d <- routing$pg$graph$time
  lpg$graph_compound$d <- routing$pg$graph_compound$time
  if(is.finite(chunk)) {
    odl <- od |>
      mutate(gr = ((1:n())-1) %/% chunk) |>
      group_by(gr) |> 
      group_split()
  } else {
    odl <- list(od)
  }
  ttm <- map_dfr(odl, ~{
    tictoc::tic()
    m_o <- as.matrix(.x |> select(o_lon, o_lat), ncol=2)
    m_d <- as.matrix(.x |> select(d_lon, d_lat), ncol=2)
    
    temps <- dodgr::dodgr_dists_pre(
      proc_g = lpg,
      from = m_o,
      to = m_d,
      shortest = FALSE,
      pairwise = TRUE)
    
    lpg$graph$d <- routing$pg$graph$d
    lpg$graph_compound$d <- routing$pg$graph_compound$d
    dist <- dodgr::dodgr_dists_pre(
      proc_g = lpg,
      from = m_o,
      to = m_d,
      shortest = FALSE,
      pairwise = TRUE)
    
    lpg$graph$d <- routing$pg$graph$dzplus
    lpg$graph_compound$d <- routing$pg$graph_compound$dzplus
    dzplus <- dodgr::dodgr_dists_pre(
      proc_g = lpg,
      from = m_o,
      to = m_d,
      shortest = FALSE,
      pairwise = TRUE)
    
    time <- tictoc::toc(quiet=TRUE)
    dtime <- (time$toc - time$tic)
    speed_log <- stringr::str_c(
      ofce::f2si2(nrow(.x)),
      "@",ofce::f2si2(nrow(.x)/dtime),
      "p/s")
    logger::log_info(speed_log)
    
    bind_cols(.x, tibble(d = dist, travel_time = temps, dzplus = dzplus))
  })
  
  
  
  ttm
}

dodgr_ttm <- function(o, d, tmax, routing, dist_only = FALSE)
{
  logger::log_info("dodgr_ttm called, {dist_only}, {nrow(o)}x{nrow(d)}")
  lpg <- routing$pg
  lpg$graph$d <- routing$pg$graph$time
  lpg$graph_compound$d <- routing$pg$graph_compound$time
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  m_o <- as.matrix(o[, .(lon, lat)])
  m_d <- as.matrix(d[, .(lon, lat)])
  temps <- dodgr::dodgr_dists_pre(
    proc_g = lpg,
    from = m_o,
    to = m_d,
    shortest = FALSE)
  o_names <- dimnames(temps)
  names(o_names[[1]]) <- o$id
  names(o_names[[2]]) <- d$id
  dimnames(temps) <- list(o$id, d$id)
  temps <- data.table(temps, keep.rownames = TRUE)
  temps[, fromId := rn |> as.integer()] [, rn:=NULL]
  temps <- melt(temps,
                id.vars="fromId",
                variable.name="toId",
                value.name = "travel_time",
                variable.factor = FALSE)
  temps <- temps[, travel_time := as.integer(travel_time/60)]
  temps <- temps[travel_time<=tmax,]
  temps[, `:=`(fromIdalt = o_names[[1]][as.character(fromId)],
               toIdalt = o_names[[2]][as.character(toId)])]
  if(!dist_only) {
    if(routing$distances) {
      lpg$graph$d <- routing$pg$graph$d
      lpg$graph_compound$d <- routing$pg$graph_compound$d
      dist <- dodgr::dodgr_dists_pre(
        proc_g = lpg,
        from = m_o,
        to = m_d,
        shortest = FALSE)
      dimnames(dist) <- list(o$id, d$id)
      dist <- data.table(dist, keep.rownames = TRUE)
      dist[, fromId:=rn |> as.integer()] [, rn:=NULL]
      dist <- melt(dist,
                   id.vars="fromId", 
                   variable.name="toId", 
                   value.name = "distance", 
                   variable.factor = FALSE)
      temps <- merge(temps, 
                     dist,
                     by=c("fromId", "toId"),
                     all.x=TRUE, 
                     all.y=FALSE)
      temps[, distance:= as.integer(distance)]
    }
    if(routing$denivele) {
      lpg$graph$d <- routing$pg$graph$dzplus
      lpg$graph_compound$d <- routing$pg$graph_compound$dzplus
      dzplus <- dodgr::dodgr_dists_pre(
        proc_g = lpg,
        from = m_o,
        to = m_d,
        shortest = FALSE)
      dimnames(dzplus) <- list(o$id, d$id)
      dzplus <- data.table(dzplus, keep.rownames = TRUE)
      dzplus[, fromId:=rn |> as.integer()] [, rn:=NULL]
      dzplus <- melt(dzplus,
                     id.vars="fromId", 
                     variable.name="toId", 
                     value.name = "dzplus", 
                     variable.factor = FALSE)
      temps <- merge(temps, 
                     dzplus,
                     by=c("fromId", "toId"),
                     all.x=TRUE, 
                     all.y=FALSE)
    }
  }
  erreur <- NULL
  
  if (nrow(temps)>0){
    temps[, `:=`(fromId=as.integer(fromId), toId=as.integer(toId))]
    setorder(temps, fromId, toId)
  }
  else
  {
    erreur <- "dodgr::travel_time_matrix empty"
    temps <- data.table(fromId=numeric(), toId=numeric(), travel_time=numeric(), distance=numeric())
    logger::log_warn(erreur)
  }
  return(list(result=temps, error=erreur))
}

dodgr_path <- function(o, d, tmax, routing, dist_only = FALSE) {
  logger::log_info("dodgr_path called, {dist_only}, {length(o)}x{length(d)}")
  if(dist_only)
    return(dodgr_ttm(o,d,tmax, routing, dist_only))
  o <- o[, .(id=as.character(id),lon,lat)]
  d <- d[, .(id=as.character(id),lon,lat)]
  
  o_g <- dodgr::match_points_to_verts(
    routing$vertices,
    o[, .(lon, lat)], connected = TRUE)
  o_g <- routing$vertices |> dplyr::slice(o_g) |> dplyr::pull(id)
  # names(o_g) <- o$id
  
  d_g <- dodgr::match_points_to_verts(
    routing$vertices,
    d[, .(lon, lat)], connected = TRUE)
  d_g <- routing$vertices |> dplyr::slice(d_g) |> dplyr::pull(id)
  # names(d_g) <- d$id
  logger::log_info(" aggrégation chemins, {length(o_g)}x{length(o_d)}")
  chemins <- dodgr::dodgr_paths(
    graph = routing$graph,
    from = o_g,
    to = d_g)
  
  o_id <- purrr::set_names(o$id, o_g)
  d_id <- purrr::set_names(d$id, d_g)
  
  trips <- agr_chemins(chemins, routing, o_id, d_id)
  
  erreur <- NULL
  
  if (nrow(trips)==0) {
    erreur <- "dodgr::travel_time_matrix empty"
    trips <- data.table(fromId=numeric(), toId=numeric(), travel_time=numeric(), distance=numeric())
    logger::log_warn(erreur)
  }
  return(list(result=trips[travel_time<=tmax, ], error=erreur))
}

agr_chemins <- function(chemins, routing, o, d) {
  res <- data.table()
  for(grp in names(chemins)) {
    for(trip in names(chemins[[grp]])) {
      nn <- stringr::str_split(trip, "-")[[1]]
      ff <- chemins[[grp]][[trip]]
      oo <- o[nn[[1]]]
      dd <- d[nn[[2]]]
      if(length(ff)>1) {
        tt <- tail(ff,-1)
        ff <- head(ff, -1)
        vert <- data.table(from = ff, to = tt)
        setkey(vert, from, to)
        dt <- routing$graph.dt[vert]
        dt <- dt[, .(
          fromId = as.integer(oo),
          toId = as.integer(dd),
          fromIdalt = nn[[1]],
          toIdalt = nn[[2]],
          distance = sum(d),
          travel_time = sum(time)/60,
          dz = sum(dz),
          dz_plus = sum(dz_plus))]
      } else {
        dt <- data.table(
          fromId = as.integer(oo),
          toId = as.integer(dd),
          fromIdalt = nn[[1]],
          toIdalt = nn[[2]],
          distance = NA,
          travel_time = NA,
          dz = NA,
          dz_plus = NA)
      }
      res <- rbind(res, dt)
    }
  }
  setorder(res, fromId, toId)
  return(res)
}

load_streetnet <- function(filename) {
  dodgr_dir <- stringr::str_c(dirname(filename), '/dodgr_files/')
  qs::qread(filename, nthreads = 8L)
}

save_streetnet <- function(graph, filename) {
  qs::qsave(graph, file = filename, nthreads = 8L, preset= "fast")
}

dgr_save_streetnet <- function (net, filename = NULL) {
  
  if (is.null (filename)) {
    stop ("'filename' must be specified.")
  }
  if (!is.character (filename)) {
    stop ("'filename' must be specified as character value.")
  }
  if (length (filename) != 1L) {
    stop ("'filename' must be specified as single character value.")
  }
  
  if (tools::file_ext (filename) == "") {
    filename <- paste0 (filename, ".Rds")
  }
  
  # This function is essentially cache_graph in reverse
  hash <- attr (net, "hash")
  td <- fs::path_temp ()
  
  fname_v <- fs::path (td, paste0 ("dodgr_verts_", hash, ".Rds"))
  if (fs::file_exists (fname_v)) {
    v <- readRDS (fname_v)
  } else {
    v <- dodgr::dodgr_vertices (net)
  }
  
  # The hash for the contracted graph is generated from the edge IDs of
  # the full graph plus default NULL vertices:
  gr_cols <- dodgr:::dodgr_graph_cols (net)
  edge_col <- gr_cols$edge_id
  hashc <- dodgr:::get_hash (net, contracted = TRUE, verts = NULL, force = TRUE)
  
  fname_c <- fs::path (td, paste0 ("dodgr_graphc_", hashc, ".Rds"))
  if (fs::file_exists (fname_c)) {
    graphc <- readRDS (fname_c)
  } else {
    graphc <- dodgr::dodgr_contract_graph (net)
  }
  
  hashe <- attr (graphc, "hashe")
  fname_vc <- fs::path (td, paste0 ("dodgr_verts_", hashe, ".Rds"))
  if (fs::file_exists (fname_vc)) {
    verts_c <- readRDS (fname_vc)
  } else {
    verts_c <- dodgr::dodgr_vertices (graphc)
  }
  
  fname_e <- fs::path (td, paste0 ("dodgr_edge_map_", hashc, ".Rds"))
  if (!fs::file_exists (fname_e)) { # should always be
    stop ("edge_map was not cached; unable to save network.")
  }
  
  edge_map <- readRDS (fname_e)
  
  fname_j <- fs::path (td, paste0 ("dodgr_junctions_", hashc, ".Rds"))
  if (!fs::file_exists (fname_j)) { # should always be
    stop ("junction list was not cached; unable to save network.")
  }
  junctions <- readRDS (fname_j)
  
  out <- list (
    graph = net,
    verts = v,
    graph_c = graphc,
    verts_c = verts_c,
    edge_map = edge_map,
    junctions = junctions
  )
  
  qs::qsave(out, filename, nthreads = 4L, preset = "fast")
  invisible(out)
}

#' Load a street network previously saved with \link{dodgr_save_streetnet}.
#'
#' This always returns the full, non-contracted graph. The contracted graph can
#' be generated by passing the result to \link{dodgr_contract_graph}.
#' @param filename Name (with optional full path) of file to be loaded.
#'
#' @examples
#' net <- weight_streetnet (hampi)
#' f <- file.path (tempdir (), "streetnet.Rds")
#' dodgr_save_streetnet (net, f)
#' clear_dodgr_cache () # rm cached objects from tempdir
#' # at some later time, or in a new R session:
#' net <- dodgr_load_streetnet (f)
#' @family cache
#' @export
dgr_load_streetnet <- function (filename) {
  
  if (!fs::file_exists (filename)) {
    stop ("filename [", filename, "] not found.")
  }
  
  td <- fs::path_temp ()
  x <- qs::qread (filename, nthreads = 8L)
  
  hash <- attr (x$graph, "hash")
  hashc <- attr (x$graph_c, "hashc") # hash for contracted graph
  hashe <- attr (x$graph_c, "hashe") # hash for edge map
  
  fname <- fs::path (td, paste0 ("dodgr_graph_", hash, ".Rds"))
  if (!fs::file_exists (fname)) {
    saveRDS (x$graph, fname)
  }
  
  fname_v <- fs::path (td, paste0 ("dodgr_verts_", hash, ".Rds"))
  if (!fs::file_exists (fname_v)) {
    saveRDS (x$verts, fname_v)
  }
  
  fname_c <- fs::path (td, paste0 ("dodgr_graphc_", hashc, ".Rds"))
  if (!fs::file_exists (fname_c)) {
    saveRDS (x$graph_c, fname_c)
  }
  
  fname_vc <- fs::path (td, paste0 ("dodgr_verts_", hashe, ".Rds"))
  if (!fs::file_exists (fname_vc)) {
    saveRDS (x$verts_c, fname_vc)
  }
  
  fname_e <- fs::path (td, paste0 ("dodgr_edge_map_", hashc, ".Rds"))
  if (!fs::file_exists (fname_e)) {
    saveRDS (x$edge_map, fname_e)
  }
  
  fname_j <- fs::path (td, paste0 ("dodgr_junctions_", hashc, ".Rds"))
  if (!fs::file_exists (fname_j)) {
    saveRDS (x$junctions, fname_j)
  }
  
  return (list(graph = x$graph, verts_c = x$verts_c))
}

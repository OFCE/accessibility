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
      if(!is.null(routing$pg))
        return(routing)
      rout <- routing
      rout$pg <- load_streetnet(routing$graph_name)
      if("dz"%in% names(rout$pg$graph)) {
        rout$pg$graph_compound$dzplus <- 
          rout$pg$graph_compound$dz * rout$pg$graph_compound$d * (rout$pg$graph_compound$dz >0)
        rout$pg$graph_compound$dzplus[is.na(rout$pg$graph_compound$dzplus)] <- 0
      }
      logger::log_info("router {graph_name} chargé")
      return(rout)
    })
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
  temps[, fromId := rn ] [, rn:=NULL]
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
      dist[, fromId:=rn ] [, rn:=NULL]
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
      dzplus[, fromId:=rn] [, rn:=NULL]
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
    setorder(temps, fromId, toId)
  }
  else
  {
    erreur <- "dodgr::travel_time_matrix empty"
    temps <- data.table(fromId=character(), toId=character(),
                        travel_time=numeric(), distance=numeric())
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

# full distance par commune ------

#' Full distance
#' 
#' Alternative à iso acessibilité, plus simple
#' ne renvoie que les distances, temps et dzplus
#' exécute le calcul par COMMUNE
#' certains batchs sont assez petits mais relativement efficace au total
#' 
#' @param idINSes une table d'idINS, avec les colonnes idINS, from (lgl), to (lgl) COMMUNE, DCLT
#' @param com2com une table donnant les paires de relation
#' @param routeur un routeur dodgr, généré par routing_setup_dodgr
#' @param parallel fait le calcul en parallèle (défaut TRUE)
#' @param clusterize regroupe les communes pour minimiser le temps de caclul (défaut FALSE)
#' pour chaque paire COMMUNE, DCLT le routage est fait sur le produit des 
#' from et to indiqués pour cette paire
#'
#' @return un data.table, tous calculs faits
#' @export
#'
dgr_distances_by_com <- function(idINSes, com2com, routeur,
                                 path = "dgr",
                                 overwrite = TRUE,
                                 parallel = TRUE,
                                 clusterize = FALSE) {
  tictoc::tic()
  
  if(dir.exists(path)&!overwrite) {
    cli::cli_alert_warning("le dataset existe déjà")
  }
  
  if(dir.exists(path)&overwrite) {
    cli::cli_alert_warning("le dataset existe déjà, il est effacé")
    unlink(path, recursive = TRUE, force = TRUE)
  } 
  
  dir.create(
    path, 
    showWarnings = FALSE, 
    recursive = TRUE )
  
  dir.create(
    glue::glue("logs/"), 
    showWarnings = FALSE, 
    recursive = TRUE)
  
  timestamp <- lubridate::stamp(
    "15-01-20 10h08.05", 
    orders = "dmy HMS",
    quiet = TRUE) (lubridate::now(tzone = "Europe/Paris"))
  
  logfile <- glue::glue("logs/dgr_full_distance_com.{timestamp}.log")
  
  logger::log_appender(logger::appender_file(logfile))
  logger::log_success("Calcul accessibilite version 3")
  logger::log_success("       par commune, mode {routeur$mode}")
  
  fmt <- logger::layout_glue_generator(
    format = "{pid} [{format(time, \"%H:%M:%S\")}] {msg}") 
  logger::log_layout(fmt)
  
  pts_from <- idINSes |> 
    filter(from) |> 
    select(idINS, COMMUNE = com)
  pts_from <- bind_cols(
    pts_from,
    r3035::idINS2lonlat(pts_from$idINS))
  
  pts_to <- idINSes |> 
    filter(to) |> 
    select(idINS, DCLT = com)
  pts_to <- bind_cols(
    pts_to,
    r3035::idINS2lonlat(pts_to$idINS))
  
  cli::cli_alert_info("Indexage de from et to")
  indexes <- dodgr:::to_from_index_with_tp_pre(
    routeur$pg, 
    as.matrix(pts_from[, c("lon", "lat")]),
    as.matrix(pts_to[, c("lon", "lat")]));
  
  pts_from <- pts_from |> 
    mutate(index = indexes$from$index,
           id = indexes$from$id) |> 
    select(idINS, COMMUNE, index, id)
  
  pts_to <- pts_to |> 
    mutate(index = indexes$to$index,
           id = indexes$to$id) |> 
    select(idINS, DCLT, index, id)
  
  com2com <- com2com |> 
    left_join(idINSes |> filter(from) |> count(com, name = "nfrom"), 
              by=c("COMMUNE"="com")) |> 
    left_join(idINSes |> filter(to) |> count(com, name = "nto"),
              by=c("DCLT"="com")) |> 
    mutate(n = nfrom*nto)
  
  npaires <- sum(com2com$n, na.rm=TRUE)
  
  if(clusterize) {
    cls <- clusterize_com2com(com2com)
    com2com <- com2com |> 
      left_join(tibble(COMMUNE = names(cls$cluster),
                       cluster = cls$cluster), by = "COMMUNE")
    steps <- cls$meta$total_k
    cli::cli_alert_info("Clustérization")
    logger::log_info(
      "Clusterization {ofce::f2si2(npaires)} paires initial, {cls$meta$k} clusters")
    logger::log_info(
      "               {ofce::f2si2(cls$meta$total)} paires en agrégation complète")
    logger::log_info(
      "               {ofce::f2si2(cls$meta$total_k)} paires en cluster")
    logger::log_info(
      "               plus petit cluster {ofce::f2si2(cls$meta$min_k)}")
    logger::log_info(
      "               réduction de temps estimée à {100-round(cls$meta$ecart_temps*100)}%")
  } else {
    com2com <- com2com |> mutate(cluster = 1)
    steps <- npaires
    logger::log_info(
      "{ofce::f2si2(npaires)} paires pour {length(unique(com2com$COMMUNE))} communes d'origine")
    
  }
  cli::cli_alert_info("Chargement du routeur")
  rout <- routeur$future_routing(routeur)
  rout <- rout$core_init(rout)
  
  COMMUNES <- com2com |> 
    group_by(cluster) |> 
    reframe(COMMUNE = unique(COMMUNE)) |>
    group_by(cluster) |> 
    group_split()
  
  logger::log_appender(logger::appender_file(logfile))
  logger::log_layout(fmt)
  
  pb <- progressr::progressor(steps = steps)
  ttms <- imap(COMMUNES, ~{
    pb(0)
    tictoc::tic()
    
    from <- pts_from |> 
      semi_join(.x, by="COMMUNE")
    dclts <- com2com |> 
      semi_join(.x, by="COMMUNE") |> 
      distinct(DCLT) |> 
      pull()
    to <- pts_to |> 
      filter(DCLT %in% dclts)
    com_list <- str_c(.x$COMMUNE, collapse = ', ')
    ss <- nrow(from)*nrow(to)
    
    logger::log_info(
      "cluster {.y}/{length(COMMUNES)} {ofce::f2si2(ss)} paires")
    
    ttm <- dgr_onedistance(rout, from, to, parallel)
    
    pb(amount=ss)
    time <- tictoc::toc(quiet=TRUE)
    dtime <- (time$toc - time$tic)
    speed_log <- stringr::str_c("@",ofce::f2si2(ss/dtime), "p/s")
    
    logger::log_info(
      "        {speed_log}")
    
    ttm <- ttm |> 
      merge(from |> select(fromId= idINS, COMMUNE), by = "fromId") |> 
      merge(to |> select(toId= idINS, DCLT), by = "toId")
    
    walk(.x$COMMUNE, \(com) {
      dsdir <- glue::glue("{path}/'COMMUNE={com}'")
      pqtname <- glue::glue("{dsdir}/ttm.parquet")
      dir.create(path = dsdir,
                 recursive = TRUE,
                 showWarnings = FALSE)
      arrow::write_parquet(ttm[COMMUNE==com,],pqtname)})
  })
  
  time <- tictoc::toc(quiet=TRUE)
  dtime <- (time$toc - time$tic)
  
  logger::log_info("Calcul terminé, dataset {path} écrit - {ofce::f2si2(npaires)} en {f2si2(dtime/60)} mn soit {f2si2(npaires/dtime)} p/s")
  path
}

#' dgr_onedistance
#' 
#' Calcule un data frame contenant time_travel, distance et dzplus
#' 
#'
#' @param routeur le routeur préprocessé
#' @param from les points de départ (préindexés sur le routeur$graph)
#' @param to les points d'arrivée (préindexés sur le routeur$graph)
#'
#' @return un data.table
#' @export
#'

dgr_onedistance <- function(routeur, from, to, parallel = TRUE) {
  
  from_to_indexes <- list(
    from = list(index = from$index, id = from$id),
    to = list(index = to$index, id = to$id)
  )
  
  routeur$pg$graph_compound$d <- routeur$pg$d
  dist <- dodgr::dodgr_dists_pre(
    to_from_indices = from_to_indexes,
    proc_g = routeur$pg,
    shortest = FALSE, 
    parallel = parallel);
  
  dimnames(dist) <- list(from$idINS, to$idINS)
  dist <- data.table(dist, keep.rownames = TRUE)
  setnames(dist, "rn", "fromId")
  dist <- melt(dist,
               id.vars="fromId",
               variable.name="toId",
               value.name = "distance",
               variable.factor = FALSE)
  
  routeur$pg$graph_compound$d <- routeur$pg$graph_compound$time
  time <- dodgr::dodgr_dists_pre(
    to_from_indices = from_to_indexes,
    proc_g = routeur$pg,
    shortest = FALSE, 
    parallel = parallel)
  
  dimnames(time) <- list(from$idINS, to$idINS)
  time <- data.table(time, keep.rownames = TRUE)
  setnames(time, "rn", "fromId")
  time <- melt(time,
               id.vars="fromId",
               variable.name="toId",
               value.name = "travel_time",
               variable.factor = FALSE)
  time[, travel_time := as.integer(travel_time/60)]
  
  routeur$pg$graph_compound$d <- routeur$pg$graph_compound$dzplus
  dzplus <- dodgr::dodgr_dists_pre(
    to_from_indices = from_to_indexes,
    proc_g = routeur$pg,
    shortest = FALSE, 
    parallel = parallel)
  
  dimnames(dzplus) <- list(from$idINS, to$idINS)
  dzplus <- data.table(dzplus, keep.rownames = TRUE)
  setnames(dzplus, "rn", "fromId")
  dzplus <- melt(dzplus,
                 id.vars="fromId",
                 variable.name="toId",
                 value.name = "dzplus",
                 variable.factor = FALSE)
  
  dist |> 
    merge(time, by = c("fromId", "toId")) |> 
    merge(dzplus, by=c("fromId", "toId"))
}

# Full distance par paires ----------------

#' Full distance
#' 
#' Alternative à iso acessibilité, plus simple
#' ne renvoie que les distances, temps et dzplus
#' exécute le calcul par paires de COMMUNE/DCLT
#' certains batchs sont assez petits
#' 
#' @param idINSes une table d'idINS, avec les colonnes idINS, from (lgl), to (lgl) COMMUNE, DCLT
#' @param com2com une table donnant les paires de relation
#' @param routeur un routeur dodgr, généré par routing_setup_dodgr
#' @param path chemin d'enregistrement du dataset
#' @param chunk taille du paquet de découpage
#' @param overwrite écrase des précédents
#'
#' pour chaque paire COMMUNE, DCLT le routage est fait sur le produit des 
#' from et to indiqués pour cette paire
#'
#' @return rien, mais un dataset est enregistré dans path
#' @export
#'
dgr_distances_by_paires <- function(
    idINSes, com2com, routeur,
    path = "dgr",
    overwrite = TRUE, 
    chunk = 5000000L) {
  
  if(dir.exists(path)&!overwrite) {
    cli::cli_alert_warning("le dataset existe déjà")
  }
  
  if(dir.exists(path)&overwrite) {
    cli::cli_alert_warning("le dataset existe déjà, il est effacé")
    unlink(path, recursive = TRUE, force = TRUE)
  } 
  
  dir.create(
    path, 
    showWarnings = FALSE, 
    recursive = TRUE )
  
  dir.create(
    "logs/", 
    showWarnings = FALSE, 
    recursive = TRUE)
  
  timestamp <- lubridate::stamp(
    "15-01-20 10h08.05", 
    orders = "dmy HMS",
    quiet = TRUE) (lubridate::now(tzone = "Europe/Paris"))
  
  logfile <- glue::glue("logs/dgr_full_distance_paires.{timestamp}.log")
  
  logger::log_appender(logger::appender_file(logfile))
  logger::log_success("Calcul accessibilite avec dodgr version 3")
  logger::log_success("")
  
  fmt <- logger::layout_glue_generator(
    format = "[{pid}]-[{format(time, \"%H:%M:%S\")}] {msg}") 
  logger::log_layout(fmt)
  
  pts_from <- idINSes |> 
    filter(from) |> 
    select(idINS, com)
  pts_from <- bind_cols(
    pts_from,
    r3035::idINS2lonlat(pts_from$idINS))
  
  pts_to <- idINSes |> 
    filter(to) |> 
    select(idINS, com)
  pts_to <- bind_cols(
    pts_to,
    r3035::idINS2lonlat(pts_to$idINS))
  
  cli::cli_alert_info("Indexage de from et to")
  indexes <- dodgr:::to_from_index_with_tp_pre(
    routeur$pg, 
    as.matrix(pts_from[, c("lon", "lat")]),
    as.matrix(pts_to[, c("lon", "lat")]));
  
  pts_from <- pts_from |> 
    mutate(index = indexes$from$index,
           id = indexes$from$id)
  
  pts_to <- pts_to |> 
    mutate(index = indexes$to$index,
           id = indexes$to$id)
  
  com2com <- com2com |> 
    left_join(idINSes |> filter(from) |> count(com, name = "nfrom"), 
              by=c("COMMUNE"="com")) |> 
    left_join(idINSes |> filter(to) |> count(com, name = "nto"),
              by=c("DCLT"="com")) |> 
    mutate(n = nfrom*nto)
  npaires <- sum(com2com$n, na.rm=TRUE)
  cli::cli_alert_info("Génération des {ofce::f2si2(npaires)} paires")
  paires <- pmap_dfr(com2com, ~{
    cross_join(
      pts_from |> filter(com == .x) |> select(idINS, index, id),
      pts_to |> filter(com== .y)  |> select(idINS, index, id), suffix = c(".from", ".to"))
  }, .progress=TRUE)
  paires <- paires |> 
    mutate(gg = (1:n()-1) %/% chunk) |> 
    group_by(gg) |> 
    group_split()
  gc()
  
  pb <- progressr::progressor(steps = sum(com2com$n, na.rm=TRUE))
  
  ttms <- imap(paires, ~{
    pb(0)
    tictoc::tic()
    logger::log_appender(logger::appender_file(logfile))
    logger::log_layout(fmt)
    
    from <- .x |> 
      select(
        idINS = idINS.from, 
        index = index.from,
        id = id.from)
    to <- .x |> 
      select(
        idINS = idINS.to, 
        index = index.to,
        id = id.to)
    
    ttm <- dgr_onepaires(routeur, from, to)
    
    dir.create(path = glue::glue("{path}/'gg={.y}'"),
               recursive = TRUE,
               showWarnings = FALSE)
    arrow::write_parquet(
      ttm, 
      glue::glue("{path}/'gg={.y}'/ttm.parquet"))
    
    ss <- nrow(ttm)
    pb(amount=ss)
    time <- tictoc::toc(quiet=TRUE)
    dtime <- (time$toc - time$tic)
    speed_log <- stringr::str_c(
      ofce::f2si2(ss), "@",ofce::f2si2(ss/dtime), "p/s")
    logger::log_info(speed_log)
  })
}

#' dgr_onedistance
#' 
#' Calcule un data frame contenant time_travel, distance et dzplus
#' 
#'
#' @param routeur le routeur préprocessé
#' @param from les points de départ (préindexés sur le routeur$graph)
#' @param to les points d'arrivée (préindexés sur le routeur$graph)
#'
#' @return un data.table
#' @export
#'

dgr_onepaires <- function(routeur, from, to, parallel = TRUE) {
  
  from_to_indexes <- list(
    from = list(index = from$index, id = from$id),
    to = list(index = to$index, id = to$id)
  )
  
  routeur$pg$graph_compound$d <- routeur$pg$d
  dist <- dodgr::dodgr_dists_pre(
    to_from_indices = from_to_indexes,
    proc_g = routeur$pg,
    shortest = FALSE,
    pairwise = TRUE, 
    parallel = parallel)
  
  routeur$pg$graph_compound$d <- routeur$pg$graph_compound$time
  time <- dodgr::dodgr_dists_pre(
    to_from_indices = from_to_indexes,
    proc_g = routeur$pg,
    shortest = FALSE,
    pairwise = TRUE, 
    parallel = parallel)
  
  routeur$pg$graph_compound$d <- routeur$pg$graph_compound$dzplus
  dzplus <- dodgr::dodgr_dists_pre(
    to_from_indices = from_to_indexes,
    proc_g = routeur$pg,
    shortest = FALSE,
    pairwise = TRUE, 
    parallel = parallel)
  
  tibble(fromId = from$idINS, 
         toId = to$idINS, 
         distance = dist,
         time_travel = time,
         dzplus = dzplus)
}

# full distance tous les produits ------

#' Full distance
#' 
#' Alternative à iso acessibilité, plus simple
#' ne renvoie que les distances, temps et dzplus
#' exécute le calcul sur toutes les paires
#' de façon à avoir des gros batchs
#' 
#' @param idINSes une table d'idINS, avec les colonnes idINS, from (lgl), to (lgl) COMMUNE, DCLT
#' @param routeur un routeur dodgr, généré par routing_setup_dodgr
#'
#' pour chaque paire from et to 
#' 
#' @return un dataset
#' @export
#'
dgr_distances_full <- function(
    idINSes, routeur,
    path = "dgr",
    overwrite = TRUE,
    chunk  = 5000000L) {
  
  if(dir.exists(path)&!overwrite) {
    cli::cli_alert_warning("le dataset existe déjà")
  }
  
  if(dir.exists(path)&overwrite) {
    cli::cli_alert_warning("le dataset existe déjà, il est effacé")
    unlink(path, recursive = TRUE, force = TRUE)
  } 
  
  dir.create(
    path, 
    showWarnings = FALSE, 
    recursive = TRUE )
  
  dir.create(
    glue::glue("logs/"), 
    showWarnings = FALSE, 
    recursive = TRUE)
  
  timestamp <- lubridate::stamp(
    "15-01-20 10h08.05", 
    orders = "dmy HMS",
    quiet = TRUE) (lubridate::now(tzone = "Europe/Paris"))
  
  logfile <- glue::glue("logs/dgr_full_distance_com.{timestamp}.log")
  
  logger::log_appender(logger::appender_file(logfile))
  logger::log_success("Calcul accessibilite version 3")
  logger::log_success("")
  
  fmt <- logger::layout_glue_generator(
    format = "[{pid}]-[{format(time, \"%H:%M:%S\")}] {msg}") 
  logger::log_layout(fmt)
  
  pts_from <- idINSes |> 
    filter(from) |> 
    select(idINS, COMMUNE = com)
  pts_from <- bind_cols(
    pts_from,
    r3035::idINS2lonlat(pts_from$idINS))
  
  pts_to <- idINSes |> 
    filter(to) |> 
    select(idINS, DCLT = com)
  pts_to <- bind_cols(
    pts_to,
    r3035::idINS2lonlat(pts_to$idINS))
  
  cli::cli_alert_info("Indexage de from et to")
  indexes <- dodgr:::to_from_index_with_tp_pre(
    routeur$pg, 
    as.matrix(pts_from[, c("lon", "lat")]),
    as.matrix(pts_to[, c("lon", "lat")]));
  
  pts_from <- pts_from |> 
    mutate(index = indexes$from$index,
           id = indexes$from$id) |> 
    select(idINS, index, id, COMMUNE)
  
  pts_to <- pts_to |> 
    mutate(index = indexes$to$index,
           id = indexes$to$id) |> 
    select(idINS, index, id, DCLT)
  
  npaires <- nrow(pts_from) * nrow(pts_to)
  n_grps <- npaires %/% chunk
  
  pts_from_list <- pts_from |>
    mutate(
      gg = (1:n() - 1) %/% (max(1, nrow(pts_from) %/% n_grps))) |> 
    group_by(gg) |> 
    group_split()
  
  cli::cli_alert_info("Chargement du routeur")
  rout <- routeur$future_routing(routeur)
  rout <- rout$core_init(rout)
  
  pb <- progressr::progressor(steps = sum(npaires, na.rm=TRUE))
  
  ttms <- imap(pts_from_list, ~{
    pb(0)
    tictoc::tic()
    logger::log_appender(logger::appender_file(logfile))
    logger::log_layout(fmt)
    
    from <- .x
    to <- pts_to
    
    ttm <- dgr_onedistance(rout, from, to)
    
    ss <- nrow(ttm)
    pb(amount=ss)
    time <- tictoc::toc(quiet=TRUE)
    dtime <- (time$toc - time$tic)
    speed_log <- stringr::str_c(
      ofce::f2si2(ss), "@",ofce::f2si2(ss/dtime), "p/s")
    logger::log_info("groupe {.y}/{length(pts_from_list)} {speed_log}")
    ttm <- ttm |> 
      merge(.x |> select(fromId = idINS, COMMUNE), by = "fromId") |> 
      merge(to |> select(toId = idINS, DCLT), by = "toId") 
    ttm
  })
  arrow::write_dataset(
    dataset = ttms, 
    path = path,
    partitioning = "COMMUNE")
  return(arrow::open_dataset(glue::glue("{path}")))
}

# clusterization -------------

#' Groupe les communes de MOBPRO afin d'optimiser le temps de calcul
#' 
#' MOBPRO défini des paires de communes pour les différents modes de transport,
#'  éventuellement réduits si on se limite aux flux inférieur à 95%
#' 
#' Si on calcule les produits origine destination par commune, 
#' on fait des calculs sur des matrices o/d trop petites pour être calculées efficacement par {dodgr}
#' L'algorithme utilisé ici regroupe les paires de communes, afin d'accroitre la taille des paquets
#' sans pour autant accroître trop le nombre total de paires calculées 
#'
#' @param data un tibble de paires de commune origine/destination
#' @param seuil le nombre de paires à partir duquel il n'y a plus de gain de vitesse (10m par défaut)
#' @param method la méthode de clusterization ("complete" par défaut)
#'
#' L'algorithme calcule une distance qui dépend de l'augmentation du nombre de paires calculées 
#' Si les deux communes ont les mêmes destinations, la distance est 0
#' Si elles n'ont destination commune, cette distance vaut 1.
#' Sur la base de cette distance, on fait une classification hiérarchique.
#' On applique ensuite une estimation de la vitesse de calcul (log(v1/v2) = 0.5log(p1/p2))
#' qui sature au seuil (v(p>seuil)=v)
#' On choisit alors le nombre de clusters qui minimise le temps estimé de calcul
#' Le seuil dépend du nombre de processeurs affectés au calcul
#' 
#' @return une liste avec les cluster, et les détails de la minimisation
#' @export
#'

clusterize_com2com <- function(data, seuil = 20e+6L, method = "complete") {
  nDCLT <- data |> distinct(DCLT, nto) |> pull(nto, name = DCLT)
  nCOMMUNE <- data |> distinct(COMMUNE, nfrom) |> pull(nfrom, name = COMMUNE)
  d_pairique <- function( com1, com2) {
    if(com1==com2)
      return(0)
    com1 <- as.character(com1)
    com2 <- as.character(com2)
    n1 <- nCOMMUNE[[com1]]
    n2 <- nCOMMUNE[[com2]]
    com1dclt <- data |> filter(COMMUNE==com1) |> pull(DCLT)
    com2dclt <- data |> filter(COMMUNE==com2) |> pull(DCLT)
    nto1 <- sum(nDCLT[com1dclt])
    nto2 <- sum(nDCLT[com2dclt])
    np_sep <- n1 * nto1 + n2 * nto2
    np_union <- (n1+n2) * sum(nDCLT[unique(c(com1dclt, com2dclt))])
    np_cross <- n1 * nto2 + n2 * nto1
    return((np_union-np_sep)/ np_cross)
  }
  
  communes <- data |> distinct(COMMUNE) |> pull()
  n <- length(communes)
  
  mm <- matrix(0, nrow = n, ncol = n, dimnames = list(communes, communes))
  for(i in 2:n)
    for(j in 1:(i-1))  
      mm[i,j] <- mm[j,i] <- d_pairique(communes[[i]], communes[[j]]) 
  mm <- as.dist(mm)
  
  kk <- map_dfr(1:(n-1), ~data |> 
                  mutate(clust  = factoextra::hcut(
                    mm,
                    method = method, 
                    k=.x)$cluster[COMMUNE]) |> 
                  group_by(clust) |> 
                  summarize(ncom = sum(nCOMMUNE[unique(COMMUNE)]) ,
                            ndclt = sum(nDCLT[unique(DCLT)]),
                            n = ncom*ndclt) |> 
                  summarize( 
                    k = .x,
                    total_k = sum(n),
                    min_k = min(ncom*ndclt),
                    total = sum(nCOMMUNE)*sum(nDCLT),
                    ecart_temps = sum((n/total) * (pmin(total, seuil)/pmin(n, seuil))^0.5), 
                    ecart = total_k/total))
  k <- kk |> filter(ecart_temps==min(ecart_temps)) |> pull(k)
  list(cluster = factoextra::hcut(mm, method = method, k=k)$cluster,
       meta = as.list(kk |> filter(ecart_temps==min(ecart_temps))))
}
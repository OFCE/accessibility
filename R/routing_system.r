# génériques ---------------------------

#' calcul du temps de trajet selon le système de routing choisi.
#' /code{iso_ttm} calcule un temps de trajets
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#' @param dist_only Ne calcule que les distances
iso_ttm <- function(o, d, tmax, routing, dist_only = FALSE)
{
  r <- switch(routing$type,
              "r5" = r5_ttm(o, d, tmax, routing, dist_only),
              "r5_di" = r5_di(o, d, tmax, routing, dist_only),
              "otpv1" = otpv1_ttm(o, d, tmax, routing, dist_only),
              "osrm" = osrm_ttm(o, d, tmax, routing, dist_only),
              "dt"= dt_ttm(o, d, tmax, routing, dist_only),
              "euclidean" = euc_ttm(o, d, tmax, routing, dist_only),
              "dodgr" = dodgr_ttm(o, d, tmax, routing, dist_only),
              "dodgr_di" = dodgr_path(o, d, tmax, routing, dist_only))
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



# Routing systems ---------------------------------------------


#' calcul du temps de trajet avec une distance euclidienne
#'
#' @param o origine
#' @param d destination
#' @param tmax temps max pour le trajet
#' @param routing système de routing
#' @param dist_only Ne calcule que les distances
#'
#' @import data.table
#' @import sf
euc_ttm <- function(o, d, tmax, routing, dist_only = TRUE)
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
#' @param dist_only Ne calcule que les distances
#'
#' @import data.table
otpv1_ttm <- function(o, d, tmax, routing, dist_only = TRUE)
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
#' @param dist_only Ne calcule que les distances
#'
#' @import data.table
osrm_ttm <- function(o, d, tmax, routing, dist_only = TRUE)
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
#' @param dist_only Ne calcule que les distances
#'
#' @import data.table
dt_ttm <- function(o, d, tmax, routing, dist_only = TRUE)
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


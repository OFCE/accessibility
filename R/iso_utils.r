#' construit la grille pour projetter le résultat en crs 4326
#'
#' \code{iso_ouetquoi} projette sur 4326 les coordonnées et fabrique les grilles nécessaires en donnant en sortie les ou et quoi utilisés pour ttm
#'
#' @param ou positions sur lesquelles sont calculés les accessibilités. Si NULL, un raster est défini par défaut.
#' @param quoi sf de variables numériques de différentes aménités, qui vont être agrégées au sein d'isochrones.
#' @param res_ou resolution des positions de départ.
#' @param res_quoi resolution des amenités/opportunités.
#' @param opp_var liste des variables d'aménités et d'opportunités.
#' @param fun_quoi fonction d'aggrégation des aménités/opportunités à la résolution demandée.
#' @param resolution resolution finale. Par défaut, identique à la résolution des aménités/opportunités.
#' @param rf suréchantillonage dui raster ou, pour cacluler les recouvrements partiels, 5 par défaut
#'
#' @import data.table
#' @import sf
iso_ouetquoi_4326 <- function(ou, quoi, res_ou, res_quoi, opp_var, fun_quoi="mean", resolution=res_quoi, rf=5)
{
  # projection éventuelle sur une grille 3035 à la résolution res_quoi ou resolution
  if (!("sfc_POINT" %in% class(sf::st_geometry(quoi))) | is.finite(res_quoi))
  {
    if(!is.finite(res_quoi))
      res_quoi <- resolution

    if("sfc_POINT" %in% class(sf::st_geometry(quoi)))
    {
      rf <- 1
      qxy <- quoi  |>
        sf::st_transform(3035) |>
        sf::st_coordinates()
      qins <- idINS3035(qxy, resolution = res_quoi, resinstr = FALSE)
      qag <- quoi|>
        sf::st_drop_geometry()  |>
        as.data.frame() |>
        as.data.table()
      qag <- qag[, id:=qins] [, lapply(.SD, function(x) sum(x, na.rm=TRUE)), by=id, .SDcols=opp_var]
      qag[, geometry:=idINS2square(qag$id, resolution=res_quoi)]
      quoi <- sf::st_as_sf(qag)
    }

    quoi <- quoi |> sf::st_transform(3035)
    quoi_surf <- sf::st_area(quoi) |> as.numeric()
    gc()
    message("...rasterization")
    rr_3035 <-
      raster::brick(
        purrr::map(
          opp_var,
          ~{
            if(!is.na(sf::st_agr(quoi)[[.x]]) & sf::st_agr(quoi)[[.x]] == "constant") {
              fonction <- mean
              facteur <- 1
            }
            else {
              fonction <- sum
              facteur <- (resolution/rf)^2/quoi_surf
            }
            if(rf>1)
              rref <- raster::disaggregate(raster_ref(quoi, resolution), fact=rf)
            else
              rref <- raster_ref(quoi, resolution)
            un_raster <-
              fasterize::fasterize(
                quoi |> dplyr::mutate(field = get(.x) * facteur),
                rref,
                fun = "sum",
                background = 0,
                field = "field")
            if(rf>1)
              un_raster <- raster::aggregate(un_raster, fact = rf, fun = fonction)
            un_raster
          }))

    xy_3035 <- raster::coordinates(rr_3035)
    quoi_3035 <- data.table(rr_3035 |> as.data.frame(),
                            x=xy_3035[,1] |> round(),
                            y=xy_3035[,2] |> round())
    quoi_4326 <- quoi_3035 |> as.data.table()
    keep <- purrr::reduce(
      purrr::map(opp_var, ~{
        xx <- quoi_4326[[.x]]
        (!is.na(xx)) & (xx!=0)
        }),
      `|`)
    quoi_4326 <- quoi_4326[keep, ]
    xy_3035 <-quoi_4326[, .(x,y)] |> as.matrix()
    xy_4326 <- sf::sf_project(xy_3035, from =  sf::st_crs(3035), to =  sf::st_crs(4326))
    rm(rr_3035)
    gc()
  }
  else # !point ou finite(resquoi)
  {
    xy_3035 <-  sf::st_coordinates(quoi|>  sf::st_transform(3035))
    xy_4326 <-  sf::st_coordinates(quoi |> sf::st_transform(4326))
    quoi_4326 <- quoi |>
      dplyr::as_tibble() |>
      dplyr::select(dplyr::all_of(opp_var)) |>
      as.data.table()
  }
  quoi_4326[, `:=`(lon = xy_4326[, 1],
                   lat = xy_4326[, 2],
                   x= xy_3035[,1]|> round(),
                   y= xy_3035[,2]|> round())]
  quoi_4326[, id := .I]
  setkey(quoi_4326, id)

  if (is.null(ou))
  {
    # pas de ou, on le prend égal à quoi en forme
    ou_3035 <- raster_ref(quoi|>  sf::st_transform(3035), resolution = res_ou)
    ncells <- 1:(ou_3035@ncols * ou_3035@nrows)
    xy_3035 <-  raster::xyFromCell(ou_3035, ncells)
    xy_4326 <- sf::sf_project(xy_3035, from = sf::st_crs(3035), to = sf::st_crs(4326))
    ou_4326 <-
      data.table(
        lon = xy_4326[, 1],
        lat = xy_4326[, 2],
        x = xy_3035[, 1] |> round(),
        y = xy_3035[, 2] |> round()
      )
  }
  else {
    # un ou
    if (is.finite(res_ou) && !("sfc_POINT" %in% class(sf::st_geometry(ou))))
    {
      # pas de points mais une résolution, on crée la grille
      ou_3035 <- ou|> sf::st_transform(3035)
      rr_3035 <- fasterize::fasterize(ou_3035|>  sf::st_sf(), raster_ref(ou_3035, res_ou), fun = "any")
      xy_3035 <- raster::xyFromCell(rr_3035, which(raster::values(rr_3035) == 1))
      xy_4326 <- sf::sf_project(xy_3035, from = sf::st_crs(3035), to = sf::st_crs(4326))
      ou_4326 <- data.table(lon = xy_4326[, 1],
                            lat = xy_4326[, 2],
                            x = xy_3035[,1] |> round(),
                            y = xy_3035[,2] |> round())
    }
    else
    {
      # des points, une résolution ou pas, on garde les points
      if (!("sfc_POINT" %in% class(sf::st_geometry(ou)))) ou <- sf::st_centroid(ou)
      xy_3035 <- ou|> sf::st_transform(3035)|> sf::st_coordinates()
      xy_4326 <- sf::sf_project(xy_3035, from = sf::st_crs(3035), to = sf::st_crs(4326))
      ou_4326 <- data.table(lon = xy_4326[, 1],
                            lat = xy_4326[, 2],
                            x = xy_3035[, 1] |> round(),
                            y = xy_3035[, 2] |> round())
    }
  }

  ou_4326[, id := .I]
  setkey(ou_4326, id)

  list(ou_4326=ou_4326, quoi_4326=quoi_4326)
}

#' produit les macro-secteurs sur la grille pour compartimenter les calculs.
#' /code{iso_split_ou} produit les groupes de positions en vue de compartimenter les calculs.
#'
#' @param ou positions sur lesquelles sont calculés les accessibilités. Si NULL, un raster est défini par défaut.
#' @param quoi sf de variables numériques de différentes aménités, qui vont être agrégées au sein d'isochrones.
#' @param chunk pas sûr
#' @param routing information sur le système de routing.
#' @param tmax temps maximal pour les isochrones.
#'
#' @import data.table
#' @import sf

iso_split_ou <- function(ou, quoi, chunk=NULL, routing, tmax=60)
{
  n <- min(100, nrow(ou))
  mou <- ou[sample(.N, n), .(x,y)] |> as.matrix()
  mquoi <- quoi[, .(x,y)] |> as.matrix()
  nquoi <- median(matrixStats::rowSums2(rdist::cdist(mou, mquoi) <= vmaxmode(routing$mode)*tmax))

  size <- as.numeric(nrow(ou)) * as.numeric(nquoi)
  bbox <- matrix(c(xmax=max(ou$lon), xmin=min(ou$lon), ymax=max(ou$lat), ymin= min(ou$lat)), nrow=2)
  bbox <- sf::sf_project(pts=bbox, from=sf::st_crs(4326), to=sf::st_crs(3035))
  surf <- (bbox[1,1]-bbox[2,1])*(bbox[1,2]-bbox[2,2])
  n_t <- if(is.null(routing$n_threads)) 1 else routing$n_threads

  if(!is.null(routing$groupes))
  {
    ngr <- length(routing$groupes)
    out_ou <- merge(ou, routing$fromId[, .(x,y,gr)], by=c("x","y"))
    ou_gr <- unique(out_ou$gr)
    names(ou_gr) <- ou_gr
    resolution <- routing$resolution
    subsampling <- 0
  }
  else
  {
    ngr <- min(max(8, round(size/chunk)), round(size/1000)) # au moins 8 groupes, au plus des morceaux de 1k
    resolution <- 12.5*2^floor(max(0,log2(sqrt(surf/ngr)/12.5)))

    subsampling <- min(max(n_t,floor(resolution/(0.1*tmax*vmaxmode(routing$mode)))),8)

    idINS <- idINS3035(ou$x, ou$y, resolution, resinstr = FALSE)
    uidINS <- unique(idINS)

    out_ou <- ou
    out_ou[, `:=`(gr=idINS)]
    ou_gr <- rlang::set_names(as.character(unique(out_ou$gr)))
  }
  Nous <- out_ou[, .N, by=gr]
  Nous <- rlang::set_names(Nous$N, Nous$gr)
  logger::log_success("taille:{f2si2(size)} gr:{f2si2(ngr)} res_gr:{resolution}")
  list(ou=out_ou, ou_gr=ou_gr, resINS=resolution, subsampling=subsampling, Nous=Nous)
}

#' récupère les infos sur le routeur
#'
#' @param routing système de routing.
#' @param groupe ??
get_routing <- function(routing, groupe) {

  if(is.null(routing$groupes))
    return(routing)
  file <- routing$time_table[[groupe]]
  ext <- stringr::str_extract(file, "(?<=\\.)[:alnum:]*(?!\\.)")
  if (ext=="csv")
    routing$time_table <- data.table::fread(file)
  else
    routing$time_table <- qs::qread(file, nthreads=4)

  routing$groupes=groupe

  routing
}

#' définition des vitesses moyennes selon le mode de transport.
#'
#' @param mode mode de transport ("TRANSIT", "RAIL", "BUS", "CAR", "BIKE", "WALK")
vmaxmode <- function(mode)
{
  vitesse <- dplyr::case_when(
    "TRANSIT" %in% mode ~ 40/60*1000,
    "RAIL" %in% mode ~ 40/60*1000,
    "BUS" %in% mode ~ 15/60*1000,
    "CAR" %in% mode ~ 60/60*1000,
    "BIKE" %in% mode ~ 12/60*1000,
    "WALK" %in% mode ~ 5/60*1000,
    TRUE ~ 60/60*1000) # vitesse en metres par minute

  vitesse
}

#' sélectionne les macro-secteurs pour le calcul de trajets.
#'
#' @param s_ou ??
#' @param k ??
#' @param routing ??
select_ancres <- function(s_ou, k, routing)
{
   if(nrow(s_ou)<=1)
    return(list(les_ou=s_ou,
                les_ou_s=s_ou$id,
                error=NULL,
                result=data.table()))

  des_iou <- sample.int(nrow(s_ou), max(1,min(nrow(s_ou)%/%4, k)), replace=FALSE) |> sort()
  les_ou <- s_ou[des_iou]
  les_autres_ou <- s_ou[-des_iou]
  # on calcule les distances entre les points choisis (ancres) et les autres
  ttm_ou <- iso_ttm(o = les_ou, d = les_autres_ou, tmax=100, routing=routing)
  if(!is.null(ttm_ou$error))
  {
    logger::log_warn("erreur de routeur ttm_ou {ttm_ou$error}")
    ttm_ou$result <- data.table(fromId = numeric(), toId = numeric(),
                                travel_time=numeric(), distance = numeric(), legs = numeric())
  }
  ttm_ou$les_ou_s <- stringr::str_c(les_ou$id, collapse=",")
  ttm_ou$les_ou <- les_ou
  ttm_ou
}

#' renvoie les matrices de distances, vides si éloignés.
#'
#' @param ppou ??
#' @param s_ou ??
#' @param quoi opportunités
#' @param ttm_0 ??
#' @param les_ou points de départ
#' @param tmax ??
#' @param routing info de routing
#' @param grdeou ??
#'
#' @import data.table
ttm_on_closest <- function(ppou, s_ou, quoi, ttm_0, les_ou, tmax, routing, grdeou)
{
  ss_ou <- s_ou[!(id%in%les_ou$id)] [closest==ppou]
  null_result <- data.table(fromId=numeric(),
                            toId=numeric(),
                            travel_time=numeric(),
                            distance = numeric(),
                            legs = numeric(),
                            npea=numeric(),
                            npep=numeric())
  if(nrow(ss_ou)>0)
  {
    marge <- max(ss_ou[["tt"]])
    cibles_ok <- ttm_0[fromId==ppou&travel_time<=tmax+marge][["toId"]]
    if(length(cibles_ok)>0)
    {

      ttm <- iso_ttm(o = ss_ou, d = quoi[cibles_ok], tmax=tmax+1, routing=routing)
      if(is.null(ttm$error))
        if(nrow(ttm$result)>0)
          return(ttm$result[, npea:=length(cibles_ok)] [, npep:=length(unique(toId)), by=fromId])
      else # 0 row ttm
      {
        logger::log_warn("paquet:{grdeou} ppou:{ppou} ttm vide")
        logger::log_warn("la matrice des distances et des opportunites (ttm) est vide")
        return(null_result)
      }
      else # ttm$error
      {
        logger::log_warn("paquet:{grdeou} ppou:{ppou} ttm vide")
        logger::log_warn("erreur {ttm$error}")
        return(null_result)
      }
    }
    else # cibles_ok lenght nulle
    {
      logger::log_warn("paquet:{grdeou} ppou:{ppou} ttm vide")
      logger::log_warn("pas de cibles")
      return(null_result)
    }
  }
  else # ss_ou vide
  {
    logger::log_warn("paquet:{grdeou} ppou:{ppou} ttm vide")
    logger::log_warn("pas d'origines")
    return(null_result)
  }
}

#' utile pour calcul parallèle.
#'
#' @param pids identifiant
get_pid <- function(pids)
{
  cpid <- future:::session_uuid()[[1]]
  if(cpid %in% pids)
    stringr::str_c("[", which(pids==cpid),"]")
  else
    ""
}

#' teste si dans répertoire
#'
#' @param groupe groupe
#' @param dir répertoire
is.in.dir <- function(groupe, dir)
{
  lf <- list.files(dir)
  stringr::str_c(groupe, ".rda") %in% lf
}

#' calcul de l'accessibilité par compartiments
#'
#' @param groupe groupe
#' @param ou_4326 les positions.
#' @param quoi_4326 les aménités/opportunités.
#' @param routing le système de routing.
#' @param k facteur de subsampling
#' @param tmax temps maximal des isochrones.
#' @param opp_var liste des variables d'aménités/opportunités.
#' @param ttm_out switch selon lequel on performe l'aggrégation ou on garde la matrice des temps
#' @param pids liste des process en //
#' @param dir dossier des résultats intermédiaires.
#' @param t2d enregistre ou non sur disque les résultats
#'
#' @import data.table
access_on_groupe <- function(groupe, ou_4326, quoi_4326, routing, k, tmax, opp_var, ttm_out, pids, dir, t2d)
{
  spid <- get_pid(pids)

  s_ou <- ou_4326[gr==groupe, .(id, lon, lat, x, y)]
  logger::log_debug("aog:{groupe} {k} {nrow(s_ou)}")

  if(t2d && is.in.dir(groupe, dir))
  {
    logger::log_success("carreau:{groupe} dossier:{dir}")
    return(data.table(file = stringr::str_c(dir, "/", groupe, ".rda")))
  }

  if(is.null(routing$ancres))
  {
    tictoc::tic()
    routing_sans_elevation <- routing
    routing_sans_elevation$elevation <- NULL
    ttm_ou <- select_ancres(s_ou, k, routing_sans_elevation)
    les_ou_s <- ttm_ou$les_ou_s

    if(is.null(ttm_ou$error))
    {
      if(nrow(ttm_ou$result)>0)
      {
        closest <- ttm_ou$result[, .(closest=fromId[which.min(travel_time)],
                                     tt = min(travel_time)),
                                 by=toId][, id:=toId][, toId:=NULL]
        s_ou <- merge(s_ou, closest, by="id")
        delay <- max(s_ou[["tt"]], na.rm = TRUE)
      }
      else
      {
        delay <- 0
      }
      logger::log_debug("ttm_ou:{nrow(ttm_ou$result)}")

      # on filtre les cibles qui ne sont pas trop loin selon la distance euclidienne
      quoi_f <- minimax_euclid(from=ttm_ou$les_ou, to=quoi_4326, dist=vmaxmode(routing$mode)*(tmax+delay))

      logger::log_debug("quoi_f:{nrow(quoi_f)}")

      # distances entre les ancres et les cibles
      if(any(quoi_f))
        ttm_0 <- iso_ttm(o = ttm_ou$les_ou,
                         d = quoi_4326[quoi_f],
                         tmax=tmax+delay+3,
                         routing=routing)
      else
        ttm_0 <- list(error=NULL, result=data.table())

      if(!is.null(ttm_0$error))
      {
        logger::log_warn("carreau:{groupe} ou_id:{les_ou_s} erreur ttm_0 {ttm_0$error}")
        ttm_0 <- data.table()
      }
      else
        ttm_0 <- ttm_0$result

      if(nrow(ttm_0)>0)
      {
        pproches <- sort(unique(s_ou$closest))
        ttm_0[ , npea:=nrow(quoi_4326)] [, npep:=length(unique(toId)), by=fromId]
        logger::log_debug("toc {nrow(ttm_0)}")

        if(!is.null(pproches))
        {
          # boucle sur les ancres
          ttm <- rbindlist(
            purrr::map(pproches,
                       function(close)
                         ttm_on_closest(close, s_ou, quoi_4326, ttm_0, ttm_ou$les_ou, tmax, routing, groupe)
            ), use.names = TRUE, fill = TRUE)
          ttm <- rbind(ttm_0[travel_time<=tmax], ttm[travel_time<=tmax], use.names=TRUE, fill=TRUE)
        }
        else
          ttm <- ttm_0

        time <- tictoc::toc(quiet = TRUE)
        dtime <- (time$toc - time$tic)

        if(nrow(ttm)> 0)
        {
          paires_fromId <- ttm[, .(npep=npep[[1]], npea=npea[[1]]), by=fromId]
          npea <- sum(paires_fromId$npea)
          npep <- sum(paires_fromId$npep)

          speed_log <- stringr::str_c(
            length(pproches),
            " ancres ", f2si2(npea),
            "@",f2si2(npea/dtime),"p/s demandees, ",
            f2si2(npep),"@",f2si2(npep/dtime), "p/s retenues")

          if(!ttm_out)
          {
            ttm_d <- merge(ttm, quoi_4326, by.x="toId", by.y="id", all.x=TRUE)
            ttm_d <- ttm_d[, purrr::map(.SD,~sum(., na.rm=TRUE)),
                           by=c("fromId", "travel_time"),
                           .SDcols=(opp_var)]
            ttm_d <- merge(ttm_d, paires_fromId, by="fromId")
            ttm_d[, gr:=groupe]
          }
          else
          {
            ttm_d <- ttm
          }
          logger::log_success("carreau:{groupe} {speed_log}")
        }
        else
        {
          ttm_d <- NULL
          logger::log_warn("carreau:{groupe} ttm vide")
        }
      } # close nrow(ttm_0)>0
      else # nrow(ttm_0)==0
      {
        time <- tictoc::toc(quiet=TRUE)
        logger::log_warn("carreau:{groupe} ou_id:{les_ou_s} ttm_0 vide")
        logger::log_warn("la matrice des distances entre les ancres et les opportunites est vide")
        ttm_d <- NULL
      }
    }
    else  # nrow(ttm_ou)==0
    {
      logger::log_warn("carreau:{groupe} ou_id:{les_ou_s} ttm_ou vide")
      logger::log_warn("la matrice des distances interne au carreau est vide")
      ttm_d <- NULL}
  }
  else # ancres=FALSE pas besoin de finasser, on y va brutal puisque c'est déjà calculé
  {
    ttm <- iso_ttm(o=s_ou, d=quoi_4326, tmax=tmax+1, routing=routing)$result
    ttm_d1 <- merge(ttm, quoi_4326, by.x="toId", by.y="id", all.x=TRUE)
    ttm_d <- ttm_d1[, purrr::map(.SD,~sum(., na.rm=TRUE)),
                    by=c("fromId", "travel_time"),
                    .SDcols=(opp_var)]
    ttm_d2 <- ttm_d1[, .(npea=.N, npep=.N), by=fromId] [, gr:=groupe]
    ttm_d <- merge(ttm_d, ttm_d2, by="fromId")
  }

  if(t2d)
  {
    file <- stringr::str_c(dir,"/", groupe, ".rda")
    qs::qsave(ttm_d, file, preset="fast", nthreads = 4)
    ttm_d <- data.table(file=file)
  }
  ttm_d
}

#' définition de la distance euclidienne.
#' /code{minimax_euclid} renvoie la distance euclidienne
#'
#' @param from point de départ
#' @param to point d'arrivée
#' @param dist plafond de distance
#'
#' @import data.table
minimax_euclid <- function(from, to, dist)
{
  m_from <- from[, .(x,y)] |> as.matrix()
  m_to <- to[, .(x,y)] |> as.matrix()
  dfrom2to <- rdist::cdist(m_from, m_to)
  dmin2to <- matrixStats::colMins(dfrom2to)
  dmin2to<=dist
}

#' Repackage la matrice de distance
#'
#' A partir de l'output de \code{\link{iso_accessibilite}} en mode \code{ttm_out=TRUE}
#' calcule les idINS et fabrique un data.table associant les points de départ (from) à ceux de destination (to)
#'
#' @param ttm l'output de \code{\link{iso_accessibilite}}
#' @param resolution la résolution par défaut 200m
#'
#' @return un data.table avec les paires o-d et les temps en fonction du mode
#' @export
#'

ttm_idINS <- function(ttm, resolution=200) {
  require("data.table")
  from <- ttm$fromId[, .(id, fromidINS = idINS3035(x,y, resolution = resolution))]
  to <- ttm$toId[, .(id, toidINS = idINS3035(x,y, resolution = resolution))]
  tt <- ttm$time_table
  if(length(tt)>1)
    tt <- rbindlist(tt, use.names=TRUE, fill = TRUE)
  else
    if("list"%in%class(tt))
      tt <- tt[[1]]
  tt <- merge(tt, from, by.x="fromId", by.y="id")
  tt[, fromId:=NULL]
  tt <- merge(tt, to, by.x="toId", by.y="id")
  tt[, toId :=NULL]
  return(tt)
}

ksplit <- function(data, k) {
  if(is.null(nrow(data)))
    n <- length(data)
  else
    n <- nrow(data)
  split(data, ceiling(1:n/(n/k)))
}

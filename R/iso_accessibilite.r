#' Calcul de l'accessibilité
#'
#' \code{iso_accessibilite} calcule l'accessibilité à des aménités définies sur une carte (sous forme de sf) et
#' renvoie un raster à une certaine résolution. Le calcul des distances isochroniques est organisé par secteurs et
#' est majoré pour évacuer des calculs inutiles entre secteurs éloignés.
#' Les étapes du calcul peuvent être enregistrées pour ne pas repartir de zéro si le calcul plante en cours de route.
#'
#' @param quoi sf de variables numériques de différentes aménités, qui vont être agrégées au sein d'isochrones.
#' @param ou positions sur lesquelles sont calculées les accessibilités.
#'           Si NULL, un raster est défini par défaut en ayant la même empreinte que les oportunités, à la résolution resolution.
#' @param res_quoi rasterisation éventuelle des lieux à la résolution donnée. Par défaut, reste un sf.
#' @param resolution résolution retenue pour l'accessibilité. Par défaut, 200m si ou est NULL, Inf sinon.
#' @param var_quoi si rasterisation, définie la fonction d'agrégation. Non implémenté.
#' @param routing défini le moteur de routage parmi "r5", "otpv1", "osrm", "dt", "euclidean". Par défaut, r5.
#' @param tmax temps maximal pour les calculs d'isochrone. Par défaut, 10 mn.
#' @param pdt pas de temps pour le calcul des isochrones. Par defaut, incrément de 1mn.
#' @param chunk taille des paquets pour le compartimentage des calculs.
#' @param future calcul parallélisé ? Par défaut, FALSE.
#' @param out format de sortie ("data.table", "sf", "raster"). Par défaut, raster.
#' @param ttm_out conserver travel_time_matrix. Par défaut, FALSE.
#' @param logs dossier pour le fichier log, par defaut la variable localdata (non définie).
#' @param dir dossier des étapes du calcul (permet de récupérer ce qui a déjà calculé en cas de plantage).
#' @param table2disk enregistre les étapes du calcul. Par défaut, si dir est défini, TRUE.
#'
#' @return Par défaut, la sortie est une liste de bricks (ensemble de rasters). Chaque élément de la liste renvoie à une aménité et la brick donne tous les rasters selon le temps de déplacement.
#'
#' @import data.table
#' @import sf
#' @importFrom utils capture.output
#'
#' @export
#'

iso_accessibilite <- function(
  quoi,                            # sf avec des variables numériques qui vont être aggrégées
  ou = NULL,                         # positions sur lesquelles sont calculés les accessibilités (si NULL, sur une grille)
  res_quoi = Inf,                    # projection éventuelle des lieux sur une grille
  resolution = ifelse(is.null(ou), 200, Inf),
  var_quoi = "individuals",          # si projection fonction d'agrégation
  routing = "r5",                         # défini le moteur de routage
  tmax = 10L,                        # en minutes
  pdt = 1L,
  chunk = 50000000,                   # paquet envoyé
  future = FALSE,
  out = ifelse(is.finite(resolution), resolution, "raster"),
  ttm_out = FALSE,
  logs = localdata,
  dir = NULL,
  table2disk = if(is.null(dir)) FALSE else TRUE)    # ne recalcule pas les groupes déjà calculés, attention !
{
  start_time <- Sys.time()

  # 1. découpe les groupes d'origines
  # 2. par groupe
  #    1. prend un point au hasard
  #    2. calcule les distances entre ce point et les cibles
  #    3. calcule les distance entre le point et les autres du paquet
  #    4. sélectionne pour chaque auter point du paquet les cibles atteignables
  #    5. calcule les distances
  # 4. aggrege
  # 5. cumule
  # 6. rasterize

  dir.create(glue::glue("{logs}/logs"), showWarnings = FALSE, recursive = TRUE)
  timestamp <- lubridate::stamp("15-01-20 10h08.05", orders = "dmy HMS", quiet = TRUE) (lubridate::now())
  logfile <- glue::glue("{logs}/logs/iso_accessibilite.{routing$type}.{timestamp}.log")
  logger::log_appender(logger::appender_file(logfile))

  logger::log_success("Calcul accessibilite version 2")
  logger::log_success("{capture.output(show(routing))}")
  logger::log_success("")
  logger::log_success("tmax:{tmax}")
  logger::log_success("pdt:{pdt}")
  logger::log_success("chunk:{f2si2(chunk)}")
  logger::log_success("resolution:{resolution}")
  logger::log_success("future:{future}")
  logger::log_success("out:{out}")

  opp_var <- names(quoi |>
                     dplyr::as_tibble()  |>
                     dplyr::select(where(is.numeric)))

  if (length(opp_var) == 0)
  {
    opp_var <- "c"
    quoi <- dplyr::mutate(quoi, c=1)
  }

  names(opp_var) <- opp_var

  logger::log_success("les variables sont {c(opp_var)}")

  # fabrique les points d'origine (ou) et les points de cibles (ou)
  # dans le système de coordonnées 4326

  ouetquoi <- iso_ouetquoi_4326(
    ou = ou,
    quoi = quoi,
    res_ou = resolution,
    res_quoi = res_quoi,
    opp_var = opp_var,
    fun_quoi = fun_quoi,
    resolution = resolution)

  ou_4326 <- ouetquoi$ou_4326
  quoi_4326 <- ouetquoi$quoi_4326

  npaires_brut <- as.numeric(nrow(quoi_4326)) * as.numeric(nrow(ou_4326))

  # établit les paquets (sur une grille)

  groupes <- iso_split_ou(
    ou = ou_4326,
    quoi = quoi_4326,
    chunk = chunk,
    routing = routing,
    tmax = tmax)

  ou_4326 <- groupes$ou
  ou_gr <- groupes$ou_gr
  k <- groupes$subsampling
  logger::log_success("{f2si2(nrow(ou_4326))} ou, {f2si2(nrow(quoi_4326))} quoi")
  logger::log_success("{length(ou_gr)} carreaux, {k} subsampling")

  if (table2disk)
    if (is.null(dir)) {
      dir <- tempdir()
      purrr::walk(ou_gr, ~file.remove(str_c(dir,"/", .x,".*")))
    }
  else
    if (!dir.exists(dir))
      dir.create(dir)

  message("...calcul des temps de parcours")
  pb <- progressr::progressor(steps=sum(groupes$Nous))

  if(routing$future & future) {
    if(!is.null(routing$core_init)) {
      pl <- future::plan()
      future::plan(pl)
      pids <- furrr::future_map(
        1:(future::nbrOfWorkers()),
        ~ future:::session_uuid()[[1]])
      lt <- logger::log_threshold()
      nw <- future::nbrOfWorkers()
      splittage <- seq(0,length(ou_gr)-1) %/% ceiling(length(ou_gr)/nw)
      workable_ous <- split(ou_gr, splittage)
      access <- furrr::future_map(workable_ous, function(gs) {
        logger::log_threshold(lt)
        logger::log_appender(logger::appender_file(logfile))
        routing$core <- routing$core_init(routing)
        purrr::map(gs, function(g) {
          pb(amount=groupes$Nous[[g]])
          rrouting <- get_routing(routing, g)
          access_on_groupe(g, ou_4326, quoi_4326, rrouting, k, tmax, opp_var, ttm_out, pids, dir, t2d=table2disk)
        })
      }, .options=furrr::furrr_options(seed=TRUE,
                                       stdout=FALSE ,
                                       packages=c("data.table", "logger", "stringr", "glue", "r5r", "rJava"))) |>
        purrr::flatten()
    }
    else {
      pl <- future::plan()
      future::plan(pl)
      pids <- furrr::future_map(
        1:(future::nbrOfWorkers()),
        ~future:::session_uuid()[[1]])
      lt <- logger::log_threshold()
      access <- furrr::future_map(
        ou_gr,
        function(g) {
          logger::log_threshold(lt)
          logger::log_appender(logger::appender_file(logfile))
          pb(amount=groupes$Nous[[g]])
          rrouting <- get_routing(routing, g)
          access_on_groupe(g, ou_4326, quoi_4326, rrouting, k, tmax, opp_var, ttm_out, pids, dir, t2d=table2disk)
        },
        .options=furrr::furrr_options(seed=TRUE, stdout=FALSE, packages=c("data.table", "logger", "stringr", "glue", "r5r")))
    }}
  else {
    pids <- future:::session_uuid()[[1]]
    access <- purrr::map(ou_gr, function(g) {
      pb(amount=groupes$Nous[[g]])
      rrouting <- get_routing(routing, g)
      access_on_groupe(g, ou_4326, quoi_4326, rrouting, k, tmax, opp_var, ttm_out, pids, dir, t2d=table2disk)
    })
  }

  access_names <- names(access)
  access <- rbindlist(access, use.names = TRUE, fill = TRUE)
  if(ttm_out)
  {
    message("...finalisation du routing engine")
    gc()
    pl <- future::plan()
    future::plan(pl) # pour reprendre la mémoire
    if(table2disk) {
      access <- purrr::map(set_names(access$file, access_names),~{
        tt <- qs::qread(.x, nthreads=4)
        if(is.null(tt)) return(NULL)
        tt[, .(fromId, toId, travel_time)]
        setkey(tt, fromId)
        setindex(tt, toId)
        tt
      }) |> purrr::compact()
      # if(length(access)>1)
      #   access <- rbindlist(access, use.names = TRUE)
    }
    npaires <- sum(map_dbl(access, nrow))
    res <- list(
      type = "dt",
      origin = routing$type,
      origin_string = routing$string,
      string = glue::glue("matrice de time travel {routing$type} precalculee"),
      time_table = access,
      fromId = ou_4326[, .(id, lon, lat, x, y, gr)],
      toId = quoi_4326[, .(id, lon, lat, x, y)],
      groupes = ou_gr,
      resolution = groupes$resINS,
      res_ou = resolution,
      res_quoi = res_quoi,
      ancres = FALSE,
      future =TRUE,
      mode = routing$mode)
  }
  else
  {
    if(table2disk)
      access <- rbindlist(
        purrr::map(access$file,~qs::qread(.x, nthreads=4)), use.names = TRUE, fill = TRUE)

    npaires <- sum(access[, .(npaires=npep[[1]]), by=fromId][["npaires"]])
    access[, `:=`(npea=NULL, npep=NULL)]

    groupes_ok <- unique(access$gr)
    groupes_pasok <- setdiff(ou_gr, groupes_ok)
    ou_pasok <- ou_4326[gr %chin% groupes_pasok, .N]

    logger::log_warn("{ou_pasok} ({signif(ou_pasok/nrow(ou_4326)*100, 1)}%) origines non evaluees ")
    access[, gr:=NULL]
    message("...cumul")
    setnames(access, new="temps", old="travel_time")
    timecrossou <- CJ(fromId=ou_4326$id,temps=c(0:tmax), sorted=FALSE)
    access <- merge(timecrossou, access, by=c("fromId", "temps"), all.x=TRUE)
    for (v in opp_var)
      set(access, i=which(is.na(access[[v]])), j=v, 0)
    setorder(access, fromId, temps)
    access_c <- access[, lapply(.SD, cumsum),
                       by=fromId,
                       .SDcols=opp_var]
    temps_c <- access[, .(temps=temps),by=fromId]
    access_c[,temps:=temps_c$temps]
    tt <- seq(pdt, tmax, pdt)
    access_c <- access_c[temps%in%tt, ]
    access_c <- merge(access_c, ou_4326, by.x="fromId", by.y="id")

    r_xy <- access_c[, .(x=x[[1]], y=y[[1]]), by=fromId] [, fromId:=NULL]

    if(is.numeric(out)) {
      outr <- resolution
      out <- "raster"
    }

    res <- switch(
      out,
      data.table = access_c,
      sf = access_c |>
        dplyr::as_tibble() |>
        st_as_sf(coords=c("x","y"), crs = 3035),
      raster = {
        message("...rasterization")
        ttn <- paste0("iso",tt, "m")
        ids <- idINS3035(ou_4326$x, ou_4326$y, resolution = outr)
        ids <- data.table(fromId = ou_4326$id, idINS200 = ids)
        purrr::map(opp_var, function(v) {
          r_xy <- data.table::dcast(access_c, fromId~temps, value.var=v)
          names(r_xy) <- c("fromId", ttn)
          r_xy <- merge(r_xy, ids, by="fromId")[, fromId:=NULL]
          raster::readAll(dt2r(r_xy, resolution=outr))
        })})

  }

  dtime <- as.numeric(Sys.time()) - as.numeric(start_time)
  red <- 100 * (npaires_brut - npaires) / npaires_brut
  tmn <- second2str(dtime)
  speed_b <- npaires_brut / dtime
  speed <- npaires / dtime
  mtime <- glue::glue("{tmn} - {f2si2(npaires)} routes - {f2si2(speed_b)} routes(brut)/s - {f2si2(speed)} routes/s - {signif(red,2)}% reduction")
  message(mtime)
  logger::log_success("{routing$string} en {mtime}")
  attr(res, "routing") <- glue::glue("{routing$string} en {mtime}")
  message("...nettoyage")
  future::plan() # remplace plan(plan())
  gc()

  res
}

# souci avec la fonction where, non exportée de tidyselect.
# la solution recommandée, au lieu de ::: est la suivante :
utils::globalVariables("where")

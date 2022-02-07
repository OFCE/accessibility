#' Crée un pavage carré en tant que géométrie d'un sf à partir de idINS.
#'
#' @param ids vecteur d'idINS.
#' @param resolution resolution, par défaut celle attachée à idINS.
#'
#'
#' @import sf
#' @export
idINS2square <- function(ids, resolution=NULL)
{
  cr_pos <- stringr::str_locate(ids[[1]], "r(?=[0-9])")[,"start"]+1
  cy_pos <- stringr::str_locate(ids[[1]], "N(?=[0-9])")[,"start"]+1
  cx_pos <- stringr::str_locate(ids[[1]], "E(?=[0-9])")[,"start"]+1
  lcoord <- cx_pos-cy_pos-1
  y <- as.numeric(stringr::str_sub(ids,cy_pos,cy_pos+lcoord))
  x <- as.numeric(stringr::str_sub(ids,cx_pos,cx_pos+lcoord))
  r <- if(is.null(resolution))
    as.numeric(stringr::str_sub(ids,cr_pos,cy_pos-cr_pos))
  else
    rep(resolution, length(x))
  purrr::pmap(list(x,y,r), ~sf::st_polygon(
    list(matrix(
      c(..1, ..1+..3,..1+..3,..1, ..1,
        ..2, ..2, ..2+..3,..2+..3,..2),
      nrow=5, ncol=2))))  |>
    st_sfc(crs=3035)
}

#' Récupère les coordonnées X et Y de idINS.
#'
#' @param ids vecteur d'idINS.
#' @param resolution resolution, par défaut celle attachée à idINS.
#'
#' @export
idINS2point <- function(ids, resolution=NULL)
{
  cr_pos <- stringr::str_locate(ids[[1]], "r(?=[0-9])")[,"start"]+1
  cy_pos <- stringr::str_locate(ids[[1]], "N(?=[0-9])")[,"start"]+1
  cx_pos <- stringr::str_locate(ids[[1]], "E(?=[0-9])")[,"start"]+1
  lcoord <- cx_pos-cy_pos-1
  y <- as.numeric(stringr::str_sub(ids,cy_pos,cy_pos+lcoord))
  x <- as.numeric(stringr::str_sub(ids,cx_pos,cx_pos+lcoord))
  r <- if(is.null(resolution))
    as.numeric(stringr::str_sub(ids,cr_pos,cy_pos-cr_pos))
  else
    rep(resolution, length(x))
  m <- matrix(c(x+r/2,y+r/2), ncol=2)
  colnames(m) <- c("X", "Y")
  m
}

#' Crée idINS à partir de coordonnées.
#'
#' @param x Ou un vecteur de la coordonnée x, ou un df avec les colonnes x et y.
#' @param y vecteur de la coordonnée y, si nécessaire. NULL par défaut.
#' @param resolution resolution, par défaut 200.
#' @param resinstr faut-il inscrire la résolution dans idINS ? Par défaut TRUE.
#'
#' @export
idINS3035 <- function(x, y=NULL, resolution=200, resinstr=TRUE)
{
  if(is.null(y))
  {
    y <- x[,2]
    x <- x[,1]
  }
  resolution <- round(resolution)
  x <- formatC(floor(x / resolution )*resolution, format="d")
  y <- formatC(floor(y / resolution )*resolution, format="d")
  resultat <- if(resinstr)
    paste0("r", resolution, "N", y, "E", x)
  else
    paste0("N", y, "E", x)

  nas <- which(is.na(y)|is.na(x))
  if (length(nas)>0) resultat[nas] <- NA

  resultat
}

#' Crée un raster de référence, vide.
#'
#' @param data sf.
#' @param resolution par défaut, 200.
#' @param crs par défaut, 3035 (European Terrestrial Reference System).
#'
#' @import sf
#' @export
raster_ref <- function(data, resolution=200, crs=3035)
{
  alignres <- max(resolution, 200)
  if(checkmate::testMultiClass(data,c("sf", "sfc")))
  {
    b <- st_bbox(data)
    crss <- st_crs(data)$proj4string
  }
  else
  {
    stopifnot("x"%in%names(data)&"y"%in%names(data))
    b <- list(xmin=min(data$x, na.rm=TRUE),
              xmax=max(data$x, na.rm=TRUE),
              ymin=min(data$y, na.rm=TRUE),
              ymax=max(data$y, na.rm=TRUE))
    crss <- sp::CRS(st_crs(crs)$proj4string)
  }
  ext <- raster::extent(floor(b$xmin / alignres )*alignres,
                        ceiling(b$xmax/alignres)*alignres,
                        floor(b$ymin/alignres)*alignres,
                        ceiling(b$ymax/alignres)*alignres)
  raster::raster(ext, crs=crss,  resolution=resolution)
}

#' Récupère la résolution dans idINS.
#'
#' @param dt data.table ou df.
#' @param idINS variable contenant idINS, par défaut, idINS.
getresINS <- function(dt, idINS="idINS") {
  purrr::map(
    purrr::keep(names(dt), ~ stringr::str_detect(.x, idINS)),
    ~{
      r <- stringr::str_extract(dt[[.x]], "(?<=r)[0-9]+") |> as.numeric()
      ur <- unique(r)
      if (length(ur)==0)
        list(idINS=.x, res=NA_integer_)
      else
        list(idINS=.x, res=ur)
    })
}

#' Crée un raster à partir d'un data.table avec idINS.
#'
#' @param dt data.table avec idINS.
#' @param resolution résolution du raster.
#' @param idINS nom de la variable idINS, par défaut "idINS".
#'
#' @import data.table
#' @import sf
#'
#' @export
dt2r <- function(dt, resolution=NULL, idINS="idINS")
{
  dt <- setDT(dt)
  rr <- getresINS(dt, idINS)
  ncol <- names(dt)
  if(length(rr)==0)
    idINSin <- FALSE
  else
  {
    if(!is.null(resolution))
      isresin <- purrr::map_lgl(rr, ~.x[["res"]]==resolution)
    else
      isresin <- c(TRUE, rep(FALSE, length(rr)-1))
    if(any(isresin))
    {
      res <- rr[which(isresin)][[1]]$res
      idINS <-rr[which(isresin)][[1]]$idINS
      idINSin <- TRUE
    }
    else
      idINSin <- FALSE
  }
  if (!idINSin)
  {
    stopifnot(!is.null(res))
    stopifnot("x" %in% ncol & "y" %in% ncol)
    dt[, idINS:=idINS3035(x,y,resolution=resolution)]
    idINS <- "idINS"
    res <- resolution
  }
  xy <- idINS2point(dt$idINS, resolution = res)
  dt[, `:=`(x=xy[,1], y=xy[,2])]
  rref <- raster_ref(dt, resolution = res, crs=3035)
  cells <- raster::cellFromXY(rref, xy)
  layers <- purrr::keep(ncol, ~(is.numeric(dt[[.x]]))&(!.x%in%c("x","y")))
  brickette <- raster::brick(
    purrr::map(layers,
               ~{
                 r <- raster::raster(rref)
                 r[cells] <- dt[[.x]]
                 r
               }))
  names(brickette) <- layers
  raster::crs(brickette) <- 3035
  brickette
}

#' Mise en forme des nombres (vient de f.communs)
#'
#' @param number nombre à mettre en forme
#' @param rounding logique. TRUE par défaut.
#' @param digits nombre de chiffre après la virgule.
#' @param unit méthode de choix de l'unité ("median" par défaut)
#'
#' @importFrom stats median
f2si2 <- function(number, rounding = TRUE, digits = 1, unit = "median") {
  lut <- c(
    1e-24, 1e-21, 1e-18, 1e-15, 1e-12, 1e-09, 1e-06,
    0.001, 1, 1000, 1e+06, 1e+09, 1e+12, 1e+15, 1e+18, 1e+21,
    1e+24
  )
  pre <- c(
    "y", "z", "a", "f", "p", "n", "u", "m", "", "k",
    "M", "G", "T", "P", "E", "Z", "Y"
  )
  ix <- ifelse(number!=0, findInterval(number, lut) , 9L)
  ix <- switch(unit,
               median = median(ix, na.rm = TRUE),
               max = max(ix, na.rm = TRUE),
               multi = ix
  )
  if (rounding == TRUE)
    scaled_number <- round(number/lut[ix], digits)
  else
    scaled_number <- number/lut[ix]

  sistring <- paste0(scaled_number, pre[ix])
  sistring[scaled_number==0] <- "0"
  return(sistring)
}

#' Mise en forme du temps
#'
#' @param temps temps en secondes
second2str <- function(temps) {
  temps <- round(temps)
  jours <- temps %/% (3600 * 24)
  temps <- temps %% (3600 * 24)
  heures <- temps %/% 3600
  temps <- temps %% 3600
  minutes <- temps %/% 60
  secondes <- temps %% 60
  s <- character(0)
  if (jours > 0) {
    s <- stringr::str_c(jours, "j ")
  }
  if (heures > 0) {
    s <- stringr::str_c(s, heures, "h ")
  }
  if (minutes > 0) {
    s <- stringr::str_c(s, minutes, "m ")
  }
  if (secondes > 0) {
    s <- stringr::str_c(s, secondes, "s")
  }
  stringr::str_replace(s, " $", "")
}

#' Projette un raster RGB
#'
#' En procédant à une interpolation, la projection simple d'un raster
#' produit un raster avec des valeurs non entières même si l'input
#' est composé de composantes RGB et entière (0-255)
#' La fonction projette donc dans un système de coordonnées
#' et rebase et arrondi les différentes couches.
#'
#' @param rgb le raster en entrée
#' @param crs le système de coordonnées en sortie
#'
#' @return un raster RGB projetté dans
#' @export
#'
projectrgb <- function(rgb, crs="3035", res=200) {
  maxs <- raster::cellStats(rgb, max)
  mins <- raster::cellStats(rgb, min)
  rgbp <- raster::projectRaster(from=rgb, res=res, crs=sp::CRS(glue::glue("EPSG:{crs}"))) # la projection fait un truc bizarre sur les entiers
  maxp <- raster::cellStats(rgbp, max)
  minp <- raster::cellStats(rgbp, min)
  rgbp <- round((rgbp-minp)/(maxp-minp)*(maxs-mins) + mins)
  rgbp
}

#' paires de trajets suivant mobpro
#'
#' A partir d'un carroyage des individus et de l'emploi, et des paires mobpro
#' on génére les paires à la résolution c200m 
#' 
#' @param mobpro les paires de communes 
#' @param c200ze un carroyage (ne seront générés que les carreaux dans c200ze)
#'
#' c200ze doit avoir une colonne com qui contient le code commune, 
#' une colonne ind pour les individus et emp pour les emplois ciblés
#' ces colonnes sont produites par le carroyage de l'insee et 
#' les données mobpro couplées aux fichiers fonciers 
#'
#' monpro doit avoir une colonne COMMUNE et une colonne DCLT
#'
#' @return un tibble avec une colonne fromidINS et une colonne toidINS
#' @export
#'
mobpro2idINS <- function(c200ze, paires_mobpro) {
  res <- map2_dfr(mobpro$COMMUNE, mobpro$DCLT, ~{
    from <- c200ze |> filter(com==.x) |> pull(idINS)
    to <- c200ze |> filter(com==.y) |> pull(idINS)
    tidyr::expand_grid(
      fromidINS = from,
      toidINS = to)
  })
}
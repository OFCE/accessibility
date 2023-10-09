

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

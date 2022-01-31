#' Choisit le jour du transit
#'
#' Permet de choisir un jour de semaine (ni samedi ni dimanche dans une plage
#' au milieu de la plage
#'
#' @param plage une plage
#' @param start_time l'heure
#'
#' @return un jour au format date
#' @export
choisir_jour_transit <- function(plage, start_time = "8:00:00") {
  jours <- plage[1]:plage[2] |>
    lubridate::as_date() |>
    purrr::discard( ~ wday(.x) == 1 | wday(.x) == 7)
  jours <- jours[ceiling(length(jours) / 2)]

  return(lubridate::ymd_hms(paste(jours, start_time)))
}

#' Choisit un jour
#'
#' En prenant un GTFS en entrée, la fonction choisit parmi les jours de semaine
#' un jour qui existe pour tous les réseaux
#'
#' @param directory le répertoire où se trouve le GTFS
#'
#' @return une date
#' @export
#'
#' @examples
choix_jour <- function(directory) {
  les_gtfs <- list.files(directory, pattern = "*gtfs*|*GTFS*") |>
    purrr::map( ~ tidytransit::read_gtfs(stringr::str_c(directory, .x, sep = "/")))

  les_jours <- purrr::map_dfr(les_gtfs, ~ {
    if (!("calendar" %in% names(.x)) ) {
      tmp <- .x$calendar_dates |>
        dplyr::filter(exception_type == 1) |>
        dplyr::group_by(date) |>
        dplyr::summarise(n = n()) |>
        dplyr::filter(lubridate::wday(date) %in% 2:6) |>
        dplyr::arrange(-n)

      return(tmp |> dplyr::slice(1) |> dplyr::transmute(min = date, max = date))
    }

    return(data.frame(min = do.call(max, purrr::map(les_gtfs, ~ min(.x$calendar$start_date))),
                      max = do.call(min, purrr::map(les_gtfs, ~ max(.x$calendar$end_date)))))
  })

  print(les_jours)

  les_jours <- les_jours |>
    dplyr::summarise(min = max(min), max = min(max))

  return(c(les_jours[[1, "min"]], les_jours[[1, "max"]]))
}


#' Plage de dates d'un GTFS
#'
#' Lit un GTFS et renvoie la plage de dates commune
#' aux différents réseaux du GTFS
#'
#' @param directory Répertoire du GTFS
#'
#' @return
#' @export
#'
#' @examples
plage <- function(directory) {
  les_gtfs <- list.files(directory, pattern = "*gtfs*|*GTFS*") |>
    purrr::map( ~ tidytransit::read_gtfs(str_c(directory, .x, sep = "/")))

  les_plages <- purrr::map_dfr(les_gtfs, get_plage_in_calendar) |>
    dplyr::summarise(debut = max(min.date), fin = min(max.date)) |>
    unlist()

  if (les_plages["debut"] > les_plages["fin"]) {
    stop("Les gtfs ne sont pas synchrones")
  } else {
      les_plages
    }
}


# non exportée

get_plage_in_calendar <- function(gtfs) {

  if(is.null(gtfs[["calendar"]])) {
    plage <- data.frame(min.date = min(gtfs[["calendar_dates"]]$date, na.rm = TRUE),
                        max.date = max(gtfs[["calendar_dates"]]$date, na.rm = TRUE))
  } else {
    plage <- data.frame(min.date = min(gtfs[["calendar"]]$start_date, na.rm = TRUE),
                        max.date = max(gtfs[["calendar"]]$start_date, na.rm = TRUE))
    }
}

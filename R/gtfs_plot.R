get_dest <- function(from, data) {
  data |>
    dplyr::filter(stop_id==!!from) |>
    dplyr::pull(to_stop)
}

add_dest <- function(line, data) {
  d <- get_dest(tail(line,1), data)
  ok <- is.na(d)
  newlines <- purrr::map(d, ~if(!is.na(.x)) c(line, .x) else line)
  list(lines=newlines, ok=ok)
}

build_line <-  function(start, data) {
  ok <- FALSE
  lines <- list(start)
  result <- list()
  while(!all(ok))
  {
    ll <- purrr::map(lines, ~add_dest(.x, data))
    lll <- purrr::map(ll, ~{
      ok <- .x$ok
      result <<- append(result, .x$lines[ok])
      list(l=.x$lines[!ok], ok=ok)
    })
    ok <- purrr::map(lll,"ok") |>
      purrr::flatten_lgl()
    lines <- purrr::map(lll, "l") |>
      purrr::flatten()
  }
  result
}

#' Extrait une ligne d'un GTFS
#'
#' @param gtfs le gtfs que l'on veut analyser
#' @param route_id l'id de la route
#'
#' @return un sf représentant la ligne en 3035
#' @export
#'
get_line <- function(gtfs, route_id)
{
  route <- gtfs$routes |>
    dplyr::filter(route_id==!!route_id)
  couleur <- stringr::str_c(
    "#",
    route |> dplyr::pull(route_color))
  trips <- gtfs$trips |>
    dplyr::filter(route_id==!!route_id, direction_id==0)
  stop_times <- dplyr::left_join(trips, gtfs$stop_times , by="trip_id")
  troncons <- stop_times |>
    dplyr::group_by(trip_id) |>
    dplyr::arrange(stop_sequence) |>
    dplyr::summarize(stop_id = list(stop_id)) |>
    dplyr::distinct(stop_id)
  if(nrow(troncons)==0) return(list(NULL))
  noninclus <- troncons$stop_id
  i <- 1
  for(l in troncons$stop_id)
  {
    sans_l <- noninclus[-i]
    inclu <- purrr::map_lgl(sans_l, ~length(setdiff(l,.x))==0)
    if(any(inclu)) noninclus <- sans_l else i <- i+1
  }
  troncons <- troncons |>
    dplyr::filter(stop_id %in% noninclus) |>
    dplyr::mutate(troncon_id=1:n()) |>
    tidyr::unnest(stop_id)

  troncons <- troncons |>
    dplyr::left_join(gtfs$stops |>
                       dplyr::select(stop_name, stop_id, lat=stop_lat, lon=stop_lon),
                     by="stop_id") |>
    sf::st_as_sf(coords=c("lon", "lat"), crs=4326) |>
    sf::st_transform(3035)
  lines <- troncons |>
    dplyr::group_by(troncon_id) |>
    dplyr::summarize(do_union=FALSE) |>
    sf::st_cast("LINESTRING") |>
    dplyr::rename(id=troncon_id) |>
    dplyr::mutate(route_id=route$route_id,
                  name=route$route_short_name,
                  route_type=route$route_type,
                  route_color=route$route_color,
                  id=as.character(id),
                  line=TRUE)
  stops <- troncons |>
    dplyr::group_by(stop_id) |>
    dplyr::summarize(name=first(stop_name)) |>
    dplyr::rename(id=stop_id) |>
    dplyr::mutate(route_id=route$route_id, route_type=route$route_type, route_color=route$route_color, line=FALSE)
  dplyr::bind_rows(lines, stops)
}

library(tmap)
library(dplyr)
library(terra)
library(data.table)
library(stars)
library(sf)
library(r5r)
load("~/larochelle/v2/baselayer.rda")
traffic_rdas = "~/larochelle/v2/bidouilles trafic/"

#a_di <- r5r::detailed_itineraries(r5r_core = r5_di$core, origins = les_individus[1:50,], destinations = les_opportunites[1:50,], mode = "CAR", all_to_all = TRUE)

# ## Données Ministère de la transition écologique : pas assez complètes (seulement les données sur les autoroutes)
# 
# tmja <- read_sf(dsn = "~/accessibility/trafic data", layer = "TMJA2019", crs = 2154) |> st_transform(crs = 4326)
# a <- st_join(tmja, st_as_sf(zone_emploi) |> st_transform(crs = 4326), join = st_intersects, left = FALSE) 
# tmap_mode("view")
# tm_shape(a) + tm_lines() + tm_shape(zone_emploi) + tm_borders()


# street_net <- street_network_to_sf(r5$core)

load(paste0(traffic_rdas, "lr_mapbox.rda"))
load(paste0(traffic_rdas, "mbpxdata.rda"))

test_traf <- as.data.table(a_di)
sf_traf <- a_di
# 
# st_rasterize(street_net$edges)
# lr_raster <- st_rasterize(lr_mpbx$traffic |> select(congestion, geometry) , options = "")

a <- lr_mpbx$traffic

rasterizeLine <- function(sfLine, rast, value){
  # rasterize roads to template
  tmplt <- stars::st_as_stars(sf::st_bbox(rast), nx = raster::ncol(rast),
                              ny = raster::nrow(rast), values = value)
  
  rastLine <- stars::st_rasterize(sfLine,
                                  template = tmplt,
                                  options = "ALL_TOUCHED=TRUE") %>%
    as("Raster")
  
  return(rastLine)
}

library(osmdata)
library(tidyverse)
library(sf)

a <- a |> mutate(congestion = as.factor(congestion) )

r <- raster::raster(a, ncol=10000, nrow=10000)

rast <- rasterizeLine(a, r, NA)

plot(rast)

st_join(rast, street_net)
lr_mpbx$traffic

testing <- st_join(lr_mpbx$traffic, sf_traf, join = st_intersects, left = FALSE)
testing2 <- st_join(lr_mpbx$traffic, sf_traf, join = st_intersects, left = TRUE)
testing3 <- st_join(nyc_cong_poly, sf_traf, join = st_intersects, left = FALSE)
testing4 <- st_join(nyc_cong_poly, sf_traf, join = st_intersects, left = TRUE)

to_pts <- test_traf[, .(idINS = unique(to_id))] |> mutate(geometry = idINS2square(idINS)) |> st_as_sf(sf_column_name = "geometry") |> st_centroid()
from_pts <- test_traf[, .(idINS = unique(from_id))] |> mutate(geometry = idINS2square(idINS)) |> st_as_sf(sf_column_name = "geometry") |> st_centroid()

tmap_mode("plot")
tm_shape(sf_traf) + tm_lines(col = "black") +
  tm_shape(to_pts) + tm_dots(col = "red") +
  tm_shape(from_pts) + tm_dots(col = "blue") + 
  tm_shape(testing3) + tm_lines(col = "congestion", palette = c("green", "orange", "red", "maroon"))
#raster::focal
tmap_mode("view")
# tm_shape(testing3) + tm_lines(col = "black") +
#   tm_shape(sf_traf) + tm_lines(col = "grey") +
  tm_shape(to_pts) + tm_dots(col = "red") +
  tm_shape(from_pts) + tm_dots(col = "blue") + 
  tm_shape(communes.scot3) + tm_borders(col = "black") +
  tm_shape(nyc_cong_poly) + tm_lines(col = "congestion", palette = c("green", "orange", "red", "maroon"))

gc()
st_length(testing)

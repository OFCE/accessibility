library(tmap)
library(data.table)
library(stars)

traffic_rdas = "~/larochelle/v2/bidouilles trafic/"

street_net <- street_network_to_sf(r5$core)

load(paste0(traffic_rdas, "lr_mapbox.rda"))
a_di <- tibble::rowid_to_column(a_di, "id")
  
test_traf <- as.data.table(a_di)
sf_traf <- a_di

st_rasterize(street_net$edges)
st_rasterize(lr_mpbx$traffic)

lr_mpbx$traffic

testing <- st_join(lr_mpbx$traffic, sf_traf, join = st_intersects, left = FALSE)
testing2 <- st_join(lr_mpbx$traffic, sf_traf, join = st_intersects, left = TRUE)
to_pts <- test_traf[, .(idINS = unique(to_id))] |> mutate(geometry = idINS2square(idINS)) |> st_as_sf(sf_column_name = "geometry") |> st_centroid()
from_pts <- test_traf[, .(idINS = unique(from_id))] |> mutate(geometry = idINS2square(idINS)) |> st_as_sf(sf_column_name = "geometry") |> st_centroid()

tmap_mode("plot")
tm_shape(sf_traf) + tm_lines(col = "black") +
  tm_shape(to_pts) + tm_dots(col = "red") +
  tm_shape(from_pts) + tm_dots(col = "blue") + 
  tm_shape(testing) + tm_lines(col = "congestion", palette = c("red", "green", "orange", "maroon")) + 
  tm_shape(testing2) + tm_lines(col = "grey") 

gc()
st_length(testing)

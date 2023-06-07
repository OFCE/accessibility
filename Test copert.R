copert_file = "~/larochelle/COPERT/"
vh_file <- "~/larochelle/COPERT/Données parc auto/"

load("~/larochelle/v2/baselayer.rda")
library(data.table)
emission_factors_hot = readxl::read_xlsx(path = paste0(copert_file, "Emission_factors_2022.xlsx"), sheet = 2)
emission_factors_cold = readxl::read_xlsx(path = paste0(copert_file, "Emission_factors_2022.xlsx"), sheet = 3)

sample = readxl::read_xlsx(path = paste0(copert_file, "Sample_1990_2020.xlsx"))
res_sample = readxl::read_xlsx(path = paste0(copert_file, "Results_Sample_1990_2020.xlsx"))

names(emission_factors_hot)[names(emission_factors_hot) == "50"] <- "V"

parc_vh <- readxl::read_xlsx(path = paste0(vh_file, "parc_voitures.xlsx"), skip = 3) 
names(parc_vh)[names(parc_vh) == "Code commune de résidence"] <- "COM"
setDT(parc_vh)
a <- parc_vh[COM %in% communes.scot3$COM, ]
a


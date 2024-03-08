library(data.table)
library(foreach)
library(doFuture)
library(stringr)
library(purrr)
library(dplyr)

ndc <- readRDS("data/ndc_to_atc_no_flags.rds")

plan(multisession, workers = 30)

opioid_flags <- foreach(code = ndc[, atc]) %dofuture% {
  list(flag_opioid_analgesic = any(str_detect(code, "N02A"), na.rm = T),
       flag_opioid_anesthetics = "N01AH" %in% code,
       flag_opioid_treat_dependence = "N07BC" %in% code, 
       flag_antispasmodics_analgesics = any(str_detect(code, "A03D"), na.rm = T),
       flag_antispasmodics_psycoleptics_analgesics = "A03EA" %in% code, 
       flag_antiinflam = any(str_detect(code, "M01"), na.rm = T), 
       flag_topical = any(str_detect(code, "M02A"), na.rm = T), 
       flag_muscle_relax = any(str_detect(code, "M03"), na.rm = T), 
       flag_other_analgesic = any(str_detect(code, "N02B"), na.rm = T), 
       flag_antidepress = any(str_detect(code, "N06A"), na.rm = T))
}

plan(sequential)

ndc[, `:=`(flag_opioid_analgesic = map_lgl(opioid_flags, "flag_opioid_analgesic"), 
           flag_opioid_anesthetics = map_lgl(opioid_flags, "flag_opioid_anesthetics"), 
           flag_opioid_treat_dependence = map_lgl(opioid_flags, "flag_opioid_treat_dependence"), 
           flag_antispasmodics_analgesics = map_lgl(opioid_flags, "flag_antispasmodics_analgesics"),
           flag_antispasmodics_psycoleptics_analgesics = map_lgl(opioid_flags, "flag_antispasmodics_psycoleptics_analgesics"), 
           flag_antiinflam = map_lgl(opioid_flags, "flag_antiinflam"), 
           flag_topical = map_lgl(opioid_flags, "flag_topical"), 
           flag_muscle_relax = map_lgl(opioid_flags, "flag_muscle_relax"), 
           flag_other_analgesic = map_lgl(opioid_flags, "flag_other_analgesic"), 
           flag_antidepress = map_lgl(opioid_flags, "flag_antidepress"))]

summarize(ndc, across(flag_opioid_analgesic:flag_antidepress, sum))
sum(summarize(ndc, across(flag_opioid_analgesic:flag_antidepress, sum)))

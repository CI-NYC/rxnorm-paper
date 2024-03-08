# READ ME -----------------------------------------------------------------
#
#       Author: Nick Williams
# Last updated: 7 March 2024
#
# Converts NDC codes to WHO ATC drug classes. Relies on running
#   RxNav-in-a-box with Docker for faster processing. 
# 
# Convert NDC -> RxCUI -> ATC
#
#  Input: data/all_unique_ndcs.rds
# 
# -------------------------------------------------------------------------

library(rxnorm)
library(data.table)
library(purrr)
library(stringr)
library(foreach)
library(readr)
library(glue)
library(doFuture)
library(yaml)

local <- TRUE

# Load list of NDCs
ndc <- as.data.table(readRDS("data/all_unique_ndcs.rds"))

# Convert NDC -> RxCUI -> ATC
plan(multisession, workers = 10)

ndc_status <- foreach(code = ndc[, NDC]) %dofuture% {
  get_ndc_status(code, local_host = TRUE)
}

ndc[, ndc_status := unlist(ndc_status)]

unknown <- ndc[ndc_status == "UNKNOWN" | is.na(ndc_status), ]
ndc <- ndc[ndc_status %in% c("ACTIVE", "OBSOLETE", "ALIEN")]

drug_classes <- foreach(code = ndc$NDC) %dofuture% {
  rxcui <- from_ndc(code, local_host = TRUE)
  atc <- get_atc(rxcui, local_host = TRUE)
  list(rxcui = rxcui, atc = atc)
}

ndc[, `:=`(rxcui = map_chr(drug_classes, "rxcui"), 
           atc = map(drug_classes, "atc"))]

obsolete <- ndc[is.na(atc) & !is.na(rxcui), ]

rxcui_status <- foreach(code = obsolete[, rxcui]) %dofuture% {
  get_rxcui_status(code, local_host = TRUE)
}

obsolete[, rxcui_status := unlist(rxcui_status)]

new_rxcui <- 
  foreach(code = obsolete[, rxcui], 
          status = obsolete[, rxcui_status]) %dofuture% {
            if (status %in% c("Remapped", "NotCurrent")) {
              return(get_remapped_rxcui(code, local_host = local))
            }
            
            if (status == "Obsolete") {
              return(get_scd_rxcui(code, local_host = local))
            }
            
            if (status == "Quantified") {
              return(get_quantified_rxcui(code, local_host = local))
            }
            
            return(code)
          }

obsolete[, rxcui := fifelse(!is.na(unlist(new_rxcui)), unlist(new_rxcui), rxcui)]

alt_drug_class <- foreach(code = unlist(new_rxcui)) %dofuture% {
  if (is.na(code)) return(NA_character_)
  get_atc(code, local_host = TRUE)
}

obsolete[, atc := alt_drug_class]

ndc <- rbind(ndc[!(NDC %in% obsolete$NDC), ], 
             obsolete[, .(NDC, ndc_status, rxcui, atc, rxcui_status)], 
             fill = TRUE)

unclassified <- ndc[is.na(atc) & !is.na(rxcui), ]

rxname <- foreach(code = unclassified[, rxcui]) %dofuture% {
  get_rx(code, local_host = local)
}

unclassified[, rxname := rxname]

saveRDS(ndc, "data/ndc_to_atc_no_flags.rds")
saveRDS(unclassified, "data/unclassified_ndc.rds")

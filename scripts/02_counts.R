# -------------------------------------
# Script:
# Author:
# Purpose:
# Notes:
# -------------------------------------

library(glue)
library(data.table)
library(arrow)
library(tidyverse)

src_root <- "/mnt/processed-data/disability"
drv_root <- "/mnt/general-data/disability/mediation_unsafe_pain_mgmt"

# Load data ---------------------------------------------------------------

# Read in RXL (pharmacy line)
files <- paste0(list.files(src_root, pattern = "TAFRXL", recursive = TRUE))
parquet_files <- grep("\\.parquet$", files, value = TRUE)
rxl <- open_dataset(file.path(src_root, parquet_files))

# Read in OTL (Other services line) 
files <- paste0(list.files(src_root, pattern = "TAFOTL", recursive = TRUE))
parquet_files <- grep("\\.parquet$", files, value = TRUE)
otl <- open_dataset(file.path(src_root, parquet_files))

ndc <- as.data.table(readRDS("data/all_unique_ndcs.rds"))
ndc <- ndc[grep("^\\d{11}$", ndc$NDC), ]

linked <- readRDS("data/ndc_to_atc_no_flags.rds")

rxl_count <- rxl |> 
  group_by(NDC) |>
  summarise(n = n()) |> 
  collect()

otl_count <- otl |> 
  group_by(NDC) |>
  summarise(n = n()) |> 
  collect()

setDT(rxl_count)
setDT(otl_count)

ndc_counts <- rbind(rxl_count, otl_count)[, .(n = sum(n)), by = NDC]
ndc_counts <- merge(ndc_counts, linked[, .(NDC, ndc_status, rxcui, atc)], all.x = TRUE)
ndc_counts <- ndc_counts[grep("^\\d{11}$", ndc_counts$NDC), ]
ndc_counts[, ndc_status := map_chr(ndc_status, \(x) ifelse(is.null(x), NA_character_, x))]
ndc_counts[, prop := n / sum(n)]

# Counts ------------------------------------------------------------------

# total claims
ndc_counts[, sum(n)]
# total NDC
ndc[, .N]
# No. of NDC per NDC status
linked[, .(.N, prop = .N / ndc[, .N]), ndc_status]
# No. of unknown NDC
ndc[, .(N = .N - linked[, .N])][, .(N, prop = N / ndc[, .N])]
# No. and proportion of linked NDC by NDC status
linked[!is.na(atc), .N, ndc_status][, .(ndc_status, N, prop = N / linked[, .N, ndc_status]$N)]
# No. and proportion of the total linked NDC
linked[!is.na(atc), .(N = .N)][, .(N, prop = N / ndc[, .N])]
# Proportion of linked NDC that account for all NDC
ndc_counts[!is.na(rxcui) & !is.na(atc), .(N = .N, prop = .N / ndc[, .N], prop_all = sum(prop))]

linked[!is.na(rxcui), .N, rxcui_status][, .(rxcui_status, N, prop = (N / linked[!is.na(rxcui_status), .N])*100)]
linked[!is.na(rxcui) & !is.na(atc), .N, rxcui_status]

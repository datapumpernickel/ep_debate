#### PAUL BOCHTLER
#### 26.10.2023

#-----------------------------------------#
#### SET ENVIRONMENT                   ####
#-----------------------------------------#

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
setwd("../")
getwd()

## empty potential rests from other scripts
rm(list = ls())

## load packages and install missing packages
require("pacman")
## define libraries to be loaded
packs <-
  c(
    'tidyverse',
    "janitor",
    'purrr',
    'rvest',
    'httr2',
    'glue',
    'furrr' )

p_load(char = packs)

source("03_code/00_functions.R")





# Get Session Dates and Links -------------------------------------------------------------------------------------

## Set Directory for Calendar Data
dir <- '01_raw_data/calendar'
dir.create(dir, recursive = T)  # Ensure directory exists

## Download Calendar Data
for(i in 5:9){
  get_calendar(i, dir)  # Fetch and save calendar data
}

## Process Calendar Files
calendars <- list.files(dir, full.names = T) |>
  map_dfr(read_csv, show_col_types = F) |>
  clean_names() |>
  mutate(id = str_remove_all(url, '.*document/') |>
           str_remove_all('.html')) |>
  distinct() |>
  filter(!is.na(url))  # Remove NA URLs

## Count Unique IDs
length(unique(calendars$id))

# Get TOCs (Table of Contents) -------------------------------------------------------------------------------------

## Set Directory for TOC Data
dir <- '01_raw_data/tocs'
dir.create(dir, recursive = T)  # Ensure directory exists

## Download and Process TOCs in Parallel
future::plan("multisession", workers = 6)
future_pmap(list(url = calendars$url, dir = dir, id = calendars$id), get_tocs, .progress = T)

## List TOC File Paths
paths <- list.files(dir, full.names = T)

# Parse TOCs -------------------------------------------------------------------------------------

## Parse TOCs in Parallel
future::plan("multisession", workers = 14)
all_tops <- future_map_dfr(paths, parse_tocs, .progress = T)

# Data Cleaning and Filtering -------------------------------------------------------------------------------------

frequent_tocs_not_debates <- all_tops |>
  filter(!str_detect(orders,'\\(debate\\)')) |>
  mutate(orders = tolower(orders) |> str_squish()) |>
  mutate(orders = str_remove_all(orders,'^ \\d{1,2}. ')) |>
  mutate(orders = str_remove_all(orders,'^\\d{1,2}. ')) |>
  count(orders) |>
  filter(!n<2)|>
  filter(!str_detect(orders,'one-minute speeches')) |>
  filter(!str_detect(orders,'human rights'))|>
  filter(!str_detect(orders,'topical and urgent debate'))|>
  filter(!str_detect(orders,'zimbabwe'))|>
  filter(!str_detect(orders,'situation in the middle east')) |>
  filter(!n<10)


unfrequent_tocs_greater_one_not_debates <- all_tops |>
  filter(!str_detect(orders,'\\(debate\\)')) |>
  mutate(orders = tolower(orders) |> str_squish()) |>
  mutate(orders = str_remove_all(orders,'^ \\d{1,2}. ')) |>
  mutate(orders = str_remove_all(orders,'^\\d{1,2}. ')) |>
  count(orders) |>
  filter(!n<2)|>
  filter(!str_detect(orders,'one-minute speeches')) |>
  filter(!str_detect(orders,'human rights'))|>
  filter(!str_detect(orders,'topical and urgent debate'))|>
  filter(!str_detect(orders,'zimbabwe'))|>
  filter(!str_detect(orders,'situation in the middle east')) |>
  filter(n<10)

unfrequent_tocs_greater_one_debates <- all_tops |>
  filter(!str_detect(orders,'\\(debate\\)')) |>
  mutate(orders = tolower(orders) |> str_squish()) |>
  mutate(orders = str_remove_all(orders,'^ \\d{1,2}. ')) |>
  mutate(orders = str_remove_all(orders,'^\\d{1,2}. ')) |>
  count(orders) |>
  filter(!n<2)|>
  filter(!str_detect(orders,'one-minute speeches')) |>
  filter(!str_detect(orders,'human rights'))|>
  filter(!str_detect(orders,'topical and urgent debate'))|>
  filter(!str_detect(orders,'zimbabwe'))|>
  filter(!str_detect(orders,'situation in the middle east'))|>
  filter(n<10) |>
  filter(!str_detect(orders,'vote|discharge|amendment|negotiation|resumption|welcome|adjournment|action taken|address by|corrigend|composition of|third voting|voting time|written declaration|written statement|statement by the president|approval of the|change to the agenda|delegated acts|^election of the|formal sitting|oral question|request for|signature of|other business|consent procedure|calendar of part-session|membership of'))

unfrequent_tocs_greater_one_not_debates <- anti_join(unfrequent_tocs_greater_one_not_debates,
                                                     unfrequent_tocs_greater_one_debates, by = 'orders')



only_debates <- all_tops |>
  mutate(orig_orders = orders,
         orders = tolower(orders) |> str_squish()) |>
  anti_join(frequent_tocs_not_debates, by = 'orders') |>
  anti_join(unfrequent_tocs_greater_one, by = 'orders') |>
  filter(!str_detect(orders,'votes'))|>
  filter(!str_detect(orders,'motion of censure'))|>
  filter(!str_detect(orders,'opening of the session'))|>
  filter(!str_detect(orders,'closing of the session'))|>
  filter(!str_detect(orders,'adjournment of the session'))|>
  filter(!str_detect(orders,'question time '))|>
  filter(!str_detect(orders,'order of business ')) |>
  mutate(id = str_remove_all(orders_ref,'/doceo/document/')) |>
  mutate(orders_ref = str_c('https://www.europarl.europa.eu',orders_ref))


# Get Debates -------------------------------------------------------------------------------------

## Set Directory for Debate Data
dir <- '01_raw_data/debates'
dir.create(dir, recursive = T)  # Ensure directory exists

## Download and Process Debates in Parallel
future::plan("multisession", workers = 24)
future_pmap(list(url = only_debates$orders_ref, dir = dir, id = only_debates$id), get_debate, .progress = T)

# Parse Debates -------------------------------------------------------------------------------------

## List Debate File Paths
files <- list.files(dir, full.names = T)

## Parse Debates in Parallel
future::plan("multisession", workers = 8)
test <- future_map_dfr(files, parse_debate, .progress = T)

# Save Processed Data -------------------------------------------------------------------------------------

## Create Clean Data Directory
dir.create('02_clean_data')

## Save Parsed Debates as RDS
write_rds(test, '02_clean_data/parsed_debates_raw.rds')

## Zip the RDS File
zip(zipfile = '02_clean_data/parsed_raw_debates.zip',
    files = '02_clean_data/parsed_debates_raw.rds')
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
    'httr2',"lubridate",
    'glue',"cli","datatable".
    'furrr',"archive")

p_load(char = packs)

source("03_code/00_functions.R")

parse_debates_logical <- F


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

## check which ones might be debates

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
  anti_join(unfrequent_tocs_greater_one_not_debates, by = 'orders') |>
  filter(!str_detect(orders,'votes'))|>
  filter(!str_detect(orders,'motion of censure'))|>
  filter(!str_detect(orders,'opening of the session'))|>
  filter(!str_detect(orders,'closing of the session'))|>
  filter(!str_detect(orders,'adjournment of the session'))|>
  filter(!str_detect(orders,'question time '))|>
  filter(!str_detect(orders,'order of business ')) |>
  mutate(id = str_remove_all(orders_ref,'/doceo/document/')) |>
  mutate(orders_ref = str_c('https://www.europarl.europa.eu',orders_ref)) |> 
  distinct(id, orig_orders,path, orders_ref)



# Get Debates -------------------------------------------------------------------------------------

## make id and links
debates_links <- all_tops |>
  mutate(orig_orders = orders,
         orders = tolower(orders) |> str_squish())|>
  mutate(id = str_remove_all(orders_ref,'/doceo/document/')) |>
  mutate(orders_ref = str_c('https://www.europarl.europa.eu',orders_ref))

## Set Directory for Debate Data
dir <- '01_raw_data/debates'
dir.create(dir, recursive = T)  # Ensure directory exists

## Download and Process Debates in Parallel
pmap(
  list(
    url = debates_links$orders_ref,
    dir = dir,
    id = debates_links$id
  ),
  get_debate,
  .progress = T
)

# Parse Debates -------------------------------------------------------------------------------------

if(parse_debates_logical) {
  ## List Debate File Paths
  files <- list.files(dir, full.names = T)
  
  ## Parse Debates in Parallel
  future::plan("multisession", workers = 32)
  full_data <- future_map_dfr(files, parse_debate, .progress = T)
  # Save Processed Data -------------------------------------------------------------------------------------
  
  ## Create Clean Data Directory
  dir.create('04_clean_data')
  
  ## Save Parsed Debates as RDS
  write_rds(full_data, '04_clean_data/parsed_debates_raw.rds')
  
  ## Zip the RDS File
  zip(zipfile = '04_clean_data/parsed_raw_debates.zip',
      files = '04_clean_data/parsed_debates_raw.rds')
}

## unpack data
archive::archive_extract('04_clean_data/parsed_raw_debates.zip', 
                         dir = "04_clean_data",
                         files = "02_clean_data/parsed_debates_raw.rds")

fs::file_move("04_clean_data/02_clean_data/parsed_debates_raw.rds","04_clean_data/parsed_debates_raw.rds")
fs::dir_delete("04_clean_data/02_clean_data")

speeches <- read_rds("04_clean_data/parsed_debates_raw.rds")


speeches_clean <- speeches |>
  left_join(
    debates_links |> mutate(
      path_toc = path,
      path = str_c("01_raw_data/debates/", id)
    ) |>
      select(orig_orders, orders_ref, path_toc, path)
  ) |>
  mutate(
    date = str_remove_all(path, ".*debates/"),
    month = str_extract(date, "-\\d{2}-") |> str_remove_all("-"),
    day = str_extract(date, "-\\d{2}-[A-Za-z]") |> str_remove("-[A-Za-z]"),
    year = str_extract(date, "\\d{4}")
  ) |>
  mutate(date_clean = dmy(str_c(day, "-", month, "-", year))) |>
  select(
    date_clean,
    text = paragraphs_text,
    speaker = paragraphs_header,
    party ,
    italics = parl_function,
    path_cre = path,
    path_toc,
    orig_orders,
    url_cre = orders_ref
  )
  

# first_paragraph <- speeches_clean |> 
#   group_by(path) |> 
#   slice(1) |> 
#   mutate(debate = str_detect(paragraphs_text,"debate|Debate")) |> 
#   mutate(year = str_extract(path, "\\d{4}")) |> 
#   ungroup()
# 
# 
# speakers <- speeches_clean |> 
#   count(paragraphs_header)


parlspeech <- read_csv("01_raw_data/ext_dta/ParlEE_EP_plenary_speeches.csv") |> 
  mutate(date_clean = dmy(date))



speeches_clean_add <- speeches_clean |>
  filter(!date_clean %in% parlspeech$date_clean) |>
  group_by(path_cre) |>
  mutate(id_speaker = consecutive_id(speaker)) |>
  rowwise() |>
  mutate(first_string = paste(speaker, party, italics,
                              collapse = " ")) |>
  mutate(first_string = str_remove_all(first_string, "NA")) |>
  group_by(path_cre, id_speaker) |>
  mutate(p_id = row_number(),
         text = if_else(p_id == 1, str_remove(text, first_string), text)) |>
  group_by(path_cre, id_speaker) |>
  mutate(
    p_id = row_number(),
    text = if_else(
      p_id == 1,
      stringi::stri_replace_all_fixed(text, 
                                      first_string, 
                                      replacement = ""),
      text
    )
  )


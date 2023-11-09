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
    'glue',"cli",
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
future::plan("multisession", workers = 32)
future_pmap(list(url = calendars$url, dir = dir, id = calendars$id), get_tocs, .progress = T)

## List TOC File Paths
paths <- list.files(dir, full.names = T)

# Parse TOCs -------------------------------------------------------------------------------------

## Parse TOCs in Parallel
all_tops <- future_map_dfr(paths, parse_tocs, .progress = T)

# Data Cleaning and Filtering -------------------------------------------------------------------------------------

## check which ones might be debates
# 
# frequent_tocs_not_debates <- all_tops |>
#   filter(!str_detect(orders,'\\(debate\\)')) |>
#   mutate(orders = tolower(orders) |> str_squish()) |>
#   mutate(orders = str_remove_all(orders,'^ \\d{1,2}. ')) |>
#   mutate(orders = str_remove_all(orders,'^\\d{1,2}. ')) |>
#   count(orders) |>
#   filter(!n<2)|>
#   filter(!str_detect(orders,'one-minute speeches')) |>
#   filter(!str_detect(orders,'human rights'))|>
#   filter(!str_detect(orders,'topical and urgent debate'))|>
#   filter(!str_detect(orders,'zimbabwe'))|>
#   filter(!str_detect(orders,'situation in the middle east')) |>
#   filter(!n<10)
# 
# 
# unfrequent_tocs_greater_one_not_debates <- all_tops |>
#   filter(!str_detect(orders,'\\(debate\\)')) |>
#   mutate(orders = tolower(orders) |> str_squish()) |>
#   mutate(orders = str_remove_all(orders,'^ \\d{1,2}. ')) |>
#   mutate(orders = str_remove_all(orders,'^\\d{1,2}. ')) |>
#   count(orders) |>
#   filter(!n<2)|>
#   filter(!str_detect(orders,'one-minute speeches')) |>
#   filter(!str_detect(orders,'human rights'))|>
#   filter(!str_detect(orders,'topical and urgent debate'))|>
#   filter(!str_detect(orders,'zimbabwe'))|>
#   filter(!str_detect(orders,'situation in the middle east')) |>
#   filter(n<10)
# 
# unfrequent_tocs_greater_one_debates <- all_tops |>
#   filter(!str_detect(orders,'\\(debate\\)')) |>
#   mutate(orders = tolower(orders) |> str_squish()) |>
#   mutate(orders = str_remove_all(orders,'^ \\d{1,2}. ')) |>
#   mutate(orders = str_remove_all(orders,'^\\d{1,2}. ')) |>
#   count(orders) |>
#   filter(!n<2)|>
#   filter(!str_detect(orders,'one-minute speeches')) |>
#   filter(!str_detect(orders,'human rights'))|>
#   filter(!str_detect(orders,'topical and urgent debate'))|>
#   filter(!str_detect(orders,'zimbabwe'))|>
#   filter(!str_detect(orders,'situation in the middle east'))|>
#   filter(n<10) |>
#   filter(!str_detect(orders,'vote|discharge|amendment|negotiation|resumption|welcome|adjournment|action taken|address by|corrigend|composition of|third voting|voting time|written declaration|written statement|statement by the president|approval of the|change to the agenda|delegated acts|^election of the|formal sitting|oral question|request for|signature of|other business|consent procedure|calendar of part-session|membership of'))
# 
# unfrequent_tocs_greater_one_not_debates <- anti_join(unfrequent_tocs_greater_one_not_debates,
#                                                      unfrequent_tocs_greater_one_debates, by = 'orders')
# 
# only_debates <- all_tops |>
#   mutate(orig_orders = orders,
#          orders = tolower(orders) |> str_squish()) |>
#   anti_join(frequent_tocs_not_debates, by = 'orders') |>
#   anti_join(unfrequent_tocs_greater_one_not_debates, by = 'orders') |>
#   filter(!str_detect(orders,'votes'))|>
#   filter(!str_detect(orders,'motion of censure'))|>
#   filter(!str_detect(orders,'opening of the session'))|>
#   filter(!str_detect(orders,'closing of the session'))|>
#   filter(!str_detect(orders,'adjournment of the session'))|>
#   filter(!str_detect(orders,'question time '))|>
#   filter(!str_detect(orders,'order of business ')) |>
#   mutate(id = str_remove_all(orders_ref,'/doceo/document/')) |>
#   mutate(orders_ref = str_c('https://www.europarl.europa.eu',orders_ref)) |> 
#   distinct(id, orig_orders,path, orders_ref)
# 


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
future_pmap(
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
# archive::archive_extract('04_clean_data/parsed_raw_debates.zip', 
#                          dir = "04_clean_data",
#                          files = "02_clean_data/parsed_debates_raw.rds")
# 
# fs::file_move("04_clean_data/02_clean_data/parsed_debates_raw.rds","04_clean_data/parsed_debates_raw.rds")
# fs::dir_delete("04_clean_data/02_clean_data")

speeches_clean <- read_rds("04_clean_data/parsed_debates_raw.rds") |>
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
  

if(F){
  ## get data from ParlEE 
  ## here: https://dataverse.harvard.edu/file.xhtml?fileId=6936027&version=1.1 
  request("https://dvn-cloud.s3.amazonaws.com/10.7910/DVN/VOPK0E/186509b6618-519382a36288?response-content-disposition=attachment%3B%20filename%2A%3DUTF-8%27%27ParlEE_EP_plenary_speeches.csv&response-content-type=text%2Fcsv&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20231030T103745Z&X-Amz-SignedHeaders=host&X-Amz-Expires=3600&X-Amz-Credential=AKIAIEJ3NV7UYCSRJC7A%2F20231030%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=f4bc63499f38fa35284e73b17e09ea5f1aacae43615f617e9c639414ed407733") |> 
    req_perform() |> 
    resp_body_string() |> 
    write_lines("01_raw_data/ext_data/parlee_ep_plenary_speeches.csv")
}


parlspeech <- read_csv("01_raw_data/ext_dta/parlee_ep_plenary_speeches.csv") |> 
  mutate(date_clean = dmy(date))


future::plan("multisession", workers = 12)

speeches_collapsed <- speeches_clean |>
  filter(!date_clean %in% parlspeech$date_clean) |>
  group_by(path_cre) |>
  mutate(id_speaker = consecutive_id(speaker)) |>
  ungroup() |>
  group_by(path_cre, id_speaker) |>
  mutate(p_id = row_number()) |>
  ungroup() |>
  mutate(text = future_pmap_chr(list(p_id, text, speaker, party, italics), ## clean mention of party and speaker from text itself 
                                function(p_id, text, speaker, party, italics) {
                                  if (p_id == 1) {
                                    if (!is.na(speaker)) {
                                      text <-
                                        stringi::stri_replace_first_fixed(text, speaker, replacement = "")
                                    }
                                    if (!is.na(party)) {
                                      text <-
                                        stringi::stri_replace_first_fixed(text, party, replacement = "")
                                    }
                                    if (!is.na(italics)) {
                                      text <-
                                        stringi::stri_replace_first_fixed(text, italics, replacement = "")
                                    }
                                    text <- text |>
                                      str_squish()
                                  } else {
                                    text <- str_squish(text)
                                  }
                                  return(text)
                                  
                                },
                                .progress = T)) |>
  group_by(date_clean,
           speaker,
           path_cre,
           path_toc,
           orig_orders,
           url_cre,
           id_speaker) |>
  summarise( ## collapse texts
    text = paste0(text |> str_squish(), collapse = "\n"),
    italics = paste0(italics |> str_squish(), collapse = "\n"),
    party = paste0(party |> str_squish(), collapse = "\n")
  ) |>
  select(date_clean,
         id_speaker,
         speaker,
         party,
         italics,
         text,
         url_cre,
         everything()) |>
  ungroup() |>
  mutate( ## keep only unique party and italics mentions for each collapsed speaker/
    party = future_map_chr(
      party,
      ~ str_split_1(.x, pattern = "\\n") |> unique() |> paste0(collapse = ";"),
      .progress = T
    ) ,
    italics = future_map_chr(
      italics,
      ~ str_split_1(.x, pattern = "\\n") |> unique() |> paste0(collapse = ";"),
      .progress = T
    )
  ) |>
  mutate(text_id = cur_group_id(), .by = c(url_cre, id_speaker)) |>
  arrange(date_clean, url_cre) |>
  mutate(session_id = cur_group_id(), .by = c(url_cre)) |>
  filter(!is.na(text)) |>
  select(text_id, session_id, everything())

## write speeches that are missing in Parlee dataset

write_csv(speeches_collapsed, "04_clean_data/missing_speeches_parlee.csv")


### tokenization happens with spacy in python (see sentence_segmentation.py)

speeches_add_sents <- read_csv("04_clean_data/missing_speeches_parlee_sents.csv") |> 
  select(-...1) |> 
  janitor::clean_names()

speeches_add_clean <- speeches_collapsed |> 
  select(-text) |> 
  left_join(speeches_add_sents, by = c("text_id","session_id","id_speaker"),multiple = "all")

future::plan("multisession", workers = 12)

translations <- read_lines("04_clean_data/translations/translated_sentences.jsonl") |> 
  future_map_dfr(jsonlite::fromJSON)

translations_with_original <- translations |> 
  janitor::clean_names() |> 
  left_join(speeches_add_clean |> select(sentence, text_id, sentence_id))

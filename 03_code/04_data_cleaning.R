
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

## read in parlee data
parlspeech <- read_csv("01_raw_data/ext_dta/parlee_ep_plenary_speeches.csv") |> 
  mutate(date_clean = dmy(date))

## read in collapsed speech data that are not in parlee
speeches_collapsed <- read_csv(speeches_collapsed, "04_clean_data/missing_speeches_parlee.csv")

### tokenization happens with spacy in python (see sentence_segmentation.py)

## read in segemented sentences 
speeches_add_sents <- read_csv("04_clean_data/missing_speeches_parlee_sents.csv") |> 
  select(-...1) |> 
  janitor::clean_names()

## combine collapsed data with sentences
speeches_add_clean <- speeches_collapsed |> 
  select(-text) |> 
  left_join(speeches_add_sents, by = c("text_id","session_id","id_speaker"),multiple = "all")

future::plan("multisession", workers = 12)

## read in translations
translations <- read_lines("04_clean_data/translations/translated_sentences.jsonl") |> 
  future_map_dfr(jsonlite::fromJSON)

translations_with_original <- translations |> 
  janitor::clean_names() |> 
  left_join(speeches_add_clean |> select(sentence, text_id, sentence_id))

#### PAUL BOCHTLER
#### 11.12.2021

#-----------------------------------------#
#### SET ENVIRONMENT                   ####
#-----------------------------------------#

setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
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


dir <- '01_raw_data/calendar'

get_calendar <- function(termid, dir){
  if(!file.exists(glue('{dir}/{termid}.csv'))){
  url <- "https://www.europarl.europa.eu/plenary/en/ajax/getSessionCalendar.html?family=CRE&termId="
  composed_url <- glue(url,termid)
  res <- request(composed_url) |>
    req_perform() |>
    resp_body_json(simplifyVector = T) |>
    pluck("sessionCalendar")

  write_csv(res,glue('{dir}/{termid}.csv'))}
}

dir.create(dir,recursive = T)

for(i in 5:9){
  get_calendar(i,dir)
}

calendars <- list.files(dir,full.names = T) |>
  map_dfr(read_csv, show_col_types = F) |>
  clean_names() |>
  mutate(id = str_remove_all(url,'.*document/') |>
           str_remove_all('.html')) |>
  distinct() |>
  filter(!is.na(url))

length(unique(calendars$id))

dir <- '01_raw_data/tocs'
dir.create(dir,recursive = T)



get_tocs <- function(url, dir, id) {
  if (!file.exists(glue('{dir}/{id}.html'))) {
    res <- request(url) |>
      req_throttle(120 / 60) |>
      req_perform() |>
      resp_body_string() |>
      write_lines(glue('{dir}/{id}.html'))
  }
}
future::plan("multisession", workers = 6)
future_pmap(list(url = calendars$url, dir = dir,id = calendars$id),get_tocs,.progress = T)


paths <- list.files(dir, full.names = T)
path <- paths[1]
parse_tocs <- function(path){

  html <- read_html(path)

  orders <- html_elements(html,'.list_summary a') |>
    html_text()
  orders_ref <- html_elements(html,'.list_summary a') |>
    html_attr('href')

  result <- tibble(orders,orders_ref,path)

}


future::plan("multisession", workers = 14)
all_tops <- future_map_dfr(paths,parse_tocs,.progress = T)



check_non_debates <- all_tops |>
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


check_non_debates2 <- all_tops |>
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

part_debates <- all_tops |>
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

check_non_debates2 <- anti_join(check_non_debates2,part_debates, by = 'orders')



only_debates <- all_tops |>
  mutate(orig_orders = orders,
         orders = tolower(orders) |> str_squish()) |>
  anti_join(check_non_debates, by = 'orders') |>
  anti_join(check_non_debates2, by = 'orders') |>
  filter(!str_detect(orders,'votes'))|>
  filter(!str_detect(orders,'motion of censure'))|>
  filter(!str_detect(orders,'opening of the session'))|>
  filter(!str_detect(orders,'closing of the session'))|>
  filter(!str_detect(orders,'adjournment of the session'))|>
  filter(!str_detect(orders,'question time '))|>
  filter(!str_detect(orders,'order of business ')) |>
  mutate(id = str_remove_all(orders_ref,'/doceo/document/')) |>
  mutate(orders_ref = str_c('https://www.europarl.europa.eu',orders_ref))


dir <- '01_raw_data/debates'
dir.create(dir,recursive = T)


get_debate <- function(url,dir,id){
  if (!file.exists(glue('{dir}/{id}.html'))) {
    res <- request(url) |>
      req_throttle(120 / 60) |>
      req_perform() |>
      resp_body_string() |>
      write_lines(glue('{dir}/{id}'))
  }
}

future::plan("multisession", workers = 3)
future_pmap(list(url = only_debates$orders_ref, dir = dir,id = only_debates$id),get_debate,.progress = T)


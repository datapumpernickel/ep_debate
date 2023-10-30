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


get_tocs <- function(url, dir, id) {
  if (!file.exists(glue('{dir}/{id}.html'))) {
    res <- request(url) |>
      req_throttle(120 / 60) |>
      req_perform() |>
      resp_body_string() |>
      write_lines(glue('{dir}/{id}.html'))
  }
}

parse_tocs <- function(path){
  
  html <- read_html(path)
  
  orders <- html_elements(html,'.list_summary a') |>
    html_text()
  orders_ref <- html_elements(html,'.list_summary a') |>
    html_attr('href')
  
  result <- tibble(orders,orders_ref,path)
  
}
# url <-  debates_links$orders_ref[22604]

safe_req_perform <- safely(req_perform)

get_debate <- function(url, dir, id) {
  if (!file.exists(glue('{dir}/{id}'))) {
    res <- request(url) %>%
      req_throttle(120 / 60)
    
    res_safe <- safe_req_perform(res)
    
    if (is.null(res_safe$result)) {
      cli::cli_alert_warning(glue("Performing request failed: {res_safe$error}"))
      return(NULL)
    }
    
    resp_body_string(res_safe$result) %>%
      write_lines(glue('{dir}/{id}'))
  }
}


parse_debate <- function(path){
  html <- read_html(path)
  
  paragraphs_header <- html_elements(html, '.contents') |>
    map_chr(~html_element(.x,'.doc_subtitle_level1_bis') |>  html_text())
  
  paragraphs_text <- html_elements(html, '.contents') |>
    map_chr(html_text)
  
  party <- html_elements(html, '.contents') |>
    map_chr(~html_element(.x,'.bold') |>  html_text())
  
  parl_function <- html_elements(html, '.contents') |>
    map_chr(~html_element(.x,'.italic') |>  html_text())
  
  result <-
    tibble(paragraphs_text, paragraphs_header, party, parl_function, path) |>
    fill(paragraphs_header, party,parl_function, .direction = 'down')
  return(result)
}
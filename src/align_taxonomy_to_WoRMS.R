library(dplyr)
library(worrms)
library(future)
library(furrr)
library(progressr)

#' align taxonomy to WoRMS
#'
#' @description align_taxonomy_to_worms` returns a table of matches to WoRMS for the
#' lowest (most specific) rank for which there is an exact match.
#'
#' @details For this to work properly, a data frame of latin names and ranks
#' should be passed to the argument `tax_df` for querying WoRMS with the
#' worrms API client. This function will automatically search from lowest to highest
#' rank until it finds an exact match.  If not exact match is found, it returns an
#' identification of 'incerate sedis' with lsid = urn:lsid:marinespecies.org:taxname:12
#' so that the taxonomy is correctly interpreted by OBIS and GBIF.
#'
#' This has an issue that needs to be resolved.  The kingdoms in WoRMS are:
#' Animalia, Archaea, Bacteria, Chromista, Fungi, Plantae, Protozoa, and Viruses.
#' So there is no entry for "Eukaryota" and we need to decide how to interpret it
#'
#' (This isn't working yet) To view progress, wrap the function in "with_progress" from the progressr package,
#' as described here: https://furrr.futureverse.org/articles/progress.html
#'
#' @param tax_df is a data frame that includes the scientific names to be queried.
#' It can also include other columns, like an identifier for an ASV or observation.
#' @param ordered_rank_columns This argument should be a character vector of ranks
#' in the order which they should be queried. The default value is
#' c("species", "genus","family","order","class","phylum","kingdom") but the strings
#' are not important and don't need to contain all ranks or match WoRMS.
#' They are specified here so that the function understands the order
#' of columns to query.
#' @param ASV_column_name Name of the row identifier.  If no value is passed (the default)
#' then the taxonomic table will only return information from WoRMS. If a column is named,
#' the value for each row will be added to the data to serve as a key for updating taxonomy in
#' related data.
#' @param parallel default value is TRUE, which will runf the function in paralle using
#' 3/4 of the available processors. If FALSE, the function will be run in serial.
#' @returns a data frame of exact matches from WoRMS.
#'
#' @import dplyr
#' @import worrms
#' @import future
#' @import furrr
#' @import progressr
#'
#' @examples
#' df <- data.frame(ASV = 12345, Kingdom = "Animalia", Phylum = "Cnidaria", Genus = "Acropora")
#' align_taxonomy_to_worms(tax_df = df, ordered_rank_columns = c("Genus", "Phylum", "Kingdom"), ASV_column_name = "ASV")

align_taxonomy_to_worms <- function(tax_df,
                                    ordered_rank_columns = c("species",
                                                             "genus",
                                                             "family",
                                                             "order",
                                                             "class",
                                                             "phylum",
                                                             "kingdom"),
                                    ASV_column_name = NULL,
                                    parallel = TRUE) {
  stopifnot(!is.null(tax_df))
  stopifnot(is.data.frame(tax_df))
  stopifnot(ordered_rank_columns %in% names(tax_df))
  
  # If parallel = FALSE, then reset workers
  if (parallel) {
    future::plan(strategy = "multisession",
                 workers = future::availableCores() * 0.75)
  } else{
    future::plan("sequential")
  }
  
  p <- progressr::progressor(steps = length(nrow(tax_df)))
  
  matches <-
    future_map(split(tax_df, 1:nrow(tax_df)), function(x) {
      p()
      
      for (i in ordered_rank_columns) {
        if (!is.na(x[[i]])) {
          q <- try(worrms::wm_records_name(name = x[[i]],
                                           fuzzy = FALSE,
                                           marine_only = FALSE))
          
          Sys.sleep(time = 1) #Without this I was getting '429 - too many request' errors
          
          if (any(class(q) != "try-error")) {
            print("match found")
            
            #If more than one match, filter to accepted, exact match
            if (nrow(q) > 1 & "accepted" %in% q$status) {
              q <- q %>% filter(status == "accepted")
            }
            
            #If more than one match, but none are accepted, print message
            if (nrow(q) > 1 & !"accepted" %in% q$status) {
              print(x)
              print("requires closer examination due to unclear matching")
            }
            
            if (!is.null(ASV_column_name)) {
              q <- cbind(x[ASV_column_name], q)
            }
            
            return(q)
            break
          }
          
          if (any(class(q) == "try-error")) {
            print(paste0("no match found: ", x[[i]]))
            q <- data.frame(scientificname = "incertae sedis",
                            lsid = "urn:lsid:marinespecies.org:taxname:12")
            if (!is.null(ASV_column_name)) {
              q <- cbind(x[ASV_column_name], q)
            }
            
            
            return(q)
          }
          
          else{
            print("NA value")
            q <- data.frame(scientificname = NA,
                            lsid = NA)
            if (!is.null(ASV_column_name)) {
              q <- cbind(x[ASV_column_name], q)
            }
          }
        }
      }
    },
    .options = furrr_options(seed = NULL)) #%>%
  #data.table::rbindlist(fill = TRUE)
  
  future::plan("sequential")
  
  return(matches)
}

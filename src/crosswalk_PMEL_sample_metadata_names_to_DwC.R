# Make crosswalk from data headers to DwC terms

library(dplyr)

# read in headers from sample_metadata files, write to CSV and edit by hand in excel
list.files(path = "data/sample_metadata/",
           pattern = "*.txt",
           full.names = TRUE) %>%
  lapply(., function(x) {
    read.table(x, sep = "\t", header = TRUE) %>% names()
    
  }) %>%
  unlist() %>%
  unique() %>%
  data.frame(PEML_names = .,
             DwC_term = NA,
             Notes = "") %>% 
  write.csv(
    file = "documentation/PMEL_sample_metadata_to_DwC_map.csv",
    row.names = FALSE
  )

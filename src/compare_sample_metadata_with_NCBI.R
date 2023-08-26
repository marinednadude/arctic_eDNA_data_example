# Compare sample names in sample_metadata to NCBI

library(xml2)
library(dplyr)

#Load data

#NCBI samples names; xml downloaded by hand from NCBI
NCBI <- read_xml(x = "documentation/PRJNA982176_biosample_result.xml") %>% 
  xml_find_all(xpath = "//Id[@db_label='Sample name']") %>% 
  as_list() %>%  
  unlist() %>% 
  sort()

sample_metadata <- read.table(file = "data/sample_metadata/sample_metadata_16S.txt",
                              sep = "\t",
                              header = TRUE) %>%
  pull(Sample) %>%
  sort()

#compare samples names

NCBI
sample_metadata

which(!NCBI %in% sample_metadata)

# It looks like a lot of the mismatches are just the middle string being 1B or 2B, let's remove that

NCBI <- sub(pattern = "1B", replacement = "2B", x = NCBI)
sample_metadata <- sub(pattern = "1B", replacement = "2B", x = sample_metadata)

#Everything in NCBI is in sample_metadata
NCBI[!NCBI %in% sample_metadata] %>% sort()

#One sample is missing from NCBI
sample_metadata[!sample_metadata %in% NCBI] %>% sort()

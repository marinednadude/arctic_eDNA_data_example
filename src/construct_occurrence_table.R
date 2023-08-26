# construct occurrence table from sample metadata, ASV taxonomy, ASV counts

library(dplyr)

target_gene <- "16S"

# align taxonomy with WoRMS ---------------------------------------------------


# This will need us to identify the most specific level that is not NA, and then compare these names to WoRMS. 
# If there is no match, then you move up one rank.
# Since it can get sticky and need notes, I like to do this as a separate script.



# read in data ----------------------------------------------------------

sample_metadata <- read.table(file = paste0("data/sample_metadata/",
                                            paste0("sample_metadata_",
                                                   target_gene,
                                                   ".txt"
                                                   )
                                            ), 
                              sep = "\t",
                              header = TRUE
                              )

tax_table <- read.table(file = paste0("data/",
                                      target_gene,
                                      "/",
                                      paste0(target_gene,
                                             "_ASV2TaxonomyTable.txt"
                                             )
                                      ), 
                        sep = "\t",
                        header = TRUE
                        )

asv_counts <- read.table(file = paste0("data/",
                                       target_gene,
                                       "/",
                                       paste0(target_gene,
                                              "_ASVCounts.txt"
                                              )
                                       ), 
                         sep = "\t",
                         header = TRUE
                         )

# pivot asv_counts so that it can be joined with sample_metadata and strip prefix "MP_"

asv_counts <- asv_counts %>% 
  tidyr::pivot_longer(names_to = "Sample", values_to = "ASV_count", cols = -x) %>% 
  rename(ASV = x) %>% 
  arrange(Sample) %>% 
  mutate(Sample = stringr::str_remove(string = Sample, pattern = "MP_"))


# align taxonomy with WoRMS -----------------------------------------------

tax_out <- align_taxonomy_to_worms(
  tax_df = head(tax_table),
  ordered_rank_columns = c(
    "Species",
    "Genus",
    "Family",
    "Order",
    "Class",
    "Phylum",
    "Kingdom"
  ),
  ASV_column_name = "ASV",
  parallel = TRUE
)

# join tables ------------------------------------------------

df <- full_join(x = sample_metadata,
                y = asv_counts) %>% 
  left_join(x = .,
            y = tax_table)

glimpse(df)


# align with Darwin Core --------------------------------------------------

#These are examples of the types of things that need to happen
occurrence_table <- df %>% 
  mutate(eventDate = ,
         eventID = ,
         occurrenceID = ,
         geodeticDatum = "WGS84") %>% 
  rename(decimalLatitude = lat)
  
# split into occurrence core and DNA derived extension --------------------


  # This will be done by calling the XML files and comparing column headers to the terms

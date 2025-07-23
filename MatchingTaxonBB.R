library(data.table)

#Load GBIF taxonomic backbone

Taxon <- fread("~/Documents/LKCNHM_volunteering/gbif/Taxon.tsv", sep = "\t", quote = "")
Taxon <- Taxon[, lapply(.SD, as.character)]

#Shrink Taxon to include only entries and fields wanted for matching

Taxonrankedonly <- Taxon[
  Taxon$taxonRank != "unranked" &
    Taxon$taxonomicStatus %in% c("accepted", "synonym", "homotypic synonym", "proparte synonym", "heterotypic synonym"),
]

Taxonsml <- Taxonrankedonly[, c("taxonID", "genericName", "genus", "specificEpithet", "infraspecificEpithet", "taxonomicStatus", "acceptedNameUsageID")]
Taxonsml <- Taxonsml[
  !(Taxonsml$genus == "" | Taxonsml$specificEpithet == ""),
]

#Load allBOS taxonsplit info
outputsplit <- read.csv("~/Documents/LKCNHM_volunteering/outputsplit.csv")

# Convert both to data.table
setDT(Taxonsml)
setDT(outputsplit)

outputsplit[, genericName := genus] 
#setnames(outputsplit, old = c("species", "subspecies", "genus"), new = c("specificEpithet", "infraspecificEpithet", "genericName"))
setnames(outputsplit, old = c("species", "subspecies"), new = c("specificEpithet", "infraspecificEpithet"))
outputsplit[] <- lapply(outputsplit, function(x) {
  if (is.character(x)) trimws(x) else x
})

# Set key for efficient join
setkey(Taxonsml, genericName, specificEpithet, infraspecificEpithet)

# Replace NA with placeholder in both tables
Taxonsml[, infraspecificEpithet := fifelse(is.na(infraspecificEpithet), "__none__", infraspecificEpithet)]
outputsplit[, infraspecificEpithet := fifelse(is.na(infraspecificEpithet), "__none__", infraspecificEpithet)]

#Datacleaning for outputsplit
outputsplit[genericName == "Glenea" & specificEpithet == "mathemathica", specificEpithet := "mathematica"]
outputsplit[genericName == "Glenea" & infraspecificEpithet == "mathemathica", infraspecificEpithet := "mathematica"]
outputsplit[genericName == "Bavia" & specificEpithet == "sexupunctata", specificEpithet := "sexpunctata"]
outputsplit[genericName == "Omoedus" & specificEpithet == "ephippigera", specificEpithet := "ephippiger"]
outputsplit[genericName == "Byblis" & specificEpithet == "kallartha", specificEpithet := "kallarthra"]
outputsplit[genericName == "Alcockpenaeopsis" & specificEpithet == "hungerfordi", specificEpithet := "hungerfordii"]
outputsplit[genericName == "Pseudosesarma" & specificEpithet == "edwardsi", specificEpithet := "edwardsii"]
outputsplit[genericName == "Urocaridella" & specificEpithet == "antonbruuni", specificEpithet := "antonbruunii"]
outputsplit[genericName == "Ocypode" & specificEpithet == "cordimanus", specificEpithet := "cordimana"]



# Perform join
matching <- Taxonsml[outputsplit, on = .(genericName, specificEpithet, infraspecificEpithet)]

# (Optional) Restore NAs if needed
matching[infraspecificEpithet == "__none__", infraspecificEpithet := NA]

# Perform left join: bring in taxonID to outputsplit
# matching <- Taxonsml[outputsplit, on = .(genus, specificEpithet, infraspecificEpithet)]

# Rename taxonID result for clarity
setnames(matching, "taxonID", "matched_taxonID")


#find out if there are any acceptedIDs which are missing from the whole taxonID values
missing_ids <- as.character(setdiff(Taxon$acceptedNameUsageID, Taxon$taxonID))

#fwrite(matching, "C:/Data/matching.csv")

checking <- matching[
  !is.na(genericName) & genericName != "" &
    !is.na(specificEpithet) & specificEpithet != "" &
    is.na(matched_taxonID)
]

# cleaning up matching
#reparse matching
matching <- matching[,c("speciesId","matched_taxonID","acceptedNameUsageID","taxonName",
                        "domain","kingdom","phylum","class","subclass","superorder",
                        "order","sub.order","infraorder","section", "subsection", "superfamily",
                        "family","subfamily","tribe","genus","subgenus","specificEpithet","infraspecificEpithet")
]

contentious <- matching[speciesId %in% speciesId[duplicated(speciesId)]]
matching <- matching[!matching$speciesId %in% matching$speciesId[duplicated(matching$speciesId)], ]

#RYAN: I DID NOT INCLUDE THIS LINE BELOW, not sure if it is needed
contentious <- contentious[!contentious$acceptedNameUsageID %in% contentious$matched_taxonID, ]

# Identify unique speciesId rows in contentious
unique_rows <- contentious[!duplicated(contentious$speciesId) & !duplicated(contentious$speciesId, fromLast = TRUE), ]

# Append to matching
matching <- rbind(matching, unique_rows)

# Remove those rows from contentious
contentious <- contentious[!contentious$speciesId %in% unique_rows$speciesId, ]


#RYAN: at 95 rows are appended and then at 105, it is removed??!
#remove rows from matching where speciesId are in contentious
matching <- matching[!speciesId %in% contentious$speciesId]

cleanfortaxoDBport <- matching[!is.na(matching$matched_taxonID), ]


#add column to Taxon for speciesId located in column 2
#create nomatch from matching which only contains values where matched_taxonId is empty
nomatch <- matching[is.na(matched_taxonID)]

#TODO: STOPPED HERE - RYAN
#set columns for parent matching
priority_cols <- c(
  "infraspecificEpithet", "specificEpithet", "genus", "family",
  "order", "class", "phylum", "kingdom", "domain"
)

#within nomatch,create column taxonRank all values are  BOSunconfirmedsp and column taxonomicStatus al values are BOSunconfirmed
nomatch[, taxonRank := "BOSunconfirmedsp"]
nomatch[, taxonomicStatus := "BOSunconfirmed"]

#create a column parentNameUsageID in nomatch
nomatch[, parentNameUsageID := NA_character_]
setcolorder(nomatch, c(
  "speciesId", "matched_taxonID", "acceptedNameUsageID", "taxonName", 
  "parentNameUsageID",
  setdiff(names(nomatch), c("speciesId", "matched_taxonID", "acceptedNameUsageID", "taxonName", "parentNameUsageID"))
))

#fill up parent names in nomatch

### split accepted_taxon_lookup into different tables based on phyllum or class
### for each table created, do the join if phylum in nomatch matches phyllum in taxon for value in each table??

accepted_taxon_lookup <- Taxon[
  taxonomicStatus == "accepted" & kingdom %in% c("Animalia", "Plantae"),
  .(canonicalName, taxonID)
]
accepted_taxon_lookup <- accepted_taxon_lookup[!(is.na(canonicalName) | canonicalName == "")]
setorder(accepted_taxon_lookup, canonicalName)
accepted_taxon_lookup[, .N, by = canonicalName][N > 1]

accepted_taxon_lookup_repeats <- accepted_taxon_lookup[
  duplicated(canonicalName) | duplicated(canonicalName, fromLast = TRUE)
]
accepted_taxon_lookup <- accepted_taxon_lookup[!(canonicalName %in% accepted_taxon_lookup_repeats$canonicalName)]


for (col in priority_cols) {
  nomatch[
    is.na(parentNameUsageID) & !is.na(get(col)) & get(col) != "",
    parentNameUsageID := accepted_taxon_lookup[
      .SD,
      on = c("canonicalName" = col),
      x.taxonID
    ],
    .SDcols = col
  ]
}

# Add canonicalName of parentNameUsageID to nomatch
nomatch <- merge(
  nomatch,
  Taxon[, .(taxonID, canonicalName)],
  by.x = "parentNameUsageID",
  by.y = "taxonID",
  all.x = TRUE,
  suffixes = c("", "_parent")
)

# Rename the new column for clarity
setnames(nomatch, "canonicalName", "parentCanonicalName")

nomatch <- nomatch[,c("speciesId","taxonName","parentNameUsageID","parentCanonicalName","taxonRank","taxonomicStatus","domain","kingdom","phylum","class","subclass","superorder","order","sub.order","infraorder","section","subsection","superfamily","family","subfamily","tribe","genus","subgenus","specificEpithet","infraspecificEpithet")]
# for each row in nomatch, find the first column from the right that is not blank find the row in taxon where taxon$canonicalName holds it and taxonomic status == accepted, fill parentNameUsageID in nomatch with the value of the taxonID for that row



# fwrite(cleanfortaxoDBport, "C:/Data/cleanfortaxoDBport.csv")
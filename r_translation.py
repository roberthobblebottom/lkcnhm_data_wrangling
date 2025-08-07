import polars as pl


def join():
    bos_df = (
        pl.read_csv("outputsplit.csv")
        .rename(
            {
                "data cleanup changes": "data cleanup changes 1",
                "species": "specificEpithet",
                "subspecies": "infraspecificEpithet",
                "domain ": "domain",
            }
        )
        .with_columns(pl.col(pl.String).str.strip_chars(" "))
        .with_columns(genericName=pl.col("genus"))
    )
    # Taxonsml <- Taxonsml[

    null_counts_df = (
        bos_df.null_count()
        .transpose(include_header=True)
        .rename({"column": "feature", "column_0": "len"})
    )
    columns_to_drop_as_all_nulls = (
        null_counts_df.filter(null_counts_df["len"] == bos_df.shape[0])
        .transpose()[0, :]
        .transpose()
        .to_series()
        .to_list()
    )
    columns_to_drop_as_they_are_just_changes_logs = [
        "cleanup changes ",
        "Data cleanup changes",
        "data cleanup changes 1",
        "changes",
        "cleanup changes",
        "Unnamed: 18",
        "Unnamed: 17",
        "Unnamed: 19",
        "query",
        "query ",
        "issue",
        "cleanup changes/comments",
        "subphylum",
    ]
    bos_df = (
        (
            bos_df.drop(columns_to_drop_as_all_nulls)
            .drop(columns_to_drop_as_they_are_just_changes_logs)
            .select(~pl.selectors.starts_with("Unnamed"))
        )
        .with_columns(
            infraspecificEpithet=pl.when(pl.col("infraspecificEpithet").is_null())
            .then(pl.lit(""))
            .otherwise(pl.col("infraspecificEpithet"))
        )
        .with_columns(
            infraspecificEpithet=pl.when(
                (pl.col("genericName") == "Glenea")
                & (pl.col("specificEpithet") == "mathemathica")
            )
            .then(pl.lit("mathematica"))
            .otherwise(pl.col("infraspecificEpithet"))
        )
        .with_columns(
            specificEpithet=pl.when(
                (pl.col("genericName") == "Glenea")
                & (pl.col("specificEpithet") == "mathemathica")
            )
            .then(pl.lit("mathematica"))
            .when(
                (pl.col("genericName") == "Bavia")
                & (pl.col("specificEpithet") == "sexupunctata")
            )
            .then(pl.lit("sexpunctata"))
            .when(
                (pl.col("genericName") == "Omoedus")
                & (pl.col("specificEpithet") == "ephippigera")
            )
            .then(pl.lit("ephippiger"))
            .when(
                (pl.col("genericName") == "Byblis")
                & (pl.col("specificEpithet") == "kallartha")
            )
            .then(pl.lit("kallarthra"))
            .when(
                (pl.col("genericName") == "Alcockpenaeopsis")
                & (pl.col("specificEpithet") == "hungerfordi")
            )
            .then(pl.lit("hungerfordii"))
            .when(
                (pl.col("genericName") == "Pseudosesarma")
                & (pl.col("specificEpithet") == "edwardsi")
            )
            .then(pl.lit("edwardsii"))
            .when(
                (pl.col("genericName") == "Urocaridella")
                & (pl.col("specificEpithet") == "antonbruuni")
            )
            .then(pl.lit("antonbruunii"))
            .when(
                (pl.col("genericName") == "Ocypode")
                & (pl.col("specificEpithet") == "cordimanus")
            )
            .then(pl.lit("cordimana"))
            .otherwise(pl.col("specificEpithet"))
        )
        .with_columns(
            domain=pl.lit("Eukarya"),
            kingdom=pl.when((pl.col("phylum").is_in(["Ascomycota", "Basidiomycota"])))
            .then(pl.lit("fungi"))
            .when(pl.col("phylum") == "Cyanobacteria")
            .then(pl.lit("Bacteria"))
            .when(pl.col("phylum") == "Ciliophora")
            .then(pl.lit("Protista"))
            .otherwise(pl.lit("Animalia")),
        )
        .with_columns(
            taxonName=pl.col("taxonName")
            .str.replace_all("<i>", "")
            .str.replace_all("</i>", ""),
        )
        # Not working:
        # .with_columns(
        #     genus=pl.when(
        #         (pl.col("genus").is_null()) & (pl.col("specificEpithet").is_not_null())
        #     )
        #     .then(
        #         pl.col("taxonName")
        #         .str.split(" ")
        #         .list[0]
        #         .str.replace("<i>", "")
        #         .str.replace("</i>", "")
        #     )
        #     .otherwise("genus")
        # )
    )
    print("bos_df columns:", bos_df.columns)
    bos_df.write_csv("bos.csv")
    # print("bos speciesId", bos_df.filter(pl.col('taxonID').is_null()))

    _l = [
        "accepted",
        "synonym",
        "homotypic synonym",
        "proparte synonym",
        "heterotypic synonym",
    ]

    _c = [
        "taxonID",
        "genericName",
        "genus",
        "specificEpithet",
        "infraspecificEpithet",
        "taxonomicStatus",
        "acceptedNameUsageID",
        # "domain",
    ]
    taxon_ranked_only = (
        pl.scan_csv(
            "gbif/Taxon.tsv",
            separator="\t",
            quote_char=None,
            cache=True,
        )
        .filter(
            (pl.col("taxonRank") != "unranked") & (pl.col("taxonomicStatus").is_in(_l))
        )
        .select(_c)
        .filter((pl.col("genus") != "") & (pl.col("specificEpithet") != ""))
        .with_columns(
            infraspecificEpithet=pl.when((pl.col("infraspecificEpithet").is_null()))
            .then(pl.lit(""))
            .otherwise(pl.col("infraspecificEpithet"))
        )
    )

    # THIS BELOW DOESN"T WORK, taxon_ranked_only doesn't have taxon name
    # .with_columns(
    #     genus=pl.when(
    #         (pl.col("genus").is_null()) & (pl.col("specificEpithet").is_not_null())
    #     )
    #     .then(
    #         pl.col("taxonName")
    #         .str.split(" ")
    #         .list[0]
    #         .str.replace("<i>", "")
    #         .str.replace("</i>", "")
    #     )
    #     .otherwise("genus")
    # )

    print("taxon.csv columns,", taxon_ranked_only.collect_schema().keys())
    matching_df = (
        taxon_ranked_only.join(
            other=pl.LazyFrame(bos_df),
            on=["genericName", "specificEpithet", "infraspecificEpithet"],
            how="right",
        )
        .rename({"taxonID": "matched_taxonID"})
        .select(
            "speciesId",
            "matched_taxonID",
            "acceptedNameUsageID",
            "taxonName",
            "domain",
            "kingdom",
            "phylum",
            "class",
            "subclass",
            "superorder",
            "order",
            "sub-order",
            "infraorder",
            "section",
            "subsection",
            "superfamily",
            "family",
            "subfamily",
            "tribe",
            "genus",
            "subgenus",
            "genericName",
            "specificEpithet",
            "infraspecificEpithet",
        )
        .with_columns(
            pl.when(pl.col("infraspecificEpithet") == "")
            .then(pl.lit(None))
            .otherwise("infraspecificEpithet")
            .alias("infraspecificEpithet")
        )
    )

    # matching and contetnious split
    # What makes a data point contentious is where it has duplicate speciesId.

    matching_df = matching_df.filter(~pl.col("speciesId").is_duplicated()).with_columns(
        acceptedNameUsageID=pl.col("acceptedNameUsageID")
        .fill_null(pl.lit(None))
        .cast(pl.Int64)
    )
    contentious = matching_df.filter(
        (pl.col("speciesId").is_duplicated())
    ).with_columns(
        acceptedNameUsageID=pl.col("acceptedNameUsageID")
        .fill_null(pl.lit(None))
        .cast(pl.Int64)
    )

    unique_contentious = contentious.filter(
        (pl.col("acceptedNameUsageID").is_null())
        & (pl.col("matched_taxonID").is_in(pl.col("acceptedNameUsageID").implode()))
    )

    unique_contentius_speciesId = (
        unique_contentious.select("speciesId").collect().to_series().implode()
    )  # Just the speciesIds
    contentious = contentious.filter(
        ~pl.col("speciesId").is_in(unique_contentius_speciesId)
    )  # Removing...

    matching_df = pl.concat(
        [matching_df, unique_contentious],
    )

    return matching_df, contentious


if __name__ == "__main__":
    join()

import polars as pl


def join():
    taxon_lf = pl.scan_csv(
        "gbif/Taxon.tsv",
        separator="\t",
        quote_char=None,
        cache=True,
    ).filter((pl.col("genus") != "") & (pl.col("specificEpithet") != ""))
    bos_df = (
        pl.read_csv("outputsplit.csv")
        .rename(
            {
                "data cleanup changes": "data cleanup changes 1",
                "species": "specificEpithet",
                "subspecies": "infraspecificEpithet",
                "genus": "genericName",
                "domain ": "domain",
            }
        )
        .with_columns(pl.col(pl.String).str.strip_chars(" "))
    )
    # Taxonsml <- Taxonsml[
    #   !(Taxonsml$genus == "" | Taxonsml$specificEpithet == ""),
    # ]

    # null_counts_df = (
    #     bos_df.null_count()
    #     .transpose(include_header=True)
    #     .rename({"column": "feature", "column_0": "len"})
    # )
    # columns_to_drop_as_all_nulls = (
    #     null_counts_df.filter(null_counts_df["len"] == bos_df.shape[0])
    #     .transpose()[0, :]
    #     .transpose()
    #     .to_series()
    #     .to_list()
    # )
    # columns_to_drop_as_they_are_just_changes_logs = [
    #     "cleanup changes ",
    #     "Data cleanup changes",
    #     "data cleanup changes 1",
    #     "changes",
    #     "cleanup changes",
    #     "Unnamed: 18",
    #     "Unnamed: 17",
    #     "Unnamed: 19",
    #     "query",
    #     "query ",
    #     "issue",
    #     "cleanup changes/comments",
    #     "subphylum",
    # ]
    bos_df = (
        # (
        #     bos_df.drop(columns_to_drop_as_all_nulls)
        #     .drop(columns_to_drop_as_they_are_just_changes_logs)
        #     .select(~pl.selectors.starts_with("Unnamed"))
        # )
        bos_df.with_columns(
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
            pl.when(pl.col("infraspecificEpithet").is_null())
            .then(pl.lit(""))
            .otherwise(pl.col("infraspecificEpithet"))
        )
    )

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
    ]
    taxon_ranked_only = (
        taxon_lf.filter(
            (pl.col("taxonRank") != "unranked") & (pl.col("taxonomicStatus").is_in(_l))
        )
        .select(_c)
        .with_columns(
            pl.when(pl.col("infraspecificEpithet").is_null())
            .then(pl.lit(""))
            .otherwise(pl.col("infraspecificEpithet"))
        )
    )

    matching_df = (
        pl.LazyFrame(bos_df)
        .join(
            other=taxon_ranked_only,
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
            "specificEpithet",
            "infraspecificEpithet",
        )
    )
    return matching_df


# .sink_csv("matching_testing.csv",batch_size=50)

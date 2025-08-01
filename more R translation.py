import marimo

__generated_with = "0.14.15"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# DF""")
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from r_translation import join

    df = join()
    return df, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # matching and contentious split
    What makes a data point contentious is where it has duplicate speciesId.
    """
    )
    return


@app.cell
def _(df, pl):
    matching = df.filter(~pl.col("speciesId").is_duplicated()).with_columns(
        acceptedNameUsageID=pl.col("acceptedNameUsageID")
        .fill_null(pl.lit(-1))
        .cast(pl.Int64)
    )
    contentious = df.filter((pl.col("speciesId").is_duplicated())).with_columns(
        acceptedNameUsageID=pl.col("acceptedNameUsageID")
        .fill_null(pl.lit(-1))
        .cast(pl.Int64)
    )

    unique_contentious = contentious.filter(
        (pl.col("acceptedNameUsageID") == -1)
        & (
            pl.col("matched_taxonID").is_in(
                pl.col("acceptedNameUsageID").implode()
            )
        )
    )
    # print(
    #     unique_contentious.select(
    #         "speciesId", "acceptedNameUsageID", "matched_taxonID"
    #     ).collect()
    # )
    unique_contentius_speciesId = (
        unique_contentious.select("speciesId").collect().to_series().implode()
    )  # Just the speciesIds
    contentious2 = contentious.filter(
        ~pl.col("speciesId").is_in(unique_contentius_speciesId)
    )  # Removing...

    # print(contentious2.select('speciesId','acceptedNameUsageID','matched_taxonID').collect())

    matching = pl.concat(
        [matching, unique_contentious],
    )
    return contentious2, matching


@app.cell(disabled=True)
def _(contentious2):
    contentious2.collect().sort("speciesId")
    return


@app.cell(disabled=True)
def _(matching):
    matching.collect().sort("speciesId")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""nomatch lazyframe is for BOS data point that has no current match in gbif data set."""
    )
    return


@app.cell
def _(matching, pl):
    nomatch = (
        matching.filter(pl.col("matched_taxonID").is_null())
        .with_columns(
            taxonRank=pl.lit("BOSuncornirmedSpecies"),
            taxonomicStatus=pl.lit("BOSunformired"),
            parentNameUsageID=pl.lit(None),
        )
        .collect()
    )
    nomatch
    return (nomatch,)


@app.cell
def _(pl):
    accepted_taxon_lookup = (
        pl.scan_csv("gbif/Taxon.tsv", separator="\t", quote_char=None, cache=True)
        .filter(pl.col("taxonomicStatus") == pl.lit("accepted"))
        .filter(pl.col("kingdom").is_in(["Animalia", "Plantae"]))
        .filter(~pl.col("canonicalName").is_null())
        .filter(pl.col("canonicalName") != "")
        .sort("canonicalName")
    )
    return (accepted_taxon_lookup,)


@app.cell
def _(accepted_taxon_lookup):
    accepted_taxon_lookup.select("kingdom").unique().collect()
    return


@app.cell
def _():
    # accepted_taxon_lookup.filter(
    #     pl.col("canonicalName") == "Abakaniella"
    # ).collect()
    return


@app.cell
def _():
    # unique_accepted_taxons = accepted_taxon_lookup.filter(
    #     ~pl.col("canonicalName").is_duplicated()
    # )
    # unique_accepted_taxons.group_by("canonicalName").len().sort("len").collect()
    return


@app.cell
def _(accepted_taxon_lookup, pl):
    repeated_accepted_taxons = accepted_taxon_lookup.filter(
        pl.col("canonicalName").is_duplicated()
    )
    # repeated_accepted_taxons.group_by("canonicalName").len().sort("len").collect()
    return (repeated_accepted_taxons,)


@app.cell
def _(repeated_accepted_taxons):
    repeated_accepted_taxons.select("kingdom").unique().collect()
    return


@app.cell
def _(mo):
    mo.md(r"""### repeated_accepted_taxons = RAT""")
    return


@app.cell
def _():
    # repeated_accepted_taxons.collect_schema().keys()
    return


@app.cell
def _(pl, repeated_accepted_taxons):
    priority_columns = [
        "infraspecificEpithet",
        "specificEpithet",
        "genus",
        "family",
        "order",
        "class",
        "phylum",
        "kingdom",
        # "domain",
    ]

    _schema = {
        "feature_that_is_equal_to_canonicalName": pl.String,
        "matches": pl.String,
    }

    RAT_interim = pl.DataFrame(schema=_schema)
    for _c in priority_columns:
        _a = (
            repeated_accepted_taxons.filter(pl.col("canonicalName") == pl.col(_c))
            .select("canonicalName")
            .unique()
            .collect()
        )
        if _a.shape[0] != 0:
            _t = _a["canonicalName"].to_list()

            _row = pl.DataFrame(
                data={"feature_that_is_equal_to_canonicalName": _c, "matches": _t},
                schema=_schema,
            )
            RAT_interim = RAT_interim.vstack(_row)
    RAT_feats = (
        RAT_interim.group_by("matches")
        .agg(pl.col("feature_that_is_equal_to_canonicalName").str.join(", "))
        .with_columns(
            pl.col("feature_that_is_equal_to_canonicalName").str.split(", ")
        )
        .sort("feature_that_is_equal_to_canonicalName")
    )
    RAT_feats
    return RAT_feats, priority_columns


@app.cell
def _(accepted_taxon_lookup):
    accepted_taxon_lookup.columns
    return


@app.cell
def _(nomatch):
    nomatch.shape
    return


@app.cell
def _(nomatch):
    nomatch.describe()
    return


@app.cell
def _():
    bool("test")
    return


@app.cell
def _(RAT_feats, nomatch, pl, priority_columns, repeated_accepted_taxons):
    priorityFeatures = [
        "infraspecificEpithet",
        "specificEpithet",
        "genus",
        "family",
        "order",
        "class",
        "phylum",
        "kingdom",
    ]
    pl.Config.set_tbl_cols(1000)


    updated_nomatch = pl.DataFrame()
    all_taxon_data_to_be_selected_from = pl.DataFrame()
    # for _f in priorityFeatureso:

    i = 0
    for _f in ["family"]:
        for _m in RAT_feats["matches"]:
            _no_match_subset_to_update = nomatch.filter(pl.col(_f) == _m)
            if _no_match_subset_to_update.shape[0] == 0:
                continue

            # _no_match_subset_to_update.with_columns(parentNameUsageID = pl.col('taxon_id')) # THis is not correct
            taxon_data_to_select_from = (
                repeated_accepted_taxons.filter(pl.col("canonicalName") == _m)
                .select(["taxonID"] + priorityFeatures)
                .collect()
            )
            # print("selecting.....", taxon_data_to_select_from)
            selected_row = -1

            for i, _t in enumerate(taxon_data_to_select_from.iter_rows()):
                # print("row", i)
                prev_col_index = priority_columns.index(
                    _f
                )  # index of previous column of _f
                # print(prev_col_index)
                # raise Exception("stopped here")
                _x = _t[prev_col_index]
                if not bool(_x):
                    selected_row = i
                    break
            chosen_taxonId = taxon_data_to_select_from[selected_row, 0]

            # print("chosen_taxonId", chosen_taxonId)
            # break

            _no_match_subset_to_update = _no_match_subset_to_update.with_columns(
                parentNameUsageID=pl.lit(chosen_taxonId)
            )
            print("-------\nFeature:", _f, ",Name:", _m)

            print(
                "taxon_data_to_select_from\n",
                taxon_data_to_select_from,
            )
            print("parentNameUsageID chosen:", chosen_taxonId)
            print("no_match_subset_to_update to update:")
            print(
                _no_match_subset_to_update.select(
                    [
                        # "speciesId",
                        # "acceptedNameUsageID",
                        "taxonName",
                        # "taxonRank",
                        # "taxonomicStatus",
                    ]
                    + priorityFeatures
                )[0, :]
            )
            print("---------\n\n\n\n")
            # i+=1
            # if i ==4:
            #     raise Exception("stopped here")
            updated_nomatch = updated_nomatch.vstack(_no_match_subset_to_update)
            all_taxon_data_to_be_selected_from = (
                all_taxon_data_to_be_selected_from.vstack(
                    taxon_data_to_select_from
                )
            )
            # if _f == "order":
    updated_nomatch.write_csv("updated_nomatch.csv")
    all_taxon_data_to_be_selected_from.write_csv(
        "all_taxon_data_to_be_selected_from.csv"
    )
    return


if __name__ == "__main__":
    app.run()

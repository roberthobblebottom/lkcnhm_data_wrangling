import marimo

__generated_with = "0.14.16"
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

    matching, contentious = join()
    matching = matching.with_columns(
        genus=pl.when(
            (pl.col("genus").is_null()) & (pl.col("specificEpithet").is_not_null())
        )
        .then(
            pl.col("taxonName").str.split(" ").list[0]
            # .str.replace("<i>", "")
            # .str.replace("</i>", "")
        )
        .otherwise("genus")
    )
    return matching, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # matching and contentious split
    What makes a data point contentious is where it has duplicate speciesId.
    """
    )
    return


@app.cell(disabled=True)
def _():
    # matching = df.filter(~pl.col("speciesId").is_duplicated()).with_columns(
    #     acceptedNameUsageID=pl.col("acceptedNameUsageID")
    #     .fill_null(pl.lit(-1))
    #     .cast(pl.Int64)
    # )
    # contentious = df.filter((pl.col("speciesId").is_duplicated())).with_columns(
    #     acceptedNameUsageID=pl.col("acceptedNameUsageID")
    #     .fill_null(pl.lit(-1))
    #     .cast(pl.Int64)
    # )

    # unique_contentious = contentious.filter(
    #     (pl.col("acceptedNameUsageID") == -1)
    #     & (
    #         pl.col("matched_taxonID").is_in(
    #             pl.col("acceptedNameUsageID").implode()
    #         )
    #     )
    # )
    # # print(
    # #     unique_contentious.select(
    # #         "speciesId", "acceptedNameUsageID", "matched_taxonID"
    # #     ).collect()
    # # )
    # unique_contentius_speciesId = (
    #     unique_contentious.select("speciesId").collect().to_series().implode()
    # )  # Just the speciesIds
    # contentious2 = contentious.filter(
    #     ~pl.col("speciesId").is_in(unique_contentius_speciesId)
    # )  # Removing...

    # # print(contentious2.select('speciesId','acceptedNameUsageID','matched_taxonID').collect())

    # matching = pl.concat(
    #     [matching, unique_contentious],
    # )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""nomatch lazyframe is for BOS data point that has no current match in gbif data set."""
    )
    return


@app.cell
def _(matching, pl):
    debug = (
        matching.filter(
            (pl.col("specificEpithet").is_not_null()) & (pl.col("genus").is_null())
        )
        .select(
            [
                "speciesId",
                "taxonName",
                "infraspecificEpithet",
                "specificEpithet",
                "genus",
                "family",
                "order",
                "class",
                "phylum",
                "kingdom",
            ]
        )
        .collect()
    )
    debug
    # matching.filter(
    #     (pl.col("specificEpithet").is_not_null())
    #     & (
    #         pl.col("speciesId").is_in(
    #             debug.select(["speciesId"]).to_series().to_list()
    #         )
    #     )
    # ).select(
    #     [
    #         "taxonName",
    #     ]
    #     + priorityFeatures
    # ).collect()
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
    repeated_accepted_taxons = (
        pl.scan_csv("gbif/Taxon.tsv", separator="\t", quote_char=None, cache=True)
        .filter(pl.col("taxonomicStatus") == pl.lit("accepted"))
        .filter(pl.col("kingdom").is_in(["Animalia", "Plantae"]))
        .filter(~pl.col("canonicalName").is_null())
        .filter(pl.col("canonicalName") != "")
        .sort("canonicalName")
        .filter(pl.col("canonicalName").is_duplicated())
    )
    return (repeated_accepted_taxons,)


@app.cell
def _(mo):
    mo.md(r"""### repeated_accepted_taxons = RAT""")
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
    _r = (repeated_accepted_taxons).collect()
    RAT_interim = pl.DataFrame(schema=_schema)
    for _c in priority_columns:
        _a = (
            _r.filter(pl.col("canonicalName") == pl.col(_c))
            .select("canonicalName")
            .unique()
            # .collect()
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
    # RAT_feats.write_csv("RAT_feats.csv")
    return RAT_feats, priority_columns


@app.cell
def _(RAT_feats, nomatch, pl, priority_columns, repeated_accepted_taxons):
    pl.Config.set_tbl_cols(1000)

    updated_nomatch = []
    all_taxon_data_to_be_selected_from = []
    _collected_repeated_taxons = repeated_accepted_taxons.collect()
    # for _f in ["family"]:
    for _f in priority_columns:
        for _m in RAT_feats["matches"]:
            if _m not in nomatch[_f].to_list():
                continue

            taxon_data_to_select_from = _collected_repeated_taxons.filter(
                pl.col("canonicalName") == _m
            ).select(["taxonID"] + priority_columns)
            selected_row = -1

            for i, _t in enumerate(taxon_data_to_select_from.iter_rows()):
                prev_col_index = priority_columns.index(
                    _f
                )  # index of previous column of _f
                _x = _t[prev_col_index]
                if not bool(_x):
                    selected_row = i  # Since there are only two rows from taxon_data_t0_select_from, the results is either 0 or 1
                    break
            chosen_taxonId = taxon_data_to_select_from[selected_row, 0]
            other_taxonId = taxon_data_to_select_from[int(~bool(selected_row)), 0]

            _no_match_subset_to_update = nomatch.filter(
                pl.col(_f) == _m
            ).with_columns(
                parentNameUsageID=pl.when(
                    (pl.col("infraspecificEpithet").is_null())
                    & (pl.col("specificEpithet").is_not_null())
                    & (pl.col("genus").is_not_null())
                )
                .then(pl.lit(other_taxonId))
                .otherwise(pl.lit(chosen_taxonId))
            )

            # Debugging
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
                        "parentNameUsageID",
                        "taxonName",
                    ]
                    + priority_columns
                )
            )
            print("---------\n\n\n\n")
            updated_nomatch.append(_no_match_subset_to_update)
            all_taxon_data_to_be_selected_from.append(taxon_data_to_select_from)


    pl.concat(updated_nomatch, rechunk=True, parallel=True).write_csv(
        "updated_nomatch.csv"
    )
    pl.concat(
        all_taxon_data_to_be_selected_from, rechunk=True, parallel=True
    ).write_csv("all_taxon_data_to_be_selected_from.csv")
    return


if __name__ == "__main__":
    app.run()

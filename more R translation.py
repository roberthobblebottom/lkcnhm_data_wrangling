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
        .then(pl.col("taxonName").str.split(" ").list[0])
        .otherwise("genus")
    )
    return contentious, matching, mo, pl


@app.cell
def _(contentious):
    contentious.collect_schema().keys()
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
        .fill_null("")  # TODO TEST IF NOT WORK, REMOVE
        .collect()
    )
    nomatch
    return (nomatch,)


@app.cell
def _(matching, pl):
    matching_with_populated_match_taxonID = matching.filter(
        pl.col("matched_taxonID").is_not_null()
    ).with_columns(
        taxonRank=pl.lit(None),
        taxonomicStatus=pl.lit(None),
        parentNameUsageID=pl.lit(None),
    )
    return (matching_with_populated_match_taxonID,)


@app.cell
def _(mo):
    mo.md(r"""# repeated_accepted_taxons = RAT""")
    return


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
def _(RAT_feats, pl):
    RAT_feats.filter(pl.col("matches") == "Tentaculata")
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    # matching attempt

    ### Still_no_match is not comprehensive, doesn't includes subset  with some matches and some without
    """
    )
    return


@app.cell
def _(RAT_feats, nomatch, pl, priority_columns, repeated_accepted_taxons):
    updated_to_matching = []
    all_taxon_data_to_be_selected_from = []
    still_no_match = []
    _collected_repeated_taxons = repeated_accepted_taxons.collect().fill_null("")
    _reversed_priority_columns = priority_columns.copy()
    _reversed_priority_columns.reverse()
    for _f in _reversed_priority_columns:
        to_skip = False
        for _m in RAT_feats["matches"]:
            # skipping those that are not in RAT_feats
            if _m not in nomatch[_f].to_list():
                continue

            print("-------\nFeature:", _f, ",Name:", _m)

            taxon_data_to_select_from = _collected_repeated_taxons.filter(
                pl.col("canonicalName") == _m
            ).select(["taxonID"] + _reversed_priority_columns)

            # Select from the two rows what taxon data to select.
            selected_row = -1
            for i, _t in enumerate(
                taxon_data_to_select_from.select(
                    ["taxonID"] + priority_columns
                ).iter_rows()
            ):
                col_index = priority_columns.index(_f)
                _x = _t[col_index]
                if not bool(_x):
                    selected_row = i  # Since there are only two rows from taxon_data_to_select_from, the results is either 0 or 1
                    break
            assert selected_row != -1

            # has_predicament1 is when kingdom to genus all has values and it is kind of leveled between the two rows of the taxon data.
            has_predicament1 = False
            _l = taxon_data_to_select_from["family"].to_list()
            if _f == "genus" and _l[0] != None and _l[1] != None:
                print("same level detected")
                has_predicament1 = True

            chosen_taxonId = taxon_data_to_select_from[selected_row, 0]
            other_taxonId = taxon_data_to_select_from[
                int(not bool(selected_row)), 0
            ]

            # Getting the _no_match_subset_to_update section
            _no_match_subset_to_update = nomatch.filter(
                pl.col(_f) == _m,
            )

            _temp2 = _reversed_priority_columns[:-3]
            features_to_find_nulls = (
                _temp2[_reversed_priority_columns.index(_f) + 1]
                if len(_temp2) > _reversed_priority_columns.index(_f) + 1
                else None
            )
            if features_to_find_nulls is not None:
                print("features_to_find_nulls", features_to_find_nulls)
                _no_match_subset_to_update = _no_match_subset_to_update.filter(
                    pl.col(features_to_find_nulls) == ""
                )

                if _no_match_subset_to_update.is_empty():
                    # If not empty parent id will still be filled below
                    still_no_match_subset = nomatch.filter(
                        pl.col(_f) == _m,
                    ).with_columns(
                        current_feature=pl.lit(_f), current_name=pl.lit(_m)
                    )
                    still_no_match.append(still_no_match_subset)
                    print(
                        "\n\n\n\n\nskipping",
                        _f,
                        _m,
                        "\n this _no_match_subset_to_update dataframe doesn't have the same tax rank level\n",
                        still_no_match_subset.select(
                            # [
                            #     "parentNameUsageID",
                            #     "taxonName",
                            # ]
                            # +
                            _reversed_priority_columns
                        ),
                    )
                    # print(
                    #     "taxon_data_to_select_from\n",
                    #     taxon_data_to_select_from,
                    # )
                    print("---------")
                    continue

            # Settleing predicament one
            # this predicament thing should only happen when the subset is of only 1 row.
            if has_predicament1 and _no_match_subset_to_update.shape[0] == 1:
                match = True
                for _f1 in ["class", "order", "family"]:
                    match *= (
                        _no_match_subset_to_update[_f1].item()
                        == taxon_data_to_select_from[selected_row, :][_f1].item()
                    )
                if not match:
                    match = True
                    for _f1 in ["class", "order", "family"]:
                        match *= (
                            _no_match_subset_to_update[_f1].item()
                            == taxon_data_to_select_from[
                                int(not bool(selected_row)), :
                            ][_f1].item()
                        )
                    assert match == True
                    print("changing chosen taxonId")
                    temp = chosen_taxonId
                    chosen_taxonId = other_taxonId
                    other_taxonId = temp

            # turning these rows to matching by filling parentNameUsageID
            _no_match_subset_to_update = _no_match_subset_to_update.with_columns(
                parentNameUsageID=pl.when(
                    (pl.col("infraspecificEpithet").is_null())
                    & (pl.col("specificEpithet").is_not_null())
                    & (pl.col("genus").is_not_null())
                )
                .then(pl.lit(other_taxonId))
                .otherwise(pl.lit(chosen_taxonId))
            )

            # Debugging for matching data from gbif to new matched rows
            print(
                "taxon_data_to_select_from\n",
                taxon_data_to_select_from,
            )
            print("parentNameUsageID chosen:", chosen_taxonId)
            print("no_match_subset_to_update ")
            print(
                _no_match_subset_to_update.select(
                    # [
                    #     "parentNameUsageID",
                    #     "taxonName",
                    # ]
                    # +
                    _reversed_priority_columns
                )
            )
            print("---------")

            updated_to_matching.append(_no_match_subset_to_update)
            all_taxon_data_to_be_selected_from.append(taxon_data_to_select_from)

    # turn them into dataframes and writing to csv files
    updated_to_matching = pl.concat(
        updated_to_matching, rechunk=True, parallel=True
    )
    updated_to_matching.write_csv("updated_to_matching.csv")
    all_taxon_data_to_be_selected_from = pl.concat(
        all_taxon_data_to_be_selected_from, rechunk=True, parallel=True
    )
    all_taxon_data_to_be_selected_from.write_csv(
        "all_taxon_data_to_be_selected_from.csv"
    )
    still_no_match = pl.concat(still_no_match, rechunk=True, parallel=True)
    still_no_match.write_csv("still_no_match.csv")
    return still_no_match, updated_to_matching


@app.cell
def _(mo):
    mo.md(
        r"""there is rows that are totally skipped and those that are partially skipped or i call it as left out.."""
    )
    return


@app.cell
def _(matching_with_populated_match_taxonID, pl, updated_to_matching):
    matching2 = pl.concat(
        [updated_to_matching, matching_with_populated_match_taxonID.collect()]
    )
    matching2.write_csv("matching2.csv")
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # still no match stuff


    """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## `current_feature` uniques""")
    return


@app.cell
def _(still_no_match):
    still_no_match["current_feature"].unique()
    return


@app.cell
def _(mo):
    mo.md(r"""## `current_name` uniques""")
    return


@app.cell
def _(still_no_match):
    still_no_match["current_name"].unique()
    return


@app.cell
def _(mo):
    mo.md(r"""## number of rows in still no match""")
    return


@app.cell
def _(still_no_match):
    still_no_match.shape[0]
    return


@app.cell
def _(mo):
    mo.md(r"""## stop at class, there is only current_feature = phylum matches""")
    return


@app.cell
def _(pl, priority_columns, still_no_match):
    debug1 = still_no_match.select(
        [
            "current_feature",
            "current_name",
            # "parentNameUsageID",
            "taxonName",
        ]
        + priority_columns
    )

    debug1.filter(pl.col("order") == "", pl.col("current_feature") == "phylum")
    return (debug1,)


@app.cell
def _(mo):
    mo.md(r"""### class names and phylum names:""")
    return


@app.cell
def _(debug1, pl):
    debug1.filter(pl.col("order") == "", pl.col("current_feature") == "phylum").select(
        'class','phylum'
    ).group_by( 'class','phylum').len()
    return


@app.cell
def _(mo):
    mo.md(r"""## stops at order there is only current_feature = phylum matches""")
    return


@app.cell
def _(debug1, pl):
    debug1.filter(
        pl.col("family") == "",
        pl.col("order") != "",
        pl.col("genus") == "",
        pl.col("current_feature") == "phylum",
    )
    return


@app.cell
def _(mo):
    mo.md(r"""### order names when current name = Arthropoda:""")
    return


@app.cell(hide_code=True)
def _(debug1, pl):
    # current Name:
    debug1.filter(
        pl.col("family") == "",
        pl.col("order") != "",
        pl.col("genus") == "",
        pl.col("current_feature") == "phylum",
    )["current_name"].unique().to_list()
    return


@app.cell
def _(debug1, pl):
    debug1.filter(
        pl.col("family") == "",
        pl.col("order") != "",
        pl.col("genus") == "",
        pl.col("current_feature") == "phylum",
    )["order"].unique().to_list()
    return


@app.cell
def _(mo):
    mo.md(
        r"""## stops at family, current_feature contains both phylum and order"""
    )
    return


@app.cell
def _(debug1, pl):
    debug1.filter(
        pl.col("family") != "",
        pl.col("genus") == "",
        pl.col("current_feature") == "phylum",
    )
    return


@app.cell
def _(mo):
    mo.md(r"""### family names where current_feature = phylum""")
    return


@app.cell
def _(debug1, pl):
    debug1.filter(
        pl.col("family") != "",
        pl.col("genus") == "",
        pl.col("current_feature") == "phylum",
    )["family"].unique().to_list()
    return


@app.cell
def _(mo):
    mo.md(r"""### current names where current_feature = phylum:""")
    return


@app.cell
def _(debug1, pl):
    debug1.filter(
        pl.col("family") != "",
        pl.col("genus") == "",
        pl.col("current_feature") == "phylum",
    )["current_name"].unique().to_list()
    return


@app.cell
def _(mo):
    mo.md(
        r"""## Stops at one of the epithet, current_feature contains both phylum and order"""
    )
    return


@app.cell
def _(debug1, pl):
    debug1.filter(pl.col("genus") != "", pl.col("current_feature") == "phylum")
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ### specificEpithet names when current_feature = phylum and current_name = "Arthropoda"
    """
    )
    return


@app.cell
def _(mo):
    mo.md(r"""#### current_name when current_feature = phylum""")
    return


@app.cell
def _(debug1, pl):
    debug1.filter(pl.col("genus") != "", pl.col("current_feature") == "phylum")[
        "current_name"
    ].unique().to_list()
    return


@app.cell
def _(debug1, pl):
    debug1.filter(pl.col("genus") != "", pl.col("current_feature") == "phylum")[
        "specificEpithet"
    ].unique().to_list()
    return


@app.cell
def _(mo):
    mo.md(r"""## when current_feature = order""")
    return


@app.cell
def _(debug1, pl):
    debug1.filter(pl.col("current_feature") == "order")
    return


@app.cell
def _(mo):
    mo.md(r"""## Tentaculata Ctenophora""")
    return


@app.cell
def _(pl, repeated_accepted_taxons):
    repeated_accepted_taxons.filter(
        pl.col("canonicalName") == "Tentaculata"
    ).collect()
    return


@app.cell
def _(pl, repeated_accepted_taxons):
    repeated_accepted_taxons.filter(
        pl.col("canonicalName") == "Tentaculates"
    ).collect()
    return


@app.cell
def _(pl, priority_columns, still_no_match):
    still_no_match.select(
        [
            "current_feature",
            "current_name",
            "parentNameUsageID",
            "taxonName",
        ]
        + priority_columns
    ).filter(pl.col("class") == "Tentaculata", pl.col("phylum") == "Ctenophora")
    return


@app.cell
def _(pl, priority_columns, repeated_accepted_taxons):
    repeated_accepted_taxons.filter(pl.col("class") == "Tentaculata").select(
        priority_columns
    ).collect()
    return


@app.cell
def _(pl, priority_columns, repeated_accepted_taxons):
    repeated_accepted_taxons.filter(pl.col("family") == "Coeloplanidae").select(
        priority_columns
    ).collect()
    return


if __name__ == "__main__":
    app.run()

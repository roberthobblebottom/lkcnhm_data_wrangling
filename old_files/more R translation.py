import marimo

__generated_with = "0.15.2"
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


@app.cell
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
        .fill_null("")
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
    return


@app.cell(hide_code=True)
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
    for _col in priority_columns:
        _canonical_names = (
            (
                _r.filter(pl.col("canonicalName") == pl.col(_col))
                .select("canonicalName")
                .unique()
            )
            .to_series()
            .to_list()
        )
        if len(_canonical_names) != 0:
            _row = pl.DataFrame(
                data={
                    "feature_that_is_equal_to_canonicalName": _col,
                    "matches": _canonical_names,
                },
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
    return RAT_feats, priority_columns


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# matching attempt""")
    return


@app.cell
def _(RAT_feats):
    RAT_feats
    return


@app.cell
def _(RAT_feats, nomatch, pl, priority_columns, repeated_accepted_taxons):
    updated_to_matching = []
    # all_taxon_data_to_be_selected_from = []
    still_no_match = []
    _collected_repeated_taxons = repeated_accepted_taxons.collect().fill_null("")
    _reversed_priority_columns = priority_columns.copy()
    _reversed_priority_columns.reverse()
    for _col in _reversed_priority_columns:
        for _match in RAT_feats["matches"]:
            # skipping those that are not in RAT_feats
            if _match not in nomatch[_col].to_list():
                continue

            print("-------\nFeature:", _col, ",Name:", _match)

            taxon_data_to_select_from = _collected_repeated_taxons.filter(
                pl.col("canonicalName") == _match
            ).select(["taxonID"] + _reversed_priority_columns)

            # Select from the two rows what taxon data to select.
            selected_row_int = -1
            for i, _tuple in enumerate(
                taxon_data_to_select_from.select(
                    ["taxonID"] + priority_columns
                ).iter_rows()
            ):
                col_index = priority_columns.index(_col)
                _x = _tuple[col_index]
                if not bool(_x):
                    selected_row_int = i  # Since there are usually only two rows from taxon_data_to_select_from, the results is either 0 or 1
                    break
            assert selected_row_int != -1

            # has_predicament1 is when kingdom to genus all has values and it is kind of leveled between the two rows of the taxon data.
            has_predicament1 = False
            _l = taxon_data_to_select_from["family"].to_list()
            if _col == "genus" and _l[0] != None and _l[1] != None:
                print("same level detected")
                has_predicament1 = True

            chosen_taxonId = taxon_data_to_select_from[selected_row_int, 0]
            other_taxonId = taxon_data_to_select_from[
                int(not bool(selected_row_int)), 0
            ]

            # Getting the _no_match_subset_to_update section
            _no_match_subset_to_update = nomatch.filter(
                pl.col(_col) == _match,
            )
            _x = _reversed_priority_columns[:-3]
            feature_to_find_nulls = (
                _x[_reversed_priority_columns.index(_col) + 1]
                if len(_x) > _reversed_priority_columns.index(_col) + 1
                else None
            )  # just the next feature to find null
            if feature_to_find_nulls is not None:
                print("feature_to_find_nulls", feature_to_find_nulls)
                _no_match_subset_to_update = _no_match_subset_to_update.filter(
                    pl.col(feature_to_find_nulls) == ""
                )
                if _no_match_subset_to_update.is_empty():
                    still_no_match_subset = nomatch.filter(
                        pl.col(_col) == _match,
                    ).with_columns(
                        current_feature=pl.lit(_col), current_name=pl.lit(_match)
                    )
                    still_no_match.append(still_no_match_subset)
                    print(
                        "\n\n\n\n\nskipping",
                        _col,
                        _match,
                        "\n this _no_match_subset_to_update dataframe doesn't have the same tax rank level\n",
                        still_no_match_subset.select(_reversed_priority_columns),
                    )
                    print("---------")
                    continue

            # Settling predicament one
            # this predicament thing should only happen when the subset is of only 1 row.
            if has_predicament1 and _no_match_subset_to_update.shape[0] == 1:
                match = True
                for _col1 in ["class", "order", "family"]:
                    match *= (
                        _no_match_subset_to_update[_col1].item()
                        == taxon_data_to_select_from[selected_row_int, :][
                            _col1
                        ].item()
                    )
                if not match:
                    match = True
                    for _col1 in ["class", "order", "family"]:
                        match *= (
                            _no_match_subset_to_update[_col1].item()
                            == taxon_data_to_select_from[
                                int(not bool(selected_row_int)), :
                            ][_col1].item()
                        )  # searching in the three columns of interests for matches
                    assert match == True  # Double check that there is no unmatch.
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
            print(_no_match_subset_to_update.select(_reversed_priority_columns))
            print("---------")

            updated_to_matching.append(_no_match_subset_to_update)
            # all_taxon_data_to_be_selected_from.append(taxon_data_to_select_from)

    # turn them into dataframes and writing to csv files
    updated_to_matching = pl.concat(
        updated_to_matching, rechunk=True, parallel=True
    )
    updated_to_matching.write_csv("updated_to_matching.csv")
    updated_to_matching.write_csv("first_matches_set_from_wrangling.csv")

    # still_no_match = nomatch.join(
    #     updated_to_matching.select("speciesId"), on="speciesId", how="anti"
    # )
    # print(still_no_match.shape)
    # all_taxon_data_to_be_selected_from = pl.concat(
    #     all_taxon_data_to_be_selected_from, rechunk=True, parallel=True
    # )
    # all_taxon_data_to_be_selected_from.write_csv(
    #     "all_taxon_data_to_be_selected_from.csv"
    # )
    still_no_match = pl.concat(still_no_match, rechunk=True, parallel=True)
    still_no_match = still_no_match.join(
        updated_to_matching.select("speciesId"), on="speciesId", how="anti"
    )
    # still_no_match.write_csv("still_no_match.csv")
    return still_no_match, updated_to_matching


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## still no match stuff""")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### `current_feature` uniques""")
    return


@app.cell
def _(still_no_match):
    still_no_match["current_feature"].unique()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### `current_name` uniques""")
    return


@app.cell
def _(still_no_match):
    still_no_match["current_name"].unique()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## number of rows in still no match""")
    return


@app.cell
def _(still_no_match):
    still_no_match.shape[0]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""# second matching wrangle of new `Taxons` df with `still_no_match`"""
    )
    return


@app.cell
def _(pl, priority_columns):
    taxons = (
        pl.scan_csv("gbif/Taxon.tsv", separator="\t", quote_char=None, cache=True)
        .filter(
            pl.col("taxonomicStatus") == pl.lit("accepted"),
            pl.col("taxonRank") != "unranked",
        )
        .filter(pl.col("kingdom").is_in(["Animalia", "Plantae"]))
        .select(["taxonID"] + priority_columns)
    )
    taxons.collect().shape
    return (taxons,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## stop at class, there is only current_feature = phylum matches""")
    return


@app.cell
def _(priority_columns):
    priority_columns
    return


@app.cell
def _(pl, still_no_match, taxons):
    ## stop at class, there is only current_feature = phylum matches
    _filter = (
        pl.col("class") != "",
        pl.col("order") == "",
        pl.col("family") == "",
        pl.col("genus") == "",
        pl.col("specificEpithet") == "",
        pl.col("infraspecificEpithet") == "",
        pl.col("current_feature") == "phylum",
    )
    match_on_class = still_no_match.with_columns(
        name_to_match=pl.when(_filter).then("class").otherwise(None)
    ).filter(_filter)

    _x1 = (
        still_no_match.filter(_filter)
        .select("class", "phylum")
        .group_by("class", "phylum")
        .len()
    )

    _taxons_subset = taxons.filter(
        pl.col("family").is_null(),
        pl.col("order").is_null(),
        pl.col("genus").is_null(),
        pl.col("class").is_in(_x1["class"].unique()),
        pl.col("specificEpithet").is_null(),
        pl.col("infraspecificEpithet").is_null(),
        pl.col("phylum").is_in(_x1["phylum"].unique()),
    ).select("taxonID", "class")
    match_on_class = (
        pl.LazyFrame(match_on_class)
        .join(
            _taxons_subset,
            on="class",
        )
        .with_columns(
            parentNameUsageID=pl.when(pl.col("taxonID").is_not_null())
            .then("taxonID")
            .otherwise("parentNameUsageID")
        )
        .drop("taxonID")
    ).collect()

    match_on_class.shape
    return (match_on_class,)


@app.cell
def _(match_on_class):
    match_on_class.group_by("parentNameUsageID", "name_to_match").len()
    return


@app.cell
def _(match_on_class):
    match_on_class.shape
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## stops at order there is only current_feature = phylum matches""")
    return


@app.cell
def _(pl, still_no_match, taxons):
    ## stops at order there is only current_feature = phylum matches
    _filter = (
        pl.col("class") != "",
        pl.col("order") != "",
        pl.col("family") == "",
        pl.col("genus") == "",
        pl.col("specificEpithet") == "",
        pl.col("infraspecificEpithet") == "",
        pl.col("current_feature") == "phylum",
    )
    match_on_order = still_no_match.with_columns(
        name_to_match=pl.when(_filter).then("order").otherwise(None)
    ).filter(_filter)
    _x1 = (
        still_no_match.filter(_filter)
        .select("order", "phylum")
        .group_by("order", "phylum")
        .len()
    )

    _taxons_subset = (
        taxons.filter(
            pl.col("family").is_null(),
            pl.col("order").is_in(_x1["order"].unique().to_list()),
            pl.col("genus").is_null(),
            pl.col("specificEpithet").is_null(),
            pl.col("infraspecificEpithet").is_null(),
            pl.col("phylum").is_in(_x1["phylum"].unique().to_list()),
        )
        .select("taxonID", "order")
        .collect()
    )
    match_on_order = (
        match_on_order.join(
            _taxons_subset,
            on="order",
        )
        .with_columns(
            parentNameUsageID=pl.when(pl.col("taxonID").is_not_null())
            .then("taxonID")
            .otherwise("parentNameUsageID")
        )
        .drop("taxonID")
    )
    match_on_order.shape
    return (match_on_order,)


@app.cell
def _(match_on_order):
    match_on_order.group_by("parentNameUsageID", "name_to_match").len()
    return


@app.cell
def _(match_on_order):
    match_on_order.shape
    return


@app.cell
def _(mo):
    mo.md(
        r"""## stops at family, current_feature contains both phylum and order"""
    )
    return


@app.cell
def _(pl, still_no_match, taxons):
    ## stops at family there is only current_feature = phylum matches
    _filter = (
        pl.col("class") != "",
        pl.col("order") != "",
        pl.col("family") != "",
        pl.col("genus") == "",
        pl.col("specificEpithet") == "",
        pl.col("infraspecificEpithet") == "",
    )
    match_on_family = still_no_match.with_columns(
        name_to_match=pl.when(_filter).then("family").otherwise(None)
    ).filter(_filter)
    _x1 = (
        still_no_match.filter(_filter)
        .select("family", "phylum", "order")
        .group_by("family", "phylum", "order")
        .len()
    )

    _taxons_subset = (
        taxons.filter(
            pl.col("family").is_in(_x1["family"].unique().to_list()),
            pl.col("genus").is_null(),
            pl.col("specificEpithet").is_null(),
            pl.col("infraspecificEpithet").is_null(),
            pl.col("phylum").is_in(_x1["phylum"].unique().to_list()),
            pl.col("order").is_in(_x1["order"].unique().to_list()),
        )
        .select("taxonID", "family")
        .collect()
    )
    match_on_family = (
        match_on_family.join(
            _taxons_subset,
            on="family",
        )
        .with_columns(
            parentNameUsageID=pl.when(pl.col("taxonID").is_not_null())
            .then("taxonID")
            .otherwise("parentNameUsageID")
        )
        .drop("taxonID")
    )
    match_on_family.shape
    return (match_on_family,)


@app.cell
def _(match_on_family, pl):
    match_on_family.group_by(
        "parentNameUsageID", "name_to_match", "current_feature"
    ).len().filter(pl.col("current_feature") == "phylum")
    return


@app.cell
def _(match_on_family, pl):
    match_on_family.group_by(
        "parentNameUsageID", "name_to_match", "current_feature"
    ).len().filter(pl.col("current_feature") == "order")
    return


@app.cell
def _(match_on_family):
    match_on_family.shape
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## genus  

    There is nothing for rank genus.
    """
    )
    return


@app.cell
def _(pl, still_no_match):
    _filter = (
        pl.col("class") != "",
        pl.col("order") != "",
        pl.col("family") != "",
        pl.col("genus") != "",
        pl.col("specificEpithet") == "",
        pl.col("infraspecificEpithet") == "",
    )
    _g = still_no_match.filter(_filter)
    _g
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## stops at specificEpithet there is only current_feature = phylum matches 

    ### something is wrong with some specificEpithet and infraSpecificEpithet for both pl.col('current_feature').is_in(['phylum','order']) cant be found in gbif.
    """
    )
    return


@app.cell
def _(pl, still_no_match, taxons):
    ## stops at family there is only current_feature = phylum matches
    _filter = (
        pl.col("class") != "",
        pl.col("order") != "",
        pl.col("family") != "",
        pl.col("genus") != "",
        pl.col("specificEpithet") != "",
        pl.col("infraspecificEpithet") == "",
    )

    to_be_match_on_specificEpithet = still_no_match.filter(_filter)

    _x1 = (
        still_no_match.filter(_filter)
        .select(
            "specificEpithet",
            "genus",
        )
        .group_by(
            "specificEpithet",
            "genus",
        )
        .len()
    )

    _taxons_subset = (
        taxons.filter(
            pl.col("specificEpithet").is_in(
                _x1["specificEpithet"].unique().to_list()
            ),
            pl.col("genus").is_in(_x1["genus"].unique().to_list()),
        )
        .select(
            "taxonID",
            "specificEpithet",
            "genus",
        )
        .collect()
    )

    # TAXON.TSV doesn't have pl.col("genus")=='Dicaeum',pl.col('specificEpithet')=='chysorrheum'
    match_on_specificEpithet = (
        to_be_match_on_specificEpithet.join(
            _taxons_subset,
            on=["genus", "specificEpithet"],
            # how="left"
        )
        .with_columns(
            parentNameUsageID=pl.when(pl.col("taxonID").is_not_null())
            .then("taxonID")
            .otherwise("parentNameUsageID")
        )
        .drop("taxonID")
    )
    match_on_specificEpithet.shape
    # match_on_specificEpithet.write_csv("matching_attempt_on_specificEpithet.csv")
    return (match_on_specificEpithet,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## infraspecificepithet

    different filter because some don't have specificEpithet either, if I am not wrong
    """
    )
    return


@app.cell
def _(pl, still_no_match, taxons):
    # InfraspecificEpithet

    ## stops at family there is only current_feature = phylum matches
    _filter = (
        pl.col("class") != "",
        pl.col("order") != "",
        pl.col("family") != "",
        pl.col("genus") != "",
        pl.col("infraspecificEpithet") != "",
    )  # different filter because some don't have specificEpithet either, if I am not wrong
    to_be_match_on_infraspecificEpithet = still_no_match.filter(_filter)

    _x1 = (
        still_no_match.filter(_filter)
        .select(
            "infraspecificEpithet",
            "genus",
        )
        .group_by(
            "infraspecificEpithet",
            "genus",
        )
        .len()
    )

    _taxons_subset = (
        taxons.filter(
            pl.col("infraspecificEpithet").is_in(
                _x1["infraspecificEpithet"].unique().to_list()
            ),
            pl.col("genus").is_in(_x1["genus"].unique().to_list()),
        )
        .select(
            "taxonID",
            "infraspecificEpithet",
            "genus",
        )
        .collect()
    )


    # TAXON.TSV doesn't have pl.col("genus")=='Dicaeum',pl.col('specificEpithet')=='chysorrheum'
    match_on_infraspecificEpithet = (
        to_be_match_on_infraspecificEpithet.join(
            _taxons_subset,
            on=["genus", "infraspecificEpithet"],
        )
        .with_columns(
            parentNameUsageID=pl.when(pl.col("taxonID").is_not_null())
            .then("taxonID")
            .otherwise("parentNameUsageID")
        )
        .drop("taxonID")
    )
    # print(match_on_infraspecificEpithet.select(priority_columns))
    return (match_on_infraspecificEpithet,)


@app.cell
def _(mo):
    mo.md(
        r"""
    #combining updated parent id rows

    todo: specificEpithet infraSpecificepithet
    """
    )
    return


@app.cell
def _(match_on_class):
    match_on_class.columns
    return


@app.cell
def _(
    match_on_class,
    match_on_family,
    match_on_infraspecificEpithet,
    match_on_order,
    match_on_specificEpithet,
    nomatch,
    pl,
    updated_to_matching,
):
    _to_drop = ["current_feature", "current_name", "name_to_match"]
    _to_drop2 = ["current_feature", "current_name"]
    _new_match = pl.concat(
        [
            match_on_class.drop(_to_drop),
            match_on_order.drop(_to_drop),
            match_on_family.drop(_to_drop),
            match_on_specificEpithet.drop(_to_drop2),
            match_on_infraspecificEpithet.drop(_to_drop2),
        ]
    )
    _new_match.write_csv("second_matches_set_from_wrangling.csv")
    print("second match set shape", _new_match.shape)
    print()
    _new_match = pl.concat(
        [
            _new_match,
            updated_to_matching.with_columns(
                pl.col("parentNameUsageID").cast(pl.Int64)
            ),
        ]
    )
    _new_match.write_csv("updated_to_matching.csv")
    print("shape of all matches:", _new_match.shape[0])
    # print("new match unique speciesId",_new_match.select("speciesId").unique())
    print("shape of nomatch before any matches", nomatch.shape[0])
    _nomatch = nomatch.join(
        _new_match.select("speciesId"), on="speciesId", how="anti"
    )
    _nomatch.write_csv("no_match.csv")
    print(_nomatch.shape)
    return


@app.cell
def _():
    5943 + 2502
    return


if __name__ == "__main__":
    app.run()

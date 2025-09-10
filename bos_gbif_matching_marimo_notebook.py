import marimo

__generated_with = "0.15.2"
app = marimo.App(width="full")


@app.cell
def _(mo):
    mo.md(
        r"""
    # Introduction  

    combination of r_translation.py script and more_r_translation.py marimo notebook. these olds scripts are under `old_files`

    for the second matching wrangle, the parts for specificEpithet and infraSpecificEpithet doesn't match with many results.

    # Imports and Initial Preprocessing
    """
    )
    return


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _(pl):
    bos = (
        pl.read_csv("bos.csv")
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
    return (bos,)


@app.cell
def _(bos):
    null_counts = (
        bos.null_count()
        .transpose(include_header=True)
        .rename({"column": "feature", "column_0": "len"})
    )
    columns_to_drop_as_all_nulls = (
        null_counts.filter(null_counts["len"] == bos.shape[0])
        .transpose()[0, :]
        .transpose()
        .to_series()
        .to_list()
    )
    return (columns_to_drop_as_all_nulls,)


@app.cell
def _():
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
    return (columns_to_drop_as_they_are_just_changes_logs,)


@app.cell
def _(
    bos,
    columns_to_drop_as_all_nulls,
    columns_to_drop_as_they_are_just_changes_logs,
    pl,
):
    bos_cleaned = (
        (
            bos.drop(columns_to_drop_as_all_nulls)
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
            kingdom=pl.when(
                (pl.col("phylum").is_in(["Ascomycota", "Basidiomycota"]))
            )
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
    )
    return (bos_cleaned,)


@app.cell
def _(pl):
    _filter = pl.col("taxonomicStatus").is_in(
        [
            "accepted",
            "synonym",
            "homotypic synonym",
            "proparte synonym",
            "heterotypic synonym",
        ]
    )
    taxon_ranked_only = (
        pl.scan_csv(
            "gbif/Taxon.tsv",
            separator="\t",
            quote_char=None,
            cache=True,
        )
        .filter((pl.col("taxonRank") != "unranked") & _filter)
        .select(
            "taxonID",
            "genericName",
            "genus",
            "specificEpithet",
            "infraspecificEpithet",
            "taxonomicStatus",
            "acceptedNameUsageID",
            # "domain",
        )
        .filter((pl.col("genus") != "") & (pl.col("specificEpithet") != ""))
        .with_columns(
            infraspecificEpithet=pl.when(
                (pl.col("infraspecificEpithet").is_null())
            )
            .then(pl.lit(""))
            .otherwise(pl.col("infraspecificEpithet"))
        )
    )
    return (taxon_ranked_only,)


@app.cell
def _(bos_cleaned, pl, taxon_ranked_only):
    matching = (
        taxon_ranked_only.join(
            other=pl.LazyFrame(bos_cleaned),
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
    matching = matching.filter(~pl.col("speciesId").is_duplicated()).with_columns(
        acceptedNameUsageID=pl.col("acceptedNameUsageID")
        .fill_null(pl.lit(None))
        .cast(pl.Int64)
    )
    contentious = matching.filter(
        (pl.col("speciesId").is_duplicated())
    ).with_columns(
        acceptedNameUsageID=pl.col("acceptedNameUsageID")
        .fill_null(pl.lit(None))
        .cast(pl.Int64)
    )

    unique_contentious = contentious.filter(
        (pl.col("acceptedNameUsageID").is_null())
        & (
            pl.col("matched_taxonID").is_in(
                pl.col("acceptedNameUsageID").implode()
            )
        )
    )

    unique_contentious_speciesId = (
        unique_contentious.select("speciesId").collect().to_series().implode()
    )  # Just the speciesIds
    contentious = contentious.filter(
        ~pl.col("speciesId").is_in(unique_contentious_speciesId)
    )  # Removing unique conentious

    matching = pl.concat(
        [matching, unique_contentious],
    )

    matching = matching.with_columns(
        genus=pl.when(
            (pl.col("genus").is_null()) & (pl.col("specificEpithet").is_not_null())
        )
        .then(pl.col("taxonName").str.split(" ").list[0])
        .otherwise("genus")
    )

    no_match = (
        matching.filter(pl.col("matched_taxonID").is_null())
        .with_columns(
            taxonRank=pl.lit("BOSuncornirmedSpecies"),
            taxonomicStatus=pl.lit("BOSunformired"),
            parentNameUsageID=pl.lit(None),
        )
        .fill_null("")
        .collect()
    )
    return (no_match,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# First Matching Wrangle""")
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
def _():
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
    return (priority_columns,)


@app.cell
def _(pl, priority_columns, repeated_accepted_taxons):
    _schema = {
        "feature_that_is_equal_to_canonicalName": pl.String,
        "matches": pl.String,
    }
    _r = (repeated_accepted_taxons).collect()
    RAT_interim = pl.DataFrame(schema=_schema)
    for _col in priority_columns:
        _canonical_names = (
            _r.filter(pl.col("canonicalName") == pl.col(_col))
            .select("canonicalName")
            .unique()
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
    return (RAT_feats,)


@app.cell
def _(RAT_feats, no_match, pl, priority_columns, repeated_accepted_taxons):
    updated_to_matching = []
    still_no_match = []
    _collected_repeated_taxons = repeated_accepted_taxons.collect().fill_null("")
    _reversed_priority_columns = priority_columns.copy()
    _reversed_priority_columns.reverse()
    for _col in _reversed_priority_columns:
        for _match in RAT_feats["matches"]:
            # skipping those that are not in RAT_feats
            if _match not in no_match[_col].to_list():
                continue

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
            if _col == "genus" and _l[0] is not None and _l[1] is not None:
                has_predicament1 = True

            chosen_taxonId = taxon_data_to_select_from[selected_row_int, 0]
            other_taxonId = taxon_data_to_select_from[
                int(
                    not bool(selected_row_int)
                ),  # converts the int value into boolean than reverse the boolean before converting back to int.
                0,
            ]

            # Getting the _no_match_subset_to_update section
            _no_match_subset_to_update = no_match.filter(
                pl.col(_col) == _match,
            )
            _x = _reversed_priority_columns[:-3]
            feature_to_find_nulls = (
                _x[_reversed_priority_columns.index(_col) + 1]
                if len(_x) > _reversed_priority_columns.index(_col) + 1
                else None
            )  # just the next feature to find null
            if feature_to_find_nulls is not None:
                _no_match_subset_to_update = _no_match_subset_to_update.filter(
                    pl.col(feature_to_find_nulls) == ""
                )
                if _no_match_subset_to_update.is_empty():
                    still_no_match_subset = no_match.filter(
                        pl.col(_col) == _match,
                    ).with_columns(
                        current_feature=pl.lit(_col), current_name=pl.lit(_match)
                    )
                    still_no_match.append(still_no_match_subset)

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
                    assert (
                        match
                    )  # Double check that there is no unmatch.  # noqa: E712
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

            updated_to_matching.append(_no_match_subset_to_update)

    # turn them into dataframes and writing to csv files
    updated_to_matching = pl.concat(
        updated_to_matching, rechunk=True, parallel=True
    )
    updated_to_matching.write_csv("first_matches_set_from_wrangling.csv")

    still_no_match = pl.concat(still_no_match, rechunk=True, parallel=True)
    still_no_match = still_no_match.join(
        updated_to_matching.select("speciesId"), on="speciesId", how="anti"
    )
    return still_no_match, updated_to_matching


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Second Matching Wrangle""")
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


    def join_and_parentId_insertion(
        match_on: pl.DataFrame, _taxons_subset: pl.DataFrame, on: list
    ) -> pl.DataFrame:
        return (
            match_on.join(_taxons_subset, on=on)
            .with_columns(
                parentNameUsageID=pl.when(pl.col("taxonID").is_not_null())
                .then("taxonID")
                .otherwise("parentNameUsageID")
            )
            .drop("taxonID")
        )
    return join_and_parentId_insertion, taxons


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""##  matching for datapoints where phylumn to class ranks features are not empty strings"""
    )
    return


@app.cell
def _(join_and_parentId_insertion, pl, still_no_match, taxons):
    _filter_class = (
        pl.col("class") != "",
        pl.col("order") == "",
        pl.col("family") == "",
        pl.col("genus") == "",
        pl.col("specificEpithet") == "",
        pl.col("infraspecificEpithet") == "",
        pl.col("current_feature") == "phylum",
    )
    match_on_class = still_no_match.with_columns(
        name_to_match=pl.when(_filter_class).then("class").otherwise(None)
    ).filter(_filter_class)

    _x_class = (
        still_no_match.filter(_filter_class)
        .select("class")
        .group_by("class")
        .len()
    )

    _taxons_subset_class = (
        taxons.filter(
            pl.col("family").is_null(),
            pl.col("order").is_null(),
            pl.col("genus").is_null(),
            pl.col("class").is_in(_x_class["class"].unique().to_list()),
            pl.col("specificEpithet").is_null(),
            pl.col("infraspecificEpithet").is_null(),
            # pl.col("phylum").is_in(_x_class["phylum"].unique().to_list()),
        )
        .select("taxonID", "class")
        .collect()
    )
    match_on_class = join_and_parentId_insertion(
        match_on_class, _taxons_subset_class, ["class"]
    )
    return (match_on_class,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""## matching for datapoints where phylum to order ranks features are not empty strings"""
    )
    return


@app.cell
def _(join_and_parentId_insertion, pl, still_no_match, taxons):
    _filter_order = (
        pl.col("class") != "",
        pl.col("order") != "",
        pl.col("family") == "",
        pl.col("genus") == "",
        pl.col("specificEpithet") == "",
        pl.col("infraspecificEpithet") == "",
        pl.col("current_feature") == "phylum",
    )
    match_on_order = still_no_match.with_columns(
        name_to_match=pl.when(_filter_order).then("order").otherwise(None)
    ).filter(_filter_order)
    _x_order = (
        still_no_match.filter(_filter_order)
        .select("order")
        .group_by("order")
        .len()
    )

    _taxons_subset_order = (
        taxons.filter(
            pl.col("family").is_null(),
            pl.col("order").is_in(_x_order["order"].unique().to_list()),
            pl.col("genus").is_null(),
            pl.col("specificEpithet").is_null(),
            pl.col("infraspecificEpithet").is_null(),
        )
        .select("taxonID", "order")
        .collect()
    )
    match_on_order = join_and_parentId_insertion(
        match_on_order, _taxons_subset_order, ["order"]
    )
    return (match_on_order,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""## matching for datapoints where phylum to family ranks features are not empty"""
    )
    return


@app.cell
def _(join_and_parentId_insertion, pl, still_no_match, taxons):
    _filter_family = (
        pl.col("class") != "",
        pl.col("order") != "",
        pl.col("family") != "",
        pl.col("genus") == "",
        pl.col("specificEpithet") == "",
        pl.col("infraspecificEpithet") == "",
    )
    match_on_family = still_no_match.with_columns(
        name_to_match=pl.when(_filter_family).then("family").otherwise(None)
    ).filter(_filter_family)
    _x_family = (
        still_no_match.filter(_filter_family)
        .select("family", "order")
        .group_by("family", "order")
        .len()
    )

    _taxons_subset_family = (
        taxons.filter(
            pl.col("family").is_in(_x_family["family"].unique().to_list()),
            pl.col("genus").is_null(),
            pl.col("specificEpithet").is_null(),
            pl.col("infraspecificEpithet").is_null(),
            pl.col("order").is_in(_x_family["order"].unique().to_list()),
        )
        .select("taxonID", "family")
        .collect()
    )
    match_on_family = join_and_parentId_insertion(
        match_on_family, _taxons_subset_family, ["family"]
    )
    return (match_on_family,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""## matching for datapoints where phylum to specificEpithet ranks features are not empty strings"""
    )
    return


@app.cell
def _(join_and_parentId_insertion, pl, still_no_match, taxons):
    _filter_specificEpithet = (
        pl.col("class") != "",
        pl.col("order") != "",
        pl.col("family") != "",
        pl.col("genus") != "",
        pl.col("specificEpithet") != "",
        pl.col("infraspecificEpithet") == "",
    )

    to_be_match_on_specificEpithet = still_no_match.filter(_filter_specificEpithet)

    _x_specificEpithet = (
        still_no_match.filter(_filter_specificEpithet)
        .select("specificEpithet", "genus")
        .group_by("specificEpithet", "genus")
        .len()
    )

    _taxons_subset_specificEpithet = (
        taxons.filter(
            pl.col("specificEpithet").is_in(
                _x_specificEpithet["specificEpithet"].unique().to_list()
            ),
            pl.col("genus").is_in(_x_specificEpithet["genus"].unique().to_list()),
        )
        .select("taxonID", "specificEpithet", "genus")
        .collect()
    )
    match_on_specificEpithet = join_and_parentId_insertion(
        to_be_match_on_specificEpithet,
        _taxons_subset_specificEpithet,
        ["specificEpithet", "genus"],
    )
    return (match_on_specificEpithet,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""##matching for datapoints where phylum to infraspecificEpithet (Except for specificEpithet, no filters involved) ranks features are not empty strings."""
    )
    return


@app.cell
def _(join_and_parentId_insertion, pl, still_no_match, taxons):
    _filter_infraspecificEpithet = (
        pl.col("class") != "",
        pl.col("order") != "",
        pl.col("family") != "",
        pl.col("genus") != "",
        pl.col("infraspecificEpithet") != "",
    )  # different filter because some don't have specificEpithet either, if I am not wrong
    to_be_match_on_infraspecificEpithet = still_no_match.filter(
        _filter_infraspecificEpithet
    )

    _x_infraspecificEpithet = (
        still_no_match.filter(_filter_infraspecificEpithet)
        .select("infraspecificEpithet", "genus")
        .group_by("infraspecificEpithet", "genus")
        .len()
    )

    _taxons_subset_infraspecificEpithet = (
        taxons.filter(
            pl.col("infraspecificEpithet").is_in(
                _x_infraspecificEpithet["infraspecificEpithet"].unique().to_list()
            ),
            pl.col("genus").is_in(
                _x_infraspecificEpithet["genus"].unique().to_list()
            ),
        )
        .select("taxonID", "infraspecificEpithet", "genus")
        .collect()
    )

    match_on_infraspecificEpithet = join_and_parentId_insertion(
        to_be_match_on_infraspecificEpithet,
        _taxons_subset_infraspecificEpithet,
        ["infraspecificEpithet", "genus"],
    )
    return (match_on_infraspecificEpithet,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""## Combining all these second matching wrangling stage dataframes of the ranks"""
    )
    return


@app.cell
def _(
    match_on_class,
    match_on_family,
    match_on_infraspecificEpithet,
    match_on_order,
    match_on_specificEpithet,
    no_match,
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
    # print("second wrangle new matches", _new_match.shape)
    _new_match.write_csv("second_matches_set_from_wrangling.csv")
    _new_match = pl.concat(
        [
            _new_match,
            updated_to_matching.with_columns(
                pl.col("parentNameUsageID").cast(pl.Int64)
            ),
        ]
    )
    _new_match.write_csv("updated_to_matching.csv")
    _no_match = no_match.join(
        _new_match.select("speciesId"), on="speciesId", how="anti"
    )
    _no_match.write_csv("no_match.csv")
    return


if __name__ == "__main__":
    app.run()

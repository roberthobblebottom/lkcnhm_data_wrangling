import marimo

__generated_with = "0.14.13"
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
    print(
        unique_contentious.select(
            "speciesId", "acceptedNameUsageID", "matched_taxonID"
        ).collect()
    )
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


@app.cell
def _(contentious2):
    contentious2.collect().sort("speciesId")
    return


@app.cell
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
    return


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
    accepted_taxon_lookup.collect()
    return


@app.cell
def _(accepted_taxon_lookup, pl):
    unique_accepted_taxons = accepted_taxon_lookup.filter(
        ~pl.col("canonicalName").is_duplicated()
    )
    unique_accepted_taxons.group_by("canonicalName").len().sort("len").collect()
    return


@app.cell
def _(accepted_taxon_lookup, pl):
    repeated_accepted_taxons = accepted_taxon_lookup.filter(
        pl.col("canonicalName").is_duplicated()
    )
    repeated_accepted_taxons.group_by("canonicalName").len().sort("len").collect()
    return (repeated_accepted_taxons,)


@app.cell
def _(repeated_accepted_taxons):
    repeated_accepted_taxons.collect_schema().keys()
    return


@app.cell
def _(repeated_accepted_taxons):
    repeated_accepted_taxons.select(
        "canonicalName",
        "infraspecificEpithet",
        "specificEpithet",
        "genus",
        "family",
        "order",
        "class",
        "phylum",
        "kingdom",
        # "domain",
    ).collect()
    return


@app.cell
def _():
    return


@app.cell
def _(pl, repeated_accepted_taxons):
    _priority_columns = [
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

    # # Unsure about this whole for loop block
    _schema = {
        "feature_that_is_equal_to_canonicalName": pl.String,
        "matches": pl.String,
    }
    df2 = pl.DataFrame(schema=_schema)
    # print(type(df2))
    for _c in _priority_columns:
        _a = (
            repeated_accepted_taxons.filter(pl.col("canonicalName") == pl.col(_c))
            .select("canonicalName")
            .unique()
            .collect()
        )
        if _a.shape[0] != 0:
            _t = _a["canonicalName"].to_list()
            # print(_t[0])
            # print(type(_t))
            # print("canonical name ==", _c, " value:", _t)
            # print("canonical name ==", type(_c), " value:", type(_t))
            _row = pl.DataFrame(
                data={"feature_that_is_equal_to_canonicalName": _c, "matches": _t},
                schema=_schema,
            )
            # print(_row)
            df2 = df2.vstack(_row)
            # break
        # nomatch.with_columns(
        #     parentNameUsageID=pl.when(
        #         (pl.col("parentNameUsageID").is_nan())
        #         & (pl.col(_c).is_not_nan())
        #         & (pl.col(_c) != "")
        #     )
        #     .then(pl.lit(_a))
        #     .otherwise("parentNameUsageID")
        # )
    return (df2,)


@app.cell
def _(df2, h, pl):
    df2.group_by("matches").agg(
        pl.col("feature_that_is_equal_to_canonicalName").str.join(", ")
    ).with_columns(
        pl.col("feature_that_is_equal_to_canonicalName").str.split(", ")
    ).sort(h)
    return


app._unparsable_cell(
    r"""
    _x = \"\"
    for _m in df2['matches']:
        _x +=\",\"+_m
    _x =_x.replace('[',\"\").replace(\"]\",\"\")[1:].replace(\"'\",\"\")
    _x = list(set([_element for _element in _x.split(\", \")]))
    _x.sort()
    _x = pl.DataFrame([_x])
    _x = _x.with_columns(
        features = 
    )

    """,
    column=None, disabled=True, hide_code=False, name="_"
)


if __name__ == "__main__":
    app.run()

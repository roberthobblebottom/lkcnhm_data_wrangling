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

    from join_tables import join

    # @mo.persistent_cache
    df = join()

    taxon = pl.scan_csv(
        "gbif/Taxon.tsv", separator="\t", quote_char=None, cache=True
    )
    return df, mo, pl, taxon


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # matching and contentious split



    """
    )
    return


@app.cell
def _(df, pl):
    contentious = df.filter(
        (~pl.col("speciesId").is_first_distinct())
    ).with_columns(
        acceptedNameUsageID=pl.col("acceptedNameUsageID")
        .fill_null(pl.lit(-1))
        .cast(pl.Int64)
    )

    contentious.filter(
        ~pl.col("acceptedNameUsageID").is_in(pl.col("matched_taxonID").implode())
    ).sink_csv("contentious_part1.csv")
    return


@app.cell
def _(df, pl):
    matching = (df.filter((~pl.col("speciesId").is_duplicated()))).with_columns(
        acceptedNameUsageID=pl.col("acceptedNameUsageID")
        .fill_null(pl.lit(-1))
        .cast(pl.Int64)
    )

    contentious_part1 = pl.scan_csv("contentious_part1.csv")

    unique_contentious = contentious_part1.filter(
        (~pl.col("speciesId").is_duplicated()) & (~pl.col("speciesId").is_null())
    )  # added the non null filter, it is not in the R script.

    unique_contentius_speciesId = (
        unique_contentious.select("speciesId").collect().to_series().implode()
    )
    contentious2 = contentious_part1.filter(
        ~pl.col("speciesId").is_in(unique_contentius_speciesId)
    )
    # contentious2.collect()["speciesId"]

    matching = pl.concat(
        [matching, unique_contentious],
    )
    return (matching,)


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
    return (nomatch,)


@app.cell
def _(taxon):
    taxon.collect_schema().keys()
    return


@app.cell
def _(pl, taxon):
    accepted_taxon_lookup = (
        taxon.filter(pl.col("taxonomicStatus") == pl.lit("accepted"))
        .filter(pl.col("kingdom").is_in(["Animalia", "Plantae"]))
        .filter(~pl.col("canonicalName").is_null())
        .filter(pl.col("canonicalName") != "")
        .sort("canonicalName")
        .filter((~pl.col("canonicalName").is_first_distinct()))
    )
    return (accepted_taxon_lookup,)


@app.cell
def _(accepted_taxon_lookup, nomatch, pl):
    _priority_columns = [
        "infraspecificEpithet",
        "specificEpithet",
        "genus",
        "family",
        "order",
        "class",
        "phylum",
        "kingdom",
        "domain",
    ]

    # Unsure about this whole for loop block
    for _c in _priority_columns:
        _a = accepted_taxon_lookup.filter(pl.col("canonicalName") == _c)["taxonID"]
        nomatch.with_columns(
            parentNameUsageID=pl.when(
                (pl.col("parentNameUsageID").is_nan())
                & (pl.col(_c).is_not_nan())
                & (pl.col(_c) != "")
            )
            .then(pl.lit(_a))
            .otherwise("parentNameUsageID")
        )
    return


if __name__ == "__main__":
    app.run()

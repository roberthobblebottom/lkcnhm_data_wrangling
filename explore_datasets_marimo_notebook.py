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



    there are something wrong with the cell below, I can't find nomatches in the next next cell.
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
    df.filter(pl.col("matched_taxonID").is_null()).collect()
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
    return contentious2, matching


@app.cell
def _(contentious2):
    contentious2.collect()
    return


@app.cell
def _(matching):
    matching.collect()
    return


@app.cell
def _(matching, pl):
    # nomatch.collect()

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
def _(nomatch):
    nomatch.columns
    return


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
    )


    accepted_taxon_lookup.collect()
    return


if __name__ == "__main__":
    app.run()

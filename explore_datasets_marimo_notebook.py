import marimo

__generated_with = "0.14.13"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from join_tables import join

    df = join()
    return df, pl


@app.cell(disabled=True)
def _(pl):
    pl.scan_csv(
        "gbif/Taxon.tsv",
        separator="\t",
        quote_char=None,
        cache=True,
    ).describe()
    return


@app.cell
def _(df, pl):
    contentious = (
        df.filter(pl.col("speciesId").is_duplicated())
        .sort(by="speciesId")
        .filter(pl.col("acceptedNameUsageID") != "")
    )
    matching = df.filter(~pl.col("speciesId").is_duplicated()).sort(by="speciesId")
    return contentious, matching


@app.cell
def _(df, pl):
    df.group_by("infraspecificEpithet").len().sort(
        "len", descending=True
    ).collect().filter(pl.col("infraspecificEpithet") == "")
    return


@app.cell
def _(df):
    df.group_by("speciesId").len().collect().sort('len')
    return


@app.cell
def _(df):
    df.select("speciesId").count().collect()
    return


@app.cell
def _(contentious):
    contentious.group_by("speciesId").len().collect()
    return


@app.cell
def _(matching):
    matching.group_by("speciesId").len().collect().sort("len")
    return


if __name__ == "__main__":
    app.run()

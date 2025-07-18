import marimo

__generated_with = "0.14.11"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl


    taxon_lf = pl.scan_csv(
        "gbif/Taxon.tsv",
        separator="\t",
        quote_char=None,
        cache=True,
    )
    columns = ", ".join(taxon_lf.collect_schema().names())
    return columns, mo, pl, taxon_lf


@app.cell(hide_code=True)
def _(columns, mo):
    mo.md(
        f"""
    #Columns
    ### {columns}
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Describe taxon_lf""")
    return


@app.cell(hide_code=True)
def _(taxon_lf):
    taxon_lf.describe()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# taxon_lf sample 10""")
    return


@app.cell(hide_code=True)
def _(mo, taxon_lf):
    @mo.persistent_cache
    def _x():
        return taxon_lf.collect().sample(5)


    _x()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(rf"""# Unique Counts""")
    return


@app.cell(hide_code=True)
def _(mo, pl, taxon_lf):
    pl.Config.set_tbl_hide_column_data_types(True)


    @mo.persistent_cache
    def _x():
        _df = (
            taxon_lf.group_by("taxonRank")
            .len()
            .collect()
            .sort(by="len")
            .transpose()
        )
        return (
            _df[1, :]
            .rename(_df.head(1).to_dicts().pop())
            .with_columns(pl.all().cast(pl.Int64))
        )


    _x()
    return


@app.cell
def _(pl):
    pl.Config.restore_defaults()
    return


@app.cell
def _(mo, taxon_lf):
    @mo.persistent_cache
    def count(feature_name):
        return (
            taxon_lf.group_by(feature_name)
            .len()
            .sort(by="len", descending=True)
            .collect()
        )
    return (count,)


@app.cell(hide_code=True)
def _(count):
    count("kingdom")
    return


@app.cell
def _(count):
    count("phylum")
    return


@app.cell
def _(count):
    count("class")
    return


@app.cell
def _(count):
    count("order")
    return


@app.cell
def _(count):
    count("family")
    return


@app.cell
def _(count):
    count("genus")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Null Check""")
    return


@app.cell(hide_code=True)
def _(mo, pl, taxon_lf):
    @mo.persistent_cache
    def _x():
        return (
            taxon_lf.null_count()
            .collect()
            .transpose(include_header=True)
            .sort(by="column_0", descending=True)
            .transpose()
        )


    pl.Config.set_tbl_hide_column_data_types(True)
    pl.Config.set_tbl_hide_column_names(True)
    _x()
    return


@app.cell
def _(pl):
    pl.Config.restore_defaults()
    return


if __name__ == "__main__":
    app.run()

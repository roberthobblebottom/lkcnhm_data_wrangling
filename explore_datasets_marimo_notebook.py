import marimo

__generated_with = "0.14.12"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    return mo, pl


@app.cell
def _(pl):
    taxon_lf = pl.scan_csv(
        "gbif/Taxon.tsv",
        separator="\t",
        quote_char=None,
        cache=True,
    )
    taxon_lf_columns = ", ".join(taxon_lf.collect_schema().names())
    return taxon_lf, taxon_lf_columns


@app.cell
def _(pl):
    bos_df_1 = pl.read_csv("outputsplit.csv")


    bos_df_1 = bos_df_1.rename({"data cleanup changes": "data cleanup changes 1"})
    bos_df_1.columns = [_c.lower() for _c in bos_df_1.columns]
    bos_df_1_columns = ", ".join(bos_df_1.collect_schema().names())
    return bos_df_1, bos_df_1_columns


@app.cell(hide_code=True)
def _(bos_df_1_columns, mo, taxon_lf_columns):
    mo.md(
        f"""
    #taxon_lf Columns
    {taxon_lf_columns}  
    #   

    # bos_df_1 Columns
    {bos_df_1_columns}
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


@app.cell
def _(mo):
    mo.md(r"""# Describe bos_df_1""")
    return


@app.cell
def _(bos_df_1):
    bos_df_1.describe()
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


@app.cell
def _(mo):
    mo.md(r"""# bos_life sample 10""")
    return


@app.cell
def _(bos_df_1):
    bos_df_1.sample(10)
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
    pass
    return


@app.cell
def _(mo, pl, taxon_lf):
    @mo.persistent_cache
    def count(feature_name):
        pl.Config.set_tbl_rows(100)
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
    mo.md(r"""# Null Check for taxon_lf""")
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
    pass
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Null Check for bos_df_1""")
    return


@app.cell
def _(bos_df_1):
    bos_length = bos_df_1.shape[0]
    bos_length
    return (bos_length,)


@app.cell
def _(bos_df_1):
    (bos_df_1.null_count())
    return


@app.cell
def _(bos_df_1):
    bos_df_1["speciesid"]
    return


@app.cell
def _(bos_df_1):
    bos_df_1[" speciesid"].unique()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""Nothing in the second speciesid column, this column will be dropped."""
    )
    return


@app.cell
def _(bos_df_1, bos_length):
    _x = (
        bos_df_1.null_count()
        .transpose(include_header=True)
        .rename({"column": "feature", "column_0": "len"})
    )
    # _x.filter(_x['len']< bos_length)
    columns_to_drop_as_all_nulls = (
        _x.filter(_x["len"] == bos_length)
        .transpose()[0, :]
        .transpose()
        .to_series()
        .to_list()
    )
    columns_to_drop_as_all_nulls
    return (columns_to_drop_as_all_nulls,)


@app.cell
def _():
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    # checking on some unique values on sparse columns

    more columns to be dropped:
    """
    )
    return


@app.cell
def _(bos_df_1):
    columns_to_drop_as_they_are_just_changes_logs = [
        "cleanup changes ",
        "data cleanup changes",
        "data cleanup changes 1",
        "changes",
        "cleanup changes",
        "unnamed: 18",
        "unnamed: 17",
    ]
    for _feature in columns_to_drop_as_they_are_just_changes_logs:
        print(
            "column",
            _feature,
            "\n",
            bos_df_1[_feature].filter(bos_df_1[_feature].is_not_null()).to_list(),
        )
    return (columns_to_drop_as_they_are_just_changes_logs,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## All ambigious columns part 1""")
    return


@app.cell
def _(
    bos_df_1,
    bos_length,
    columns_to_drop_as_all_nulls,
    columns_to_drop_as_they_are_just_changes_logs,
    pl,
):
    _x = (
        bos_df_1.null_count()
        .transpose(include_header=True)
        .rename({"column": "feature", "column_0": "len"})
    )
    # _x.filter(_x['len']< bos_length)
    nulls_number_threshold = 100
    _ambigious_columns = (
        _x.filter(
            (_x["len"] > bos_length - nulls_number_threshold)
            & (
                ~pl.col("feature").is_in(columns_to_drop_as_all_nulls)
                & (
                    ~pl.col("feature").is_in(
                        columns_to_drop_as_they_are_just_changes_logs
                    )
                )
            )
        )
        .transpose()[0, :]
        .transpose()
        .to_series()
        .to_list()
    )

    for _feature in _ambigious_columns:
        print(
            "column",
            _feature,
            "\n",
            bos_df_1[_feature]
            .filter(bos_df_1[_feature].is_not_null())
            .unique()
            .to_list(),
        )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## some of the columns are manually removed from part 1""")
    return


@app.cell
def _(bos_df_1):
    ambigiousolumns_2 = [
        "unnamed: 19",
        "query",
        "issue",
        "query ",
        "unnamed: 21",
        "unnamed: 22",
        "unnamed: 20",
        "unnamed: 23",
    ]

    for _feature in ambigiousolumns_2:
        print(
            "column",
            _feature,
            "\n",
            bos_df_1[_feature]
            .filter(bos_df_1[_feature].is_not_null())
            .unique()
            .to_list(),
        )
    return


@app.cell
def _():
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Dropping selected columns for bos_df_1; new df bos_df_2""")
    return


@app.cell
def _(
    bos_df_1,
    columns_to_drop_as_all_nulls,
    columns_to_drop_as_they_are_just_changes_logs,
):
    bos_df_2 = bos_df_1.drop(columns_to_drop_as_all_nulls).drop(
        columns_to_drop_as_they_are_just_changes_logs
    )
    return (bos_df_2,)


@app.cell
def _(bos_df_2):
    bos_df_2.columns
    return


@app.cell
def _(bos_df_2):
    bos_df_2
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# R script translation""")
    return


@app.cell
def _(taxon_lf):
    taxon_lf.select("taxonomicStatus").collect().unique()
    return


@app.cell
def _(taxon_lf):
    taxon_lf.select("taxonRank").collect().unique()
    return


@app.cell
def _(pl, taxon_lf):
    _l = [
        "accepted",
        "synonym",
        "homotypic synonym",
        "proparte synonym",
        "heterotypic synonym",
    ]

    _l2 = [
        "taxonID",
        "genericName",
        "genus",
        "specificEpithet",
        "infraspecificEpithet",
        "taxonomicStatus",
        "acceptedNameUsageID",
    ]
    taxon_ranked_only = (
        taxon_lf.filter(
            (pl.col("taxonRank") != "unranked")
            & (pl.col("taxonomicStatus").is_in(_l))
        )
        .select(_l2)
        .filter((pl.col("genus") != "") | (pl.col("specificEpithet") != ""))
    )
    return (taxon_ranked_only,)


@app.cell
def _(taxon_ranked_only):
    taxon_ranked_only.collect().sample(100)
    return


if __name__ == "__main__":
    app.run()

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
    # bos_df_1.columns = [_c.lower() for _c in bos_df_1.columns]
    print(bos_df_1.columns)
    bos_df_1 = bos_df_1.rename(
        {
            "species": "specificEpithet",
            "subspecies": "infraspecificEpithet",
            "genus": "genericName",
        }
    )

    bos_df_1 = bos_df_1.with_columns(pl.col(pl.String).str.strip_chars(" "))
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


@app.cell
def _(bos_df_1_columns):
    bos_df_1_columns.find("genericName")
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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# bos_life sample 10""")
    return


@app.cell
def _(bos_df_1):
    bos_df_1.sample(10)
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
    bos_df_1["speciesId"]
    return


@app.cell
def _(bos_df_1):
    bos_df_1[" speciesId"].unique()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Nothing in the second speciesId column, this column will be dropped.""")
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


@app.cell(hide_code=True)
def _():
    return


@app.cell(hide_code=True)
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
        "Data cleanup changes",
        "data cleanup changes 1",
        "changes",
        "cleanup changes",
        "Unnamed: 18",
        "Unnamed: 17",
        "Unnamed: 19",
        "query",
        "issue",
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
        "Unnamed: 19",
        "query",
        "issue",
        "query ",
        "Unnamed: 21",
        "Unnamed: 22",
        "Unnamed: 20",
        "Unnamed: 23",
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
    # taxon_lf.select(pl.col('acceptedNameUsageID')).collect().head(10)
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
    pl,
):
    bos_df_2 = bos_df_1.drop(columns_to_drop_as_all_nulls).drop(
        columns_to_drop_as_they_are_just_changes_logs
    ).select(~pl.selectors.starts_with("Unnamed"))
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
    mo.md(r"""##  name cleaning""")
    return


@app.cell
def _(bos_df_2, pl):
    bos_df_3 = (
        bos_df_2

            .with_columns(
                infraspecificEpithet=pl.when(
                (pl.col("genericName") == "Glenea")
                & (pl.col("specificEpithet") == "mathemathica")
            )
            .then(pl.lit("mathematica"))
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
        )       .fill_nan("")
        .fill_null("")
    )


    bos_df_3
    return (bos_df_3,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# R script translation""")
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

    _c = [
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
        .select(_c)
        # .filter((pl.col("genus") != "") | (pl.col("specificEpithet") != ""))
        .fill_null("")
        .fill_nan("")
    )
    taxon_ranked_only.select(pl.len()).collect().item()
    return (taxon_ranked_only,)


@app.cell
def _(taxon_ranked_only):
    taxon_ranked_only.collect_schema().keys()
    return


@app.cell
def _(taxon_ranked_only):
    taxon_ranked_only.select(
        ["genericName", "specificEpithet", "taxonID"]
    ).unique().collect()
    return


@app.cell
def _(bos_df_3):
    bos_df_3.columns

    return


@app.cell
def _(bos_df_3, pl):
    bos_df_3.select(
        ["genericName", "specificEpithet", "infraspecificEpithet"]
    ).filter(pl.col("genericName")!="")
    return


@app.cell
def _(bos_df_3, pl):
    bos_df_3.select(
        ["genericName", "specificEpithet", "infraspecificEpithet"]
    ).filter(pl.col("genericName")=="")
    return


@app.cell
def _(bos_df_3, pl):
    bos_df_3.select(
        ["genericName", "specificEpithet", "infraspecificEpithet"]
    ).filter(pl.col("specificEpithet") != "")
    return


@app.cell
def _(bos_df_3, pl):
    bos_df_3.select(
        ["genericName", "specificEpithet", "infraspecificEpithet"]
    ).filter(pl.col("infraspecificEpithet") != "")
    return


@app.cell
def _(taxon_ranked_only):
    taxon_ranked_only.select(
        [ "specificEpithet"]
    ).unique().collect()
    return


@app.cell(disabled=True)
def _(taxon_ranked_only):
    taxon_ranked_only.collect().head(10)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Join Attempt""")
    return


@app.cell
def _(bos_df_3, pl, taxon_ranked_only):
    matching_df = (
        pl.LazyFrame(bos_df_3)
        .join(
            other=taxon_ranked_only,
            on=["genericName", "specificEpithet", "infraspecificEpithet"],
            how="inner",
        )
        .rename({"taxonID": "matched_taxonID"})
    )
    return (matching_df,)


@app.cell
def _(matching_df):
    matching_df.collect()
    return


@app.cell(disabled=True)
def _(matching_df, pl):
    _df = matching_df.select(
        ["genericName", "specificEpithet", "matched_taxonID"]
    )
    print(_df.filter(pl.col("genericName")=="").collect())
    print()
    print(_df.filter(pl.col("specificEpithet")=="").collect())
    print()
    print(
        _df.filter(
             (pl.col("matched_taxonID")==0)
        ).collect()
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## contentious and matching rows""")
    return


@app.cell
def _(matching_df, pl):
    matching_df_2 = matching_df.select(
        (
            "speciesId",
            "matched_taxonID",
            "acceptedNameUsageID",
            "taxonName",
            "domain ",
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
            "specificEpithet",
            "infraspecificEpithet",
        )
    )


    contentious = matching_df_2.filter(pl.col("speciesId").is_duplicated()).sort(
        by="speciesId"
    )
    matching_df_2 = matching_df_2.filter(
        ~pl.col("speciesId").is_duplicated()
    ).sort(by="speciesId")


    # I don't know how to do this lines:
    # R code:
    # contentious <- contentious[!contentious$acceptedNameUsageID %in% contentious$matched_taxonID, ]
    # Python code attempt:
    # contentious = contentious.filter(~pl.col('acceptedNameUsageID') == (pl.col('matched_taxonID')))


    # matching_df_2.collect().describe()
    return contentious, matching_df_2


@app.cell
def _():
    # contentious.collect().describe()
    return


@app.cell(disabled=True)
def _(matching_df_2, pl):
    matching_df_2.filter(pl.col("acceptedNameUsageID").is_null()).collect().shape
    return


@app.cell(disabled=True)
def _(contentious, pl):
    contentious_2 = contentious.filter(
        pl.col("acceptedNameUsageID") != ""
    ).collect()
    # _contentious_unique.collect()
    # matching_df_3 = matching_df_2.collect().vstack(contentious_2)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## No match""")
    return


@app.cell
def _():
    # nomatch_df = matching_df_2.filter(pl.col("matched_taxonID").is_not_null())

    # nomatch_df.collect()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""the vstack may not be needed according to the R code.""")
    return


if __name__ == "__main__":
    app.run()

import marimo

__generated_with = "0.14.13"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    import polars as pl
    return (pl,)


@app.cell(hide_code=True)
def _(pl):
    matching_lf = pl.scan_csv('matching_testing.csv')
    return (matching_lf,)


@app.cell
def _(matching_lf):
    matching_lf.collect().shape
    return


if __name__ == "__main__":
    app.run()

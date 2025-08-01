import marimo

__generated_with = "0.14.15"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    df = pl.read_csv("outputsplit.csv")

    df['kingdom'].unique()
    return (df,)


@app.cell
def _(df):
    df['domain'].unique()
    return


if __name__ == "__main__":
    app.run()

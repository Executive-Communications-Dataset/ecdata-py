

<p align="center">
<a href="https://joshuafayallen.github.io/ecdata/">
<img src="hex-logo.png" height = "350" class = "center"> </a>
</p>

`ecdata` is a minimal package for downloading *Executive Communications
Dataset*. It includes subsets of all the country data, the full dataset,
data dictionaries, and a sample script to help users expand the dataset.
For our full replication archive, see the relevant subdirectories in
[our
GitHub](https://github.com/joshuafayallen/executivestatements/tree/main/raw-data).
This repository *is* the Python implementation; for the R package see
[ecdata](https://github.com/Executive-Communications-Dataset/ecdata).

## Installation

Install from GitHub. **Neither package index currently serves a current version**:
PyPI holds `1.1.3`, which predates the data audit and defaults to data release
`1.0.0`, and the R package was archived from CRAN on 2025-01-12. Until both are
re-published, GitHub is the only route to a package that defaults to the repaired
data.

## Python

    pip install git+https://github.com/Executive-Communications-Dataset/ecdata-py

## R

``` r
pak::pkg_install('Executive-Communications-Dataset/ecdata')
```

Both give you version `1.2.0`, which defaults to data release
[`1.0.1`](https://github.com/Executive-Communications-Dataset/ecdata/releases/tag/1.0.1).
`pip install ecdata` will silently give you `1.1.3` instead, which defaults to
`1.0.0` and carries the defects the audit found.

## Usage

To see a list of countries in our dataset and the associated file name
in the GitHub release, you can run:

## R

``` r
library(ecdata)

ecd_country_dictionary |>
    head()
```

      name_in_dataset  file_name
    1       Argentina  argentina
    2       Australia  australia
    3         Austria    austria
    4      Azerbaijan azerbaijan
    5         Bolivia    bolivia
    6          Brazil     brazil

## Python

``` python
import ecdata as ec

ec.country_dictionary().head(2)
```

## Loading the Executive Communications Dataset

We offer variety of options to load the ECD. You can specify single
countries

## R

``` r
load_ecd(country = 'United States of America') |>
    head(n = 2)
```

                       country
    1 United States of America
    2 United States of America
                                                                                              url
    1 https://www.presidency.ucsb.edu/documents/remarks-luncheon-for-the-us-olympic-medal-winners
    2 https://www.presidency.ucsb.edu/documents/remarks-luncheon-for-the-us-olympic-medal-winners
                                                                                                                                                                         text
    1                                                                                                                                                            About Search
    2 I hope you are understanding people. I appreciate your patience and ask for your forgiveness. I would like to introduce to you a few of our distinguished guests today.
            date title         executive   type language file isonumber gwc
    1 1964-12-01  <NA> Lyndon B. Johnson Speech  English <NA>       840 USA
    2 1964-12-01  <NA> Lyndon B. Johnson Speech  English <NA>       840 USA
      cowcodes polity_v polity_iv vdem year_of_statement
    1      USA      USA       USA   20              1964
    2      USA      USA       USA   20              1964

## Python

``` python
ec.load_ecd(country = 'United States of America').head(2)
```

You can specify multiple countries to `load_ecd` like this

## R

``` r
load_ecd(country = c('United States of America', 'Turkey', 'France'))  |>
    head(n = 3)
```

    ✔ Successfully downloaded data for United States of America, Turkey, and France

      country
    1  France
    2  France
    3  France
                                                                                           url
    1 https://www.elysee.fr/emmanuel-macron/2020/01/06/conseil-des-ministres-du-6-janvier-2020
    2 https://www.elysee.fr/emmanuel-macron/2020/01/06/conseil-des-ministres-du-6-janvier-2020
    3 https://www.elysee.fr/emmanuel-macron/2020/01/06/conseil-des-ministres-du-6-janvier-2020
                               text       date
    1 6 janvier 2020 - Compte-rendu 2020-01-06
    2                PROJETS DE LOI 2020-01-06
    3                    ORDONNANCE 2020-01-06
                                        title executive                 type
    1 Conseil des ministres du 6 janvier 2020      <NA> Council Of Ministers
    2 Conseil des ministres du 6 janvier 2020      <NA> Council Of Ministers
    3 Conseil des ministres du 6 janvier 2020      <NA> Council Of Ministers
      language file isonumber gwc cowcodes polity_v polity_iv vdem
    1   French <NA>       250 FRN      FRN      FRN       FRN   76
    2   French <NA>       250 FRN      FRN      FRN       FRN   76
    3   French <NA>       250 FRN      FRN      FRN       FRN   76
      year_of_statement
    1              2020
    2              2020
    3              2020

## Python

``` python
ec.load_ecd(country = ['United States of America', 'Turkey', 'France']).head(2)
```

`load_ecd` accepts a single name, any iterable of names (list, tuple, set) or
a mapping whose keys are names. You can also filter by language, and use
`lazy_load_ecd` for a `LazyFrame` if you would rather not materialise a large
country like India or Denmark:

``` python
ec.load_ecd(language = 'Danish')

(ec.lazy_load_ecd(country = 'India', deduplicate = True)
   .filter(pl.col('year_of_statement') >= 2020)
   .collect())
```

## Which release you get

`load_ecd` defaults to **`1.0.1`**, a repair release: `1.0.0` with the defects that
could be fixed from the published files fixed. Pass `ecd_version='1.0.0'` for the
original assets, which are still published and unchanged.

**Row counts are 82.8% lower in `1.0.1`** — 2,891,622 rows against 16,845,134 — because
that is how many rows were exact duplicates. India goes from 7,970,491 to 82,682.
Ecuador and the Dominican Republic are also much smaller, because in `1.0.0` they
shared one pooled corpus published under two labels. If you have numbers from
`1.0.0`, expect them to change, and read the
[release notes](https://github.com/Executive-Communications-Dataset/ecdata/releases/tag/1.0.1)
before you do.

What `1.0.1` still gets wrong — overlapping executive terms, Colombia's YouTube
provenance, Russia's English translations, `type` in the US file — needs the source
data back, so `load_ecd` still warns for those assets.

## Known data issues in release 1.0.0

These are the defects in the original `1.0.0` assets. Most are fixed in `1.0.1`; the
table is kept because `1.0.0` is still published and still citable. `load_ecd` warns
when you touch an affected asset and offers two options that work around the worst
of it.

| Issue | Affected | Workaround |
|---|---|---|
| `ecuador.parquet` and `dominican_republic.parquet` hold the same pooled corpus with swapped country and executive labels | Ecuador, Dominican Republic | none -- treat both as unreliable |
| Up to 99% of rows are exact duplicates | India, Denmark, France, Turkey, Mexico, Austria, Korea | `deduplicate=True` |
| Assets do not share a schema, so a plain concat raises | Portugal, Dominican Republic, USA, Germany | handled: `normalize_schema=True` (default) |
| `full_ecd.parquet` duplicates Ecuador and holds one empty row for Portugal | full dataset | load those two countries individually |
| Text mis-decoded (UTF-8 read as latin-1) | Venezuela | none |
| Every `url` has the host doubled | Jamaica | none |
| `url` is 100% null, and the text is the English kremlin.ru edition rather than the Russian original | Russia | none |
| Every `url` is a YouTube link rather than an official record | Colombia | none |
| `executive` splits Biden across two spellings, dates the Obama/Trump handover to 2016, and runs Ford to 1996; `type` holds president names; `language` is null | USA | none |
| Rows are paragraph-level in some countries and document-level in others | varies | check `text` length before comparing counts |

Every country in this table raises a `UserWarning` when you load it, naming the
defect. `tests/test_warnings.py` holds the table and the code to it, so the two
cannot drift apart.

[`ecd_validate.py`](https://github.com/Executive-Communications-Dataset/ecdata/blob/main/data-validation/ecd_validate.py),
in the dataset repository, reproduces all of the above from the published assets,
and [`ecd_repair.py`](https://github.com/Executive-Communications-Dataset/ecdata/blob/main/data-validation/ecd_repair.py)
alongside it is what produced `1.0.1` and `1.0.2`.

## Example Scrappers

We also provide a set of an example scrappers in part to quickly
summarize our replication files and for other researchers to either
collect more recent data or expand the cases in our dataset. To call
these scrappers simply run:

``` r
# static website scrapper
example_scrapper(scrapper_type = 'static')

# dynamic website scrapper 

example_scrapper(scrapper_type = 'dynamic')
```

If `scrapper_type = 'static'` this will open a R script in your current
editor. If `scrapper_type = 'dynamic'` this will open a Python script in
your editor.

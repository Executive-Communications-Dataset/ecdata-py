# Data audit and release validation

They are not here. Both live in the dataset's own repository, because they are
about the data rather than about this package:

* [`data-validation/DATA_AUDIT.md`](https://github.com/Executive-Communications-Dataset/ecdata/blob/main/data-validation/DATA_AUDIT.md)
  — the audit of release `1.0.0`
* [`data-validation/ecd_validate.py`](https://github.com/Executive-Communications-Dataset/ecdata/blob/main/data-validation/ecd_validate.py)
  — the release gate that reproduces every finding from the published assets
* [`data-validation/ecd_repair.py`](https://github.com/Executive-Communications-Dataset/ecdata/blob/main/data-validation/ecd_repair.py)
  — the repair pass that produced releases `1.0.1` and `1.0.2`
* [`data-validation/report-1.0.0.txt`](https://github.com/Executive-Communications-Dataset/ecdata/blob/main/data-validation/report-1.0.0.txt)
  — what the validator says about `1.0.0`

This directory previously held its own copies of the first two. They drifted
within a fortnight: the copy here kept a `sql()` helper that let duckdb exceptions
escape instead of wrapping them, and an executive query missing a `::TIMESTAMP`
cast, so the validator crashed rather than reporting in the environment its own
workflow built. One copy, in the repository that publishes the releases, is the
only arrangement that does not repeat that.

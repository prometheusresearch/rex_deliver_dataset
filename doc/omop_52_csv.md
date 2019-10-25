# OMOP CDM v5.2 CSV Datasets

When you specify a `dataset_type` of `omop:5.2:csv` in your configuration,
`rex_deliver_dataset` will allow a set of CSV files structured according to
[v5.2 of the OMOP Common Data
Model](https://github.com/OHDSI/CommonDataModel/raw/v5.2.2/OMOP_CDM_v5_2.pdf).
The requirements for this type of dataset are as follows:

* Only data for the tables defined in the following sections of the OMOP
  specification are allowed:
  * Clinical Data Tables
  * Health System Data Tables
  * Health Economics Data Tables
  * Derived Elements
  * Metadata
* Data for the tables defined in the Vocabulary section of the OMOP
  specification is not allowed.
  * The requirement is that data in tables from the other areas of the CDM that
    refer to records in the Vocabulary tables will be using the standard
    content (e.g., as published by [Athena](http://athena.ohdsi.org)).
* The data for each table in OMOP CDM must be delivered as a separate file.
  * The base names of the files must be exactly as the tables are named in OMOP
    (including underscores, if used).
  * The names can be in any case. E.g., the file that contains data for the
    `PERSON` table could be named `PERSON.csv`, `person.CSV`, or `PeRsOn.CsV`.
  * All files must have an extension of `.csv`.
  * There can only be one file delivered per table. You cannot provide both a
    `PERSON.csv` and a `person.csv`.
* Each file must contain all columns defined for the given OMOP table, even if
  they’re not being used.
  * The column names must be listed as the first record in the file.
  * Column names are case-insensitive.
  * The ordering of the columns in the file is not defined. You may provide
    them in any order, as they all exist.
  * No columns beyond those specified by OMOP can be present in the file.
* The contents of the files must be structured as Comma-Separated Values files
  as described in section 2 of [RFC4180](https://tools.ietf.org/html/rfc4180).
  Notably:
  * Each record is delimited by a CRLF (ASCII 13  & 10).
  * Each column is delimited by a comma (ASCII 44).
  * Column values may be enclosed in double quotes (ASCII 34).
  * Column values that contain commas, double quotes, or CRLFs must be
    enclosed in double quotes.
  * All records must have the same number of columns.
* Column values must be appropriately formatted according to the types
  specified by OMOP:
  * integer
    * Must be represented as whole numbers using Arabic numerals (ASCII 48
      through 57)
    * Commas or other digit grouping separators are not permitted.
  * float
    * Must be represented as decimal numbers using Arabic numerals (ASCII 48
      through 57)
    * A period (ASCII 46) must be used to separate the whole number from the
      fractional part.
  * varchar/text/clob/string
    * Can be any string that fits within the length restrictions specified by
      OMOP
  * date
    * Must be represented as YYYY-MM-DD (e.g., `2019-05-22`)
  * datetime
    * Must be represented as YYYY-MM-DDTHH:MM:SS (e.g., `2019-05-22T12:34:56`)
    * If a timezone is being specified, it must be appended to the date as
      ±HH:MM (e.g. `2019-05-22T12:34:56+04:00`)
    * If the timezone should be interpreted as UTC, then either no timezone
      offset should be specified, or use the single letter `Z` in the place
      of the offset (e.g. `2019-05-22T12:34:56Z`)
    * Fractional seconds are not allowed.
* Columns defined as required in the OMOP specification must have values
  provided in every record.


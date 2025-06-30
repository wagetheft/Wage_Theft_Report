# Wage_Theft_Report Project

This project analyzes and processes wage theft data for research and reporting. The codebase is designed to be accessible for contributors of all skill levels. Use the tables below to find modules that match your experience and interests.

## Beginner Modules

| Module/File                        | Description                                                      |
|------------------------------------|------------------------------------------------------------------|
| api/util_zipcode.py                | Zip code inference and cleaning using pandas and string matching. |
| api/wagetheft_read_df.py           | Reads and prepares dataframes from CSV and other sources.         |
| api/util_filter.py                 | Simple data filtering helpers.                                    |
| api/util_group.py                  | Grouping and aggregation helpers.                                 |
| api/constants/industries.py        | Industry constants and lookup tables.                             |
| api/constants/prevailingWageTerms.py| Prevailing wage term definitions.                                 |
| api/constants/signatories.py       | Signatory constants and lookup tables.                            |
| api/constants/zipcodes.py          | Zip code and city lookup tables.                                  |

## Intermediate Modules

| Module/File                        | Description                                                      |
|------------------------------------|------------------------------------------------------------------|
| api/wageReportAPI.py               | Main API logic for wage theft report generation.                  |
| api/wagetheft_gen_report.py        | Generates wage theft reports from processed data.                 |
| api/wagetheft_tables.py            | Table formatting and output utilities.                            |
| api/util_industry_pattern.py       | Industry pattern matching and classification.                     |
| api/util_signatory_pattern.py      | Signatory pattern matching and classification.                    |
| api/util_trade_pattern.py          | Trade pattern matching and classification.                        |
| api/wagetheft_io_utils.py          | Input/output utilities for reading and writing data.              |
| api/wagetheft_inference_util.py    | Inference utilities for wage theft data.                          |
| api/wagetheft_shape_df.py          | Data shaping and transformation utilities.                        |
| api/wagetheft_calc_utils.py        | Calculation utilities for wage theft analysis.                    |
| api/wagetheft_clean_value_utils.py | Data cleaning, type inference, and deduplication.                 |
| api/wagetheft_print_utils.py       | String formatting and output utilities for large datasets.         |

## Advanced Modules

| Module/File                        | Description                                                      |
|------------------------------------|------------------------------------------------------------------|
| *(None currently identified)*      |                                                                  |

## Getting Started

- **New to Python or pandas?** Start with the modules in the Beginner table. These use basic data manipulation and are a great way to get familiar with the codebase.
- **Ready for more?** Try modules in the Intermediate table, which use more advanced pattern matching and report generation.
- **Expert?** As this is an opensource project, to remain assesible to a volunteer dev team, there are no advanced modules and intermediate modules are revised to a begineer level where possible.

## Contributing

- Please read the comments and docstrings in each file for guidance.
- If you have questions, open an issue or ask for help in the project discussion board.
- We welcome all contributions, from documentation to code improvements! 
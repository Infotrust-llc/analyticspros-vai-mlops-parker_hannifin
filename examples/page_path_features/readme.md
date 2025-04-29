# Page Path Features

## Description

In this example, we used SQL to determine the fraction of traffic that goes through page paths on the given GA4 property. Then, we take the weight of the page paths and implement them into our system as features that may have solid predictive power.

## How to Use

1. Copy the files from this folder to your main AnDY directory.

2. Change the variables in the config.yaml

3. Ensure you have a sp_create_dataset.sqlx.tpl file with is formatted propery for Jinja to parse and fill the necessary fields.

4. Run the feature_finder.py file to create your sp_create_dataset.sqlx file.

   - Use the examples below as a starting point for running the command line application.

5. Run the AnDY system with your generated sp_create_dataset.sqlx file

## Example

### Run from config

```
python feature_finder.py \
--config_filename=./config.yaml \
--render
```

### Run from command line arguments

```
python feature_finder.py \
--billing_project_id="as-dev-christopher" \
--src_project_id="bigquery-public-data" \
--src_dataset_id="ga4_obfuscated_sample_ecommerce" \
--date_start="2020-12-01" \
--date_end="2020-12-15" \
--re_page_path="https://shop.googlemerchandisestore.com(/[a-z-0-9]*/?).*" \
--perc_keep=95 \
--min_engagement=2 \
--kwargs lookback=14 lookahead=7 \
--render
```

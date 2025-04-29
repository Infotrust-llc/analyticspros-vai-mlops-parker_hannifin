# Botscore

## Description

This example implements another feature of "botscores". The methodology for determining a botscore is to calculate the cadence of page_view events and determine if the rate at which those items occur is "normal" or not.

## How to Use

Use the sp*create_dataset.sqlx file in this example folder and adjust it's sources (i.e. change `FROM ``bigquery-public-data.ga4_obfuscated_sample_ecommerce.events\**`` ` to `FROM ``my*project.my_ga4_dataset.events*\_`` `) to your GA4 dataset.

# BQML AutoML

## Description

How to set up config for AutoML models.

## How to Use
The main thing is setting up config.yaml correctly. Specifically the model section.

```yaml
...
model:
    type: BQML  
    create_model_params:
        model_type: AUTOML_CLASSIFIER | AUTOML_REGRESSOR
        budget_hours: 2
...
```
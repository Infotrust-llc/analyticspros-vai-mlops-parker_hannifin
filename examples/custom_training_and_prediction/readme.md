# Custom Training and Prediction

## Description

This example shows the processes involved in creating a custom model and automating its training pipeline. The prediction pipeline utilizes a default sklearn container to automate predictions. If you would like to customize the prediction pipeline as well, see `./custom_training_and_prediction_v2`.

Please review `notebook.ipynb`, and the other example files in this directory. In it, one will see the overarching processes associated with developing a custom model and its training and prediction pipelines. View the notebook as a high level instruction manual/template on the steps one needs to take-your code implementation may vary. Overall, the steps to execute this process are as follows:  

## How to Use
High level steps:
1. Create `task.py` as your entry point into the training
2. Create a Dcokerfile to package the training/eval container (`task.py` should be at root) 
3. Push the training/eval container to Artifact Repository
4. Create appropriate `config.yaml` and `sp_create_dataset.sqlx`

```yaml
...
model:
    type: CUSTOM
    custom_training_params:
        container_uri: us-central1-docker.pkg.dev/as-dev-anze/conv-propensity/vai-training-container:prod
        model_serving_container_image_uri: us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-2:latest
        machine_type: n1-standard-4
```
*container_uri* - URI of the container used for training and evaluation

*model_serving_container_image_uri* - URI of the container used to serve the predictions
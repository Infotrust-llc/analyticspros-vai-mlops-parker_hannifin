# Custom Training and Prediction

## Description

This example shows the steps and and sample code that can be used to deploy a custom model and automate its training and prediction pipelines. 

Please review `notebook.ipynb`, and the other example files in this directory. In it, one will see the overarching processes associated with developing a custom model and its relevant pipelines. View the notebook as a high level instruction manual/template on the steps one needs to take- your code implementation may vary. Overall, the steps to execute this process are as follows:  

## How to Use
High level steps:
1. Create a custom training/eval code (ex. folder `./training`)
2. Create a custom serving code (ex. folder `./serving`)
3. Test steps 1. and 2. locally to ensure the server routing is working properly
4. Dockerize both codebases and push images to Artifacts Repository
5. Create appropriate `config.yaml` and `sp_create_dataset.sqlx` 
    This process is the same as any other AnDY implementation

Please review example `serving` and `training` folders as well as `notebook.ipynb`.

```yaml
...
model:
    type: CUSTOM
    custom_training_params:
        container_uri: us-central1-docker.pkg.dev/as-dev-anze/conv-propensity/vai-training-container:prod
        model_serving_container_image_uri: us-central1-docker.pkg.dev/as-dev-anze/vaimlops-custom-model-custom/vai-serving-container:prod
        machine_type: n1-standard-4
```
*container_uri* - URI of the container used for training and evaluation

*model_serving_container_image_uri* - URI of the container used to serve the predictions
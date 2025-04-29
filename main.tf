terraform {
  required_providers {
    google = {
      version = "~> 5.0.0"
    }
    jinja = {
      source  = "NikolaLohinski/jinja"
      version = "1.17.0"
    }
  }
}

variable "config_path" { # Allows our config path to be passed by command line or environment variable
  type    = string
  default = "config.yaml"
}

locals {
  yaml_config         = yamldecode(file(var.config_path))
  yaml_config_hash    = filemd5(var.config_path)
  project_id          = local.yaml_config["gcp_project_id"]
  dataset_id          = local.yaml_config["bq_dataset_id"]
  region              = local.yaml_config["gcp_region"]
  repo_name_docker    = replace(local.dataset_id, "_", "-")
  gcr_docker          = "${local.region}-docker.pkg.dev/${local.project_id}/${local.repo_name_docker}/vai-pipelines:prod"
  repo_name_pipelines = "${local.repo_name_docker}-pipelines"
  bucket_uri          = "${local.project_id}-${local.repo_name_docker}-pipelines"
  bq_sp_sqlx_hash     = filemd5(local.yaml_config["bq_stored_procedure_path"])
}

data "google_project" "project" {
  project_id = local.project_id
}


data "external" "check_for_bq_dataset" {
  program = ["python", "${path.module}/terraform/check_for_bq_dataset.py"]
  query = {
    gcp_project_id = local.project_id
    bq_dataset_id  = local.dataset_id
  }
}

locals {
  project_number  = data.google_project.project.number
  service_account = "${local.project_number}-compute@developer.gserviceaccount.com"
  create_dataset  = tonumber(data.external.check_for_bq_dataset.result.create_dataset)
}


provider "google" {
  project = local.project_id
  region  = local.region
}

provider "jinja" {
  delimiters {
    // The values below are the defaults
    variable_start = "{{"
    variable_end   = "}}"
    block_start    = "{%"
    block_end      = "%}"
    comment_start  = "{#"
    comment_end    = "#}"
  }
  strict_undefined = true
}

# Validate configuration YAML
resource "null_resource" "validate_configuration_yaml" {
  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py -f ${var.config_path} -v"
  }
}

# Enable all necessary GCP APIs
resource "google_project_service" "aiplatform" {
  project                    = local.project_id
  service                    = "aiplatform.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}
resource "google_project_service" "analyticsadmin" {
  project                    = local.project_id
  service                    = "analyticsadmin.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}
resource "google_project_service" "artifactregistry" {
  project                    = local.project_id
  service                    = "artifactregistry.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}
resource "google_project_service" "bigquery" {
  project                    = local.project_id
  service                    = "bigquery.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}
resource "google_project_service" "cloudbuild" {
  project                    = local.project_id
  service                    = "cloudbuild.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}
resource "google_project_service" "cloudresourcemanager" {
  project                    = local.project_id
  service                    = "cloudresourcemanager.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}
resource "google_project_service" "compute" {
  project                    = local.project_id
  service                    = "compute.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_project_service" "notebooks" {
  project                    = local.project_id
  service                    = "notebooks.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_project_service" "serviceusage" {
  project                    = local.project_id
  service                    = "serviceusage.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

resource "google_project_service" "storage" {
  project                    = local.project_id
  service                    = "storage-component.googleapis.com"
  disable_on_destroy         = false
  disable_dependent_services = false
}

# Create BigQuery Dataset
resource "google_bigquery_dataset" "andy_pipeline" {
  count = local.create_dataset

  dataset_id                 = local.dataset_id
  friendly_name              = local.dataset_id
  description                = "This is a BQ dataset for our AnDY pipelines' training/inference sets."
  delete_contents_on_destroy = true

  labels = {
    vai-mlops = "general"
  }

  depends_on = [google_project_service.bigquery]
}

# Create BigQuery Stored Procedure
data "jinja_template" "create_dataset_sql" {
  template = "${path.module}/${local.yaml_config["bq_stored_procedure_path"]}"
  context {
    type = "yaml"
    data = "${path.module}/config.yaml"
  }
}

resource "google_bigquery_job" "create_dataset" {
  job_id = "create_dataset_procedure_job_${local.bq_sp_sqlx_hash}_${formatdate("YYYYMMDDhhmmss", timestamp())}"

  query {
    query = data.jinja_template.create_dataset_sql.result

    create_disposition = ""
    write_disposition  = ""
  }

  labels = {
    vai-mlops = "general"
  }

  depends_on = [google_bigquery_dataset.andy_pipeline]
}

resource "google_bigquery_job" "create_dataset_test_training_dataset" {
  job_id = "create_dataset_test_training_dataset_${local.bq_sp_sqlx_hash}_${formatdate("YYYYMMDDhhmmss", timestamp())}"

  query {
    query = <<EOT
    DECLARE table_name STRING DEFAULT '${local.dataset_id}.tmp_training';
    DECLARE date_start DATE DEFAULT '${formatdate("YYYY-MM-DD", timeadd(timestamp(), "-240h"))}';
    DECLARE date_end DATE DEFAULT '${formatdate("YYYY-MM-DD", timeadd(timestamp(), "-240h"))}';
    DECLARE mode STRING DEFAULT 'TRAINING';
    CALL `${local.project_id}.${local.dataset_id}.create_dataset`(table_name, date_start, date_end, mode);
    EOT

    create_disposition = ""
    write_disposition  = ""
  }

  labels = {
    vai-mlops = "general"
  }

  depends_on = [google_bigquery_job.create_dataset]
}

resource "google_bigquery_job" "create_dataset_test_inference_dataset" {
  job_id = "create_dataset_test_inference_dataset_${local.bq_sp_sqlx_hash}_${formatdate("YYYYMMDDhhmmss", timestamp())}"

  query {
    query = <<EOT
    DECLARE table_name STRING DEFAULT '${local.dataset_id}.tmp_inference';
    DECLARE date_start DATE DEFAULT '${formatdate("YYYY-MM-DD", timeadd(timestamp(), "-240h"))}';
    DECLARE date_end DATE DEFAULT '${formatdate("YYYY-MM-DD", timeadd(timestamp(), "-240h"))}';
    DECLARE mode STRING DEFAULT 'INFERENCE';
    CALL `${local.project_id}.${local.dataset_id}.create_dataset`(table_name, date_start, date_end, mode);
    EOT

    create_disposition = ""
    write_disposition  = ""
  }

  labels = {
    vai-mlops = "general"
  }

  depends_on = [google_bigquery_job.create_dataset, google_bigquery_job.create_dataset_test_training_dataset]
}

resource "null_resource" "validate_create_dataset_routine" {
  triggers = {
    config_change = local.yaml_config_hash
    sp_change = local.bq_sp_sqlx_hash
  }
  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py -f ${var.config_path} -vcdr"
  }

  depends_on = [
    google_bigquery_job.create_dataset,
    google_bigquery_job.create_dataset_test_training_dataset,
    google_bigquery_job.create_dataset_test_training_dataset
  ]
}

# Vertex AI Docker Image Deploy
resource "google_artifact_registry_repository" "vai_artifact_repo" {
  location      = local.region
  repository_id = local.repo_name_docker
  format        = "DOCKER"
  labels = {
    vai-mlops = "general"
  }
  depends_on = [google_project_service.artifactregistry, google_project_service.aiplatform]
}

resource "terraform_data" "cmd_cloud_builds_submit" {
  provisioner "local-exec" {
    command = "gcloud builds submit --project ${local.project_id} --region=${local.region} --tag ${local.gcr_docker} --quiet"
  }
  depends_on = [google_artifact_registry_repository.vai_artifact_repo, google_project_service.cloudbuild]
}

# Compile and Deploy Pipelines
resource "google_storage_bucket" "pipelines_bucket" {
  name          = local.bucket_uri
  location      = local.region
  force_destroy = true
  labels = {
    vai-mlops = "general"
  }
  depends_on = [terraform_data.cmd_cloud_builds_submit, google_project_service.storage]
}

resource "google_storage_bucket_iam_member" "member" {
  bucket = google_storage_bucket.pipelines_bucket.name
  role   = "roles/storage.admin"
  member = "serviceAccount:${local.service_account}"

  depends_on = [google_storage_bucket.pipelines_bucket]
}

resource "google_artifact_registry_repository" "pipelines_artifact_repo" {
  location      = local.region
  repository_id = local.repo_name_pipelines
  format        = "KFP"
  labels = {
    vai-mlops = "general"
  }
  depends_on = [google_storage_bucket.pipelines_bucket]
}

# Compile and Push Training Pipeline 
resource "null_resource" "compile_training" {
  count = lookup(local.yaml_config, "training", false) != false ? 1 : 0
  triggers = {
    config_change = local.yaml_config_hash
  }
  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py -f ${var.config_path} -t -c"
  }
  provisioner "local-exec" {
    when    = destroy
    command = "rm -f ${path.module}/training_pipeline.yaml"
  }
  depends_on = [google_artifact_registry_repository.pipelines_artifact_repo]
}

resource "null_resource" "deploy_training" {
  count = lookup(local.yaml_config, "training", false) != false ? 1 : 0
  triggers = {
    config_change = local.yaml_config_hash
  }
  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py -f ${var.config_path} -t -d"
  }
  depends_on = [null_resource.compile_training]
}

resource "null_resource" "schedule_training" {
  count = lookup(local.yaml_config, "training", false) != false ? 1 : 0
  triggers = {
    config_change = local.yaml_config_hash
  }
  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py -f ${var.config_path} -t -s"
  }
  provisioner "local-exec" {
    when    = destroy
    command = "python ${path.module}/terraform/helpers.py -t -e"
  }
  depends_on = [null_resource.deploy_training]
}

# Compile and Push Prediction Pipeline 
resource "null_resource" "compile_prediction" {
  triggers = {
    config_change = local.yaml_config_hash
  }
  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py -f ${var.config_path} -p -c"
  }
  provisioner "local-exec" {
    when    = destroy
    command = "rm -f ${path.module}/prediction_pipeline.yaml"
  }
  depends_on = [google_artifact_registry_repository.pipelines_artifact_repo]

}

resource "null_resource" "deploy_prediction" {
  triggers = {
    config_change = local.yaml_config_hash
  }
  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py -f ${var.config_path} -p -d"
  }
  depends_on = [null_resource.compile_prediction]
}

resource "null_resource" "schedule_prediction" {
  triggers = {
    config_change = local.yaml_config_hash
  }
  provisioner "local-exec" {
    command = "python ${path.module}/terraform/helpers.py -f ${var.config_path} -p -s"
  }
  provisioner "local-exec" {
    when    = destroy
    command = "python ${path.module}/terraform/helpers.py  -p -e"
  }
  depends_on = [null_resource.deploy_prediction]
}

resource "null_resource" "ga4_setup" {
  count = lookup(lookup(lookup(local.yaml_config, "activation", {}), "ga4mp", {}), "create_ga4_setup", false) ? 1 : 0
  provisioner "local-exec" {
    command = "python ${path.module}/terraform/ga4_setup.py --config_path ${var.config_path} -c"
  }
  provisioner "local-exec" {
    when    = destroy
    command = "python ${path.module}/terraform/ga4_setup.py -d"
  }
  depends_on = [null_resource.compile_prediction]
}

terraform {
  required_version = ">= 1.5"

  required_providers {
    kind = {
      source  = "tehcyx/kind"
      version = "~> 0.9"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.9"
    }
  }
}

resource "kind_cluster" "sluice" {
  name           = "sluice"
  node_image     = var.kind_node_image
  wait_for_ready = true

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role = "control-plane"
    }
  }
}

provider "helm" {
  kubernetes {
    config_path = kind_cluster.sluice.kubeconfig_path
  }
}

resource "helm_release" "sluice" {
  name  = "sluice"
  chart = "${path.module}/../helm/sluice"

  set {
    name  = "kafka.partitions"
    value = var.partitions
  }
  set {
    name  = "gateway.replicas"
    value = var.gateway_replicas
  }
  set {
    name  = "consumer.replicas"
    value = var.consumer_replicas
  }
  set {
    name  = "dataInfra.hostGatewayIP"
    value = var.host_gateway_ip
  }

  depends_on = [kind_cluster.sluice]
}

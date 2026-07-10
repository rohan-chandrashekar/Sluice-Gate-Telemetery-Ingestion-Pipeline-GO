output "kubeconfig_path" {
  description = "Path to the kubeconfig for the kind cluster this created"
  value       = kind_cluster.sluice.kubeconfig_path
}

output "cluster_name" {
  value = kind_cluster.sluice.name
}

variable "partitions" {
  description = "Kafka partition count for the telemetry topic"
  type        = number
  default     = 12
}

variable "gateway_replicas" {
  description = "Number of gateway pod replicas"
  type        = number
  default     = 1
}

variable "consumer_replicas" {
  description = "Number of consumer pod replicas - this is the variable scripts/run_phase4.sh sweeps over {1,2,4}"
  type        = number
  default     = 1
}

variable "host_gateway_ip" {
  description = "IP the kind node containers use to reach docker-compose's host-published Kafka/TimescaleDB/Redis ports. Find with: docker network inspect kind --format '{{(index .IPAM.Config 0).Gateway}}'"
  type        = string
  default     = "172.18.0.1"
}

variable "kind_node_image" {
  description = <<-EOT
    kindest/node image tag. Pinned below v1.31 deliberately: this machine runs the legacy
    cgroup v1 hierarchy (not v2/unified), and kubelet in Kubernetes 1.31+ kindest/node images
    hard-fails with "kubelet is configured to not run on a host using cgroup v1" - confirmed by
    direct kubelet journal inspection on this host. v1.29.8 still runs under cgroup v1 (with a
    deprecation warning, not a failure). If your host runs cgroup v2 (check with
    `stat -fc %T /sys/fs/cgroup/` - "cgroup2fs" means v2), you can drop this pin.
  EOT
  type        = string
  default     = "kindest/node:v1.29.8"
}

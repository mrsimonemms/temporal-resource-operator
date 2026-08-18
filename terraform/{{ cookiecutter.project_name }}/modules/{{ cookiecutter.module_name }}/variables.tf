variable "name" {
  type        = string
  description = "Name of the project"
}

variable "workspace" {
  type        = string
  description = "Terraform workspace name"
  default     = "default"
}

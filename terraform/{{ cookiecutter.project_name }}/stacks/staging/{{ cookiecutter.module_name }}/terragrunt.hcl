terraform {
  source = "../../../modules/${basename(get_terragrunt_dir())}"
}

include {
  path = "../../common.hcl"
}

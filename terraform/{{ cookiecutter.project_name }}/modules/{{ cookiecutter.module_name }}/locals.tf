locals {
  name_format = join("-", [
    "{{ cookiecutter.module_name }}",
    var.name,
    "%s", # resource name
    local.workspace_name
  ]) # use `format(local.name_format, "<name>")` to use this
  workspace_name = replace(var.workspace, "/[\\W]/", "") # alphanumeric workspace name
}

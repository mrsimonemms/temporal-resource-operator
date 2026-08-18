# {{ cookiecutter.project_name }}

{{ cookiecutter.description }}

<!-- toc -->

* [Contributing](#contributing)
  * [Open in a container](#open-in-a-container)
  * [Commit style](#commit-style)

<!-- Regenerate with "pre-commit run -a markdown-toc" -->

<!-- tocstop -->

## Contributing

Set the Terraform Cloud token to an environment variable called
`TF_TOKEN_app_terraform_io`. By default, this should be set in a file called
`.envrc`

### Open in a container

* [Open in a container](https://code.visualstudio.com/docs/devcontainers/containers)

### Commit style

All commits must be done in the [Conventional Commit](https://www.conventionalcommits.org)
format.

```git
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

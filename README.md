# New

A blank project for getting started.

<!-- toc -->

* [Templates](#templates)
* [Contributing](#contributing)
  * [Open in a container](#open-in-a-container)
  * [Commit style](#commit-style)

<!-- Regenerate with "pre-commit run -a markdown-toc" -->

<!-- tocstop -->

## Templates

To create a new repo, either run:
* `make <dir>`
* `cruft create https://github.com/mrsimonemms/new --skip=.git --directory <dir>`

The available templates are:
* [blank](/blank/) - Simple blank project. If unsure, use this one
* [golang-cobra](/golang-cobra/) - Golang Cobra starter project
* [js](/js/) - JavaScript starter project
* [monorepo](/monorepo/) - Monorepo starter project designed for NodeJS and Golang
  projects
* [terraform](/terraform/) - Terraform starter project

## Contributing

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

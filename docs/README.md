# Documentation For Axoniq Framework.

This folder contains the docs related to the Axon Framework project. The docs in this folder are written as part of
the [AxonIQ Docs](https://docs.axoniq.io), and are written in AsciiDoc and built with Antora.

This repository holds the sources for the
following [Antora Components](https://docs.antora.org/antora/latest/component-name-and-version/) in the corresponding
folders:

- [
  `./deadlines-guide/`](deadlines-guide): Legacy Deadlines Guide, currently not actively linked in the reference
- [
  `./reference-guide/`](reference-guide) : [The Axon Framework reference guide](https://docs.axoniq.io/axon-framework-reference/latest)

## Distributed content

The Axon Framework reference guide is implemented as an [Antora distributed component version](https://docs.antora.org/antora/latest/distributed-component-version/). 
This means the reference-guide documentation sources in this repository are merged together with the reference-guide documentation sources in the [Axon Framework repository](https://github.com/AxonIQ/AxonFramework). 
To provide a unified documentation to our users use the following pattern to inline Axoniq Framework documentation content in Axon Framework documentation pages:

The content page in the AxonFramework reference guide:
```asciidoc
= Command Handling Infrastructure
// ...

== Simple Command Bus
// ...

ifdef::advanced-framework[]
include::distributed-messaging:partial$distributed-command-bus.adoc[]
endif::[]
```

The `distributed-command-bus.adoc` partial in the AxoniqFramework reference guide doc sources:
```asciidoc
// all content is inlined as is, thus we can have heading level 2 here
== Distributed command bus
// ...
```

Due to the integrated nature of the reference guide, when editing Axoniq Framework documentation you will most probably always need to edit the Axon Framework reference guide as well. 

The recommended workflow to work across bot repositories is to 
* either manually check out [Axon Framework repository](https://github.com/AxonIQ/AxonFramework) as sibling to the root of this repository, or
* run `docs/_playbook/localLinks/update.sh`, which also checks out the Axon Framework repository, 

and then editing and commiting the Axon Framework documentation files from
`docs/_playbook/localLinks/AxonFramework/docs` to have proper linking support for `xref` and `include` in most AsciiDoc
aware editors. When building the docs be sure to use the `playbook-dev.yaml` instead of `playbook.yaml` (which defaults
to the current main branch of the Axon Framework reference).

## Contributing to the docs

You are welcome to contribute to these docs. Whether you want to fix a typo, or you find something missing, something
that it's not clear or can be improved, or even if you want to write an entire piece of docs to illustrate something
that could help others to understand the use of Axon Framework, you are more than welcome to send a Pull Request to this
github repository.

## Building and previewing these docs locally

If you want to build and explore the docs locally (because you have made changes or before contributing), you can use
the Antora build file in the [`./_playbook`](_playbook) folder following these steps:

### Install dependencies

1. Make sure you have [Node](https://nodejs.org/en/download) (a LTS version is preferred)
   and [Vale](https://vale.sh/docs/install) installed in your system.
2. CD to the [`./_playbook`](_playbook) folder.
3. Run `npm install.` to install Antora

### Build and preview

1. Run `node watch.js`. Antora will generate the set of static html files under [
   `./_playbook/build/site`](_playbook/build/site), which will be served using [Express.js}(https://expressjs.com/) via http://localhost:3000
2. Each of the existing Antora Components will be available under a different folder in the webserver's root (named
   after the component name in the corresponding `antora.yml`).
3. Antora determines the component
   version [based on the git refname](https://docs.antora.org/antora/latest/component-version-key/#refname) according to
   the configuration in the `antory.yml`. The rendered pages are available inside a subdirectory of the component's
   version directory, named after the component's version (depending on the git branch you have checked out).
4. rebuilds will trigger automatically when changing documentation files

## Publishing documentation

The `.github/workflows/docs.yml` github workflow in this repository and
the [Axon Framework repository](https://github.com/AxonIQ/AxonFramework) repository trigger the publish workflow in
the [axoniq-library-site repo](https://github.com/AxonIQ/axoniq-library-site) for pushes on main /
release-branches, so whenever either of the repos change the documentation is published, which allows for independent
adjustments in both locations. 

In case there is a documentation change spanning both repositories, be sure to merge the
AxoniqFramework repository first, as it's new contents are most likely referenced from the AxonFramework repo and should
be present when the doc changes in AxonFramework depending on AxoniqFramework content are going to a branch that gets
published.
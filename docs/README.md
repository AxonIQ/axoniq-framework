# Documentation For Axoniq Framework.

This folder contains the docs related to the advanced Axon Framework features. The docs in this folder are written as part of the [AxonIQ Documentation](https://docs.axoniq.io), and are [written in AsciiDoc and built with Antora.](https://docs.axoniq.io/contribution_guide/overview/platform.html)

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

## Contributing to the docs.

You are welcome to contribute to these docs. Whether you want to fix a typo, or you find something missing, something that it's not clear or can be improved, or even if you want to write an entire piece of docs to illustrate something that could help others to understand the use of the Bike Rental App, you are more than welcome to send a Pull Request to this github repository. Just make sure you follow the guidelines explained in [AxonIQ Library Contribution Guide](https://docs.axoniq.io/contribution_guide/index.html)

## Building and testing these docs locally.

If you want to build and explore the docs locally (because you have made changes or before contributing), you can use the Antora's build file in `docs/_playbook` folder.

You can check the [detailed information on how the process to build the docs works](https://docs.axoniq.io/contribution_guide/overview/build.html), but in short, all you have to do is: 

1. Make sure you have Node (a LTS version is preferred), Antora and Vale installed in your system.
2. CD to the `docs/_playbook` folder.
3. Run `npm install.`
4. Run `npx antora playbook-dev.yaml`. Antora will generate the set of static html files under `docs/_playbook/build/site`
5. Move to `docs/_playbook/build/site` and execute some local http server to serve files in that directory. For example by executing `python3 -m http.server 8070`
6. Open your browser and go to `http://localhost:8070`. You should be able to navigate the local version of the docs.

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
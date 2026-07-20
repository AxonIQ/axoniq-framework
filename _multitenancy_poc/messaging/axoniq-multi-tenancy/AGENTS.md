# Agents instructions for multi tenancy

- only do changes in ~/IdeaProjects/axoniq/axoniq-framework, dont modify or create files anywhere else without explicit instructions
- inside messaging/axoniq-multi-tenancy/src is the main source folder
  -  we will also work in ~/IdeaProjects/axoniq/axoniq-framework/examples
  -  we will also work in ~/IdeaProjects/axoniq/axoniq-framework/dependency-injection/spring-boot-starter
  -  we will also work in ~/IdeaProjects/axoniq/axoniq-framework/integration-tests
- we are targeting a new commercial feature for axoniq-framework only
- for reference, the _old folder contains a complete git copy of the axon-framework 4 extension library
  - use this for inspiration and reference
  - when you find that a concept that exists in _old is still applicable, do a git mv to the src folder instead of recreating the files, so we keep the git history if possible
  - when you take inspirations from _old or _tmp but do not mv the complete file, respect the author tags on the file
  - All files should have a valid javadoc, stating @since 5.3.0 and at least my @author tag: "Jan Galinski" (plus the ones copied from _old/_tmp sources)

- all new files must contain the license header
- all new packages must contain a package-info.java with jspecify NullMarked 
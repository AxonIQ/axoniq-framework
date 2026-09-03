# Refactoring / Integration
v integrate `runtime` (rename the module to `*-engine`), `spring-boot`, `dsl`, `test` as maven modules underneath of `workflow`
v double-check maven coordinates
v change package structure !!!
v integrate example/integrationtests
v integrate docs
v move examples out -> put them into axon-framework repo as a separate commit (just copy) -> prefix with workflow
v change @since -> use LLM
v validate build
v integrate archunit / checkstyle (validate)
v integrate architecture docs/ADRs -> spec
v integrate coverage

# After 
- copy files from TLA+/DST branch -> separate PR
- create patch for WorkflowManager -> separate PR


# Observations

- junit4 is on classpath
- spring boot version is 3.5.16
- docs integration -> help needed
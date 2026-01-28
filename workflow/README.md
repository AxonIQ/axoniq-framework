# Axon Flow Specification

**Specification-first repository** for a Workflow (Flow) Engine built on Axon Framework 5.

> All specifications are reviewed and approved via PR before any implementation begins.
> This ensures team alignment on *what* and *how* we build.

## Why Workflows?

Business processes like order fulfillment, loan applications, and customer onboarding need:
- **Audit trails** - Every action tracked for compliance
- **Reliability** - Survive crashes, resume from where you left off
- **Scalability** - Handle thousands of concurrent processes

Event sourcing delivers all of this, but comes with significant complexity: command handlers, event handlers, saga patterns, and thousands of lines of state management.

**Our goal:** Let developers write simple, imperative business logic while the engine handles event sourcing complexity automatically.

```
// Developer writes this
loanWorkflow = workflow {
    creditScore = execute("check-credit") { creditBureau.getScore(customerId) }

    if (creditScore < 650) return failed("Credit too low")

    documents = waitForEvent("DocumentsUploaded", timeout = 7.days)
    approval = execute("underwriter-review") { underwriter.review(creditScore, documents) }

    return success(approval.contractId)
}

// Engine handles: events, retries, timeouts, state persistence, crash recovery
```

## Contributing

1. Create a PR with spec changes
2. Team reviews and discusses
3. Merge when consensus is reached
4. Implement based on approved spec

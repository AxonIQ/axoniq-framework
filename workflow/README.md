# Axon Framework - Workflow Extension

Repository for a Workflow (Flow) Engine built on Axon Framework 5.

## Why Workflows?

Business processes like order fulfillment, loan applications, and customer onboarding need:

- **Audit trails** - Every action tracked for compliance
- **Reliability** - Survive crashes, resume from where you left off
- **Scalability** - Handle thousands of concurrent processes

Event sourcing delivers all of this, but comes with significant complexity: command handlers, event handlers, saga
patterns, and thousands of lines of state management.

**Our goal:** Let developers write simple, imperative business logic while the engine handles event sourcing complexity
automatically.

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

## Documentation

To preview the docs locally with live reload:

```bash
cd docs/_playbook && npm install && node watch.js
```

Then open [http://localhost:3001](http://localhost:3001).

## Licensing

Axon Framework consists out of a number of different modules, each with different licenses. Modules residing under the
Axon Framework GitHub organization, with group identifier org.axonframework, are Apache 2 licensed. Modules under the
Axoniq GitHub organization, with group identifier io.axoniq, are licensed under Axoniq's proprietary license.

Please refer to individual module's [LICENSE](LICENSE.txt) file for details.
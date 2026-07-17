package snapshotting.index.immutableclass;

// tag::account-class[]
public class Account {
    private final String accountId;
    private final int balance;

    public Account(String accountId, int balance) {
        this.accountId = accountId;
        this.balance = balance;
    }

    public String getAccountId() { return accountId; }
    public int getBalance() { return balance; }
}
// end::account-class[]

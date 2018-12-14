# dbbft
implement bft algorithm in sqlite db.

I think the core of bft algorithm is message analysis. The action what we should do depends on what messages we have now.

So we can implement the process logic as insert trigger.

When insert a message into db, trigger will be invoked automatically and output the action.

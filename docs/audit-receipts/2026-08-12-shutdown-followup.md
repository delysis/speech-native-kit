# Speech shutdown follow-up receipt — 2026-08-12

This follow-up starts from merged `main` commit
`4c8da9f5a98f291041291dca7e2bde00f410824e`. The merge was green locally,
in pull-request CI, and in post-merge CI, but subsequent independent source
review found correctness gaps that those gates did not cover:

- the first host or backend shutdown caller still owned coordinator progress,
  so cancelling that caller could strand followers in `Quiescing`;
- a backend shutdown panic could prevent the host from publishing a retained
  terminal result;
- Apple and Parakeet shutdown waiters registered `Notify` futures without
  enabling them before checking state;
- `TaskSupervisor` retained exact worker-ID histories for process lifetime;
- an ID-only cancellation racing request-ID reuse treated a natural generation
  change as host corruption.

This candidate moves host, Apple, and Parakeet shutdown work into detached
coordinators. Caller cancellation no longer owns progress. Inner coordinator
and backend shutdown panics become retained errors, and every waiter observes a
published terminal result. All wait loops pin and enable their notification
before checking state.

`TaskSupervisor` now retains exact IDs only for a bounded work epoch. Whenever
a new task is admitted from idle, the previous epoch's strings are cleared;
concurrent tasks accumulate in the same epoch, and joined IDs are recorded only
after actual handles are awaited. Process-lifetime work remains represented by
checked admitted/completed counters. At quiescence, the latest exact epoch is
frozen for the shutdown contract. Ten-thousand sequential generic tasks assert
a one-entry bound; one-thousand concurrent Apple and Parakeet workers assert
storage no larger than that bounded concurrency epoch.

Public ID-only cancellation rechecks the captured route generation against the
current operation lease. A request-ID reuse race returns zero without marking
the host faulty. Ticket cancellation remains generation-scoped and cannot
cancel a newer request.

The earlier green merge and post-merge W1 run are historical evidence only;
they do not establish acceptance of these follow-up properties. Acceptance
requires this candidate's full local gates, pull-request CI, merge, and green
post-merge `main` runs.

# DRAFT: email to the Caerus authors (Tony sends; Cloudflare thread edits)

Status: draft, not sent. One placeholder to fill from the single-region run:
[SINGLE-REGION RESULT]. Recipients: Joshua Hildred, Khuzaima Daudjee;
cc Michael Abebe.

---

Subject: Questions about region failure in Caerus (PVLDB 17(3))

Dear Joshua, Khuzaima, and Michael,

I have been working on a project that looks at how replicated systems behave when part of the system becomes unreachable, and in the course of that work I read your Caerus paper and then built and ran the artifact you released (commit aaf0eb6 on GitHub). Since I would rather ask you directly than guess at what you intended, I am writing before anything we have found goes into a paper; it is entirely possible that some of what follows is answered by a version of the code you have not released, in which case I would be grateful to know that.

What we did was fairly simple: we ran six regions, one machine each, on a single host with the inter-region delays from your Table 1 emulated, using the MovR workload at the settings in your README, and then killed regions and watched what the survivors did. (We did have to make two changes to run it this way: we removed the thread pinning, since six co-located processes otherwise share 23 cores, and we raised RUNTIME so that the runs lasted long enough to observe; neither touches sequencing, merging, or admission.) When two of the six regions die, every surviving region stops committing within a few seconds, regardless of the fraction of multi-region transactions, *including* a workload with no multi-region transactions at all, and does not resume over the following ten minutes. [SINGLE-REGION RESULT] The conflict graph is empty after the stop, so this does not appear to be stuck transactions; attaching gdb shows the connection multiplexer thread blocked in a ZMQ send (connection.cc:225, with no ZMQ_DONTWAIT), which, since that one thread carries all of a process's traffic, would stop everything. We have not confirmed which peer the blocked send is addressed to, and we have only tested killed processes, not a silent network failure, so we may be misreading what we see.

Reading the source, we also could not find the region-level replication and recovery described in Section 4.7.1 (we did find the within-region acknowledgement quorum), and storage, including the input log, appears to be in memory only, so a restart begins from freshly initialized state. Our questions, which a colleague and I put together, are these:

1. Is the region-level recovery of Section 4.7.1 implemented anywhere, for instance in an unreleased branch, and is there a durable input log in that version?
2. Is the blocking-send behaviour on a dead peer known to you, and is it what you would expect from the released prototype?
3. In step 3 of the recovery protocol, does "each region exchanges its copy" mean every live region, or is a set of N-K reports sufficient to proceed?
4. Does a non-sequencer region execute a transaction as soon as it appears in all relevant partial sequences, before K regions hold it, or is that execution provisional?
5. What orders two concurrent recoveries for the same failed region (an epoch, ballot, or term)?
6. Can a recovered region rejoin, and how are its late messages rejected?

Our scripts, logs, and the pre-registration we wrote before each experiment are available, and I would be happy to share them or to run anything you suggest; if we have configured something incorrectly, we would much rather know now. Since I spend a fair amount of my time thinking about how recovery paths get exercised (or more often, do not), I should say that none of this is meant as a criticism of the work; recovery is almost never evaluated in systems papers, which is precisely the reason we are looking at it.

Thank you for your time, and for releasing the code in the first place, which is what made any of this possible.

Best regards,

Tony Mason

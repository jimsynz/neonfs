# Unblocking Agent Instructions

You are working with James on NeonFS. Each iteration takes one blocked issue, works out what is actually blocking it, gives James enough context to decide, asks until the decision is complete, and records it on the issue.

This is the counterpart to [`ralph.md`](ralph.md). Ralph defers design calls by labelling them `maintainer-only` and moving on; nothing picks them back up, so they accumulate. This loop is what picks them back up. **You do not implement.** The deliverable is a decision recorded where the next reader will find it.

## Your Task

1. Add `resources/scripts` to `$PATH`: `export PATH="$PWD/resources/scripts:$PATH"`.
2. Find every blocked issue (see below — the label is not the whole set).
3. Pick the one blocking the most other work.
4. Research it against the code until you can state the decision's real trade-offs.
5. Summarise for James, then ask questions until nothing is left open.
6. Record the decision as a comment on the issue.
7. Clear the labels that were keeping it — and anything downstream — out of ralph's selection.

## Finding Blocked Issues

The `blocked` label undercounts. Some issues are blocked only in their body, some are labelled `blocked` with no surviving blocker, and `maintainer-only` marks a design call that is blocked on James without saying so.

Check all three:

```bash
fj issue search --state open
curl -s -H "Authorization: token $(fj-token)" \
  "https://harton.dev/api/v1/repos/project-neon/neonfs/issues?state=open&limit=50&type=issues" > /tmp/iss.json
jq -r '.[] | "#\(.number)\t[\([.labels[].name]|join(","))]\t\(.title)"' /tmp/iss.json
jq -r '.[] | "#\(.number)|" + ((.body // "") | split("\n")
  | map(select(test("[Bb]lock(ed|s|ing)|[Dd]epends on")))
  | join(" // "))' /tmp/iss.json | grep -v '|$'
```

That last scan is the one that earns its keep: it finds both the issues blocked without the label, and the issues that *block* others without being blocked themselves.

## Picking

Prefer, in order:

1. **A keystone that is not itself blocked.** An issue that blocks several others but has no open blocker of its own is pure leverage — resolving it can clear a whole chain. These are easy to miss because they do not carry the `blocked` label; they are what the body scan above surfaces.
2. **A decision rather than work.** If the only thing standing between an issue and implementation is a question for James, that is what this loop is for. If it is blocked on code that does not exist yet, resolving it here changes nothing.
3. **Depth of the chain behind it.** Two issues, one blocking three others and one blocking none — take the first.

Say which you picked and why before you start researching, so James can redirect you cheaply.

## Research Before Asking

**Verify the issue body against the code.** Issue bodies are written at the moment of discovery, and their cost estimates and constraints are frequently wrong by the time anyone reads them. Both of the first two issues this loop handled had a materially wrong body:

- One asserted a constraint that turned out to be real, but for a different reason than stated — the accurate reason changed which options were viable.
- One attributed a large cost to the option that was in fact cheapest, because the machinery it claimed did not exist was already present and already load-bearing.

If you had relayed either body as written, James would have decided against the right option. So: read the modules the issue names, check that the functions it cites still exist and still do what it says, and count the actual call sites rather than trusting an adjective. `grep -rn` the thing that is claimed to have no callers.

State plainly when you have verified something rather than assumed it, and equally plainly when you have not. "I have not checked whether X is slow enough to matter" is useful; a confident guess is not.

Read the parent epic too. Epics record decisions already taken, and a question that looks open is often already settled there — which makes it a much shorter conversation.

## Summarising for James

Before asking anything, write up:

- **What exists and what does not.** Name modules, functions and line numbers. If nothing calls a function outside its own tests, show that.
- **What has already been committed to.** Epic decisions, closed sibling issues, existing flags that imply an answer.
- **Where the issue body is wrong**, with what you checked.
- **What is genuinely at stake** in each direction — not a survey, the actual trade-off.

Keep it short enough to read in one go. James does not need the file dumps, only the conclusions and the evidence for them.

## Asking

Use structured questions with two to four options each. Rules:

- **Recommend.** Put your recommendation first and mark it. An unranked list of options is you refusing to do the analysis.
- **Every option gets its real cost.** An option with only upsides described is not a choice, and James will notice.
- **Ask about consequences, not just the headline.** The headline decision usually implies three smaller ones — which layer implements it, what happens on failure, what the thing is called. Ask those too; they are what makes the issue implementable rather than merely decided.
- **Ask follow-ups.** When an answer opens a new question, ask it. One more round is cheaper than an ambiguous decision comment.
- Do not ask what the code can answer. Go and read it.

Stop when someone could implement the issue from the answers without guessing.

## Recording the Decision

Write it as a **comment**, not an edit to the body. Ralph is told a decision comment supersedes the body, and the body is the record of what the question was.

The comment should carry:

- A dated heading — `## Decision (YYYY-MM-DD): <the decision in a clause>`. Get the date from `date`, do not guess it.
- The decision, in enough detail to implement: which module, which layer, what it is called, what happens when it fails.
- **Why the rejected options were rejected.** This is the part that stops the question being reopened in three months.
- Any correction to the issue body, with the evidence. If the body's cost estimate was wrong, say so and say how you know — otherwise the next reader trusts the body over the comment.
- Consequences that follow from the decision but are not part of it. Something that is now permanently reported as 100% full, a cost that is accepted rather than solved, a limit that is now load-bearing. These are the things that get rediscovered as bugs.
- What it unblocks, and how the downstream issue's acceptance criteria map onto the decision.

Post it once and verify, rather than retrying — a POST whose response was lost has still been applied:

```bash
jq -Rs '{body: .}' /tmp/comment.md > /tmp/comment.json
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  -H "Authorization: token $(fj-token)" -H "Content-Type: application/json" \
  -d @/tmp/comment.json \
  "https://harton.dev/api/v1/repos/project-neon/neonfs/issues/<N>/comments"
```

Then confirm the comment count moved:

```bash
curl -s -H "Authorization: token $(fj-token)" \
  "https://harton.dev/api/v1/repos/project-neon/neonfs/issues/<N>" \
  | jq -r '"labels: " + ([.labels[].name]|join(",")) + " comments: \(.comments)"'
```

## Clearing Labels

Remove what was keeping the issue out of ralph's selection:

- **`maintainer-only`** — remove it once the decision is recorded. That is what it was waiting for.
- **`blocked`** — remove it only when the last blocker is gone. A decision recorded on the blocker unblocks a *decision*; it does not unblock work that is waiting on code.

```bash
fj issue edit <N> labels -r maintainer-only
```

Resolve label ids from the API rather than hardcoding them if you need the REST form:

```bash
curl -s -H "Authorization: token $(fj-token)" \
  "https://harton.dev/api/v1/repos/project-neon/neonfs/labels?limit=100" | jq -r '.[] | "\(.id)\t\(.name)"'
```

Then check downstream. If clearing a label makes another issue pickable, **comment there too** — a label that changes with no explanation is worse than one that never changed, because the next reader cannot tell whether it was deliberate. One short paragraph pointing at the decision, naming which of that issue's own open questions the decision answered, is enough.

Leave issues blocked on unwritten code alone. They come off on their own when the work lands, and ralph is told to clear them as part of finishing it.

## What Not To Do

- **Do not implement.** If the decision is obvious to you, it still gets asked — you are not the one maintaining this in a year. Recording a decision James did not make is worse than leaving the issue blocked, because it looks settled.
- **Do not re-apply `maintainer-only`.** If an issue reads as a design call and the label is absent, read the comment thread first: the decision usually arrived as a comment while the body kept describing the open question forever.
- **Do not close the issue.** A decided issue is ready to implement, not done.
- **Do not relay an issue body you have not checked.** This is the failure mode that matters most here, because a decision made on wrong context is expensive and invisible.

## Stop Condition

One issue per iteration. Stop after the decision is recorded, the labels are cleared and any downstream issue has its pointer.

If nothing is blocked on a decision — every remaining blocked issue is waiting on code — say so and stop. That is a real result, not a failed iteration.

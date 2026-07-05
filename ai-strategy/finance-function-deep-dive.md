# Deep Dive: Generative AI in the Bank Finance & Accounting Function

**What's occurring now, and where it's heading**

*Companion to `ai-comparative-advantage-strategy.md` (Part I §4, greenfield finance). Compiled July 2026 from consulting, big tech, vendor, academic, and regulatory sources — see `research-resources.md` for the full citation list.*

---

## Why bank finance is ground zero for gen AI

Finance and accounting sit at the intersection of everything gen AI is good at: dense text (policies, standards, commentary, memos), structured data (ledgers, trial balances, reg returns), rule-following (accounting standards, prudential rules), and repetitive judgement-adjacent work (reconciliation, variance analysis, exception investigation). Accenture's task analysis found finance-heavy roles among the most exposed in banking to both automation and augmentation.

But bank finance is also the hardest place to deploy carelessly. The function's output *is* the bank's regulatory and market face: statutory accounts, APRA returns, capital ratios, provisioning under IFRS 9/AASB 9. Errors are not workflow defects; they are disclosure events. This tension — highest exposure, lowest error tolerance — shapes everything below.

The current resolution across the industry can be summarised in one pattern: **deterministic core, probabilistic edge**. Gen AI is being kept out of the calculation path (ledger postings, capital calculations, ECL models remain deterministic and validated) and deployed intensively around it — drafting, explaining, reconciling, investigating, testing, and summarising. The frontier is now moving inward, one control gate at a time.

---

## Part A — What's occurring now

### 1. The adoption picture: broad, shallow, and honest about it

Gartner's data frames the reality: **84% of finance organisations have implemented or plan to implement AI — but only 7% report high impact.** Adoption is real; transformation is rare. The most common live use cases in finance are unglamorous:

- Knowledge management / policy Q&A — 49% of finance organisations
- Accounts payable automation — 37%
- Error and anomaly detection — 34%

This matters strategically: the gap between the 84% and the 7% is the finance-function version of the MIT GenAI Divide, and it will separate finance teams the same way — those who wired AI into redesigned workflows versus those who gave accountants a chatbot.

### 2. The copilot layer at scale: the JPMorgan proof point

JPMorgan's **LLM Suite** is the reference case for what a bank can actually deploy today: ~250,000 employees with access (essentially everyone outside branches and call centres), roughly **half using it daily**, saving an estimated **3–6 hours per week**, with reported efficiency gains of 30–40% in some areas. Finance-relevant usage includes analysing earnings transcripts, comparing financial documents, drafting presentations, and synthesising data — with models from multiple providers (OpenAI, Anthropic) behind a bank-controlled compliance layer, refreshed on an eight-week cycle.

Two lessons for an Australian bank's finance function:

1. **Scale is a governance achievement, not a technology one.** JPMorgan's differentiator is the controlled environment that made bank-wide rollout acceptable to risk and regulators.
2. **Daily habit is the metric.** Half the workforce daily is metabolism; a licence count is not.

### 3. The first agentic wave: controllership is where it's landing

2025–26 marks the moment agents entered the accounting stack through the front door — embedded in the ERPs and close platforms banks already run:

- **SAP S/4HANA Cloud (2602 release)** shipped an **Accounting Accruals Agent** — the first embedded agent that autonomously proposes, calculates, and documents postable journal entries, with the accountant's role explicitly redesigned as review-and-approve.
- **Workday Illuminate** provides journal insights, ML anomaly detection, and automated audit-prep for finance customers.
- A specialist layer (BlackLine, FloQast, Numeric, Vic.ai, Trullion, Kognitos and peers) now sells agents for reconciliation, journal entries, intercompany elimination, variance analysis, and close-package preparation.
- Gartner expected **80% of ISVs to embed gen AI in enterprise applications by 2026, up from under 5% in 2024** — meaning bank finance teams are acquiring AI capability passively through their existing vendor stack, whether or not the operating model is ready for it.

The pattern across all of these: the agent does the data gathering, calculation, and documentation; the human does review, approval, and exception judgement. This is the §2 "graduated autonomy" ladder playing out in general ledger workflows.

### 4. Frontier previews: what AI-native finance functions look like

Non-bank examples show the end state more clearly than any bank yet does:

- **OpenAI's own finance team** runs a contract-reader bot that extracts terms, applies revenue-recognition logic (ASC 606/IFRS 15), and auto-generates journal entries — contributing to a finance function running at roughly **22% of the headcount of comparable tech firms**.
- **Adyen** (a regulated European payments bank — the closest comparable) spent years centralising financial data into a "Finance Data Core" that now powers automated reconciliation, accounting-memo generation, and faster reporting cycles. The sequencing lesson is pointed: **the data unification came first; the AI leverage followed.**
- Continuous-reconciliation deployments (e.g. Spendesk) run matching all month, turning the close from an event into a state — the practical front edge of the "continuous close."

These are smaller, simpler organisations than a major bank — but they are the greenfield answer to §4's question, running in production.

### 5. The audit side is transforming in parallel

The external audit — bank finance's counterparty — is being rebuilt around AI at speed:

- **EY Helix** already analyses **100% of client journal entries**, replacing sampling with full-population testing.
- **KPMG Clara** embeds AI across the audit workflow, and KPMG's **Workbench** (June 2025) is a multi-agent environment explicitly designed to mirror human audit teams.
- The **PCAOB** has published research on gen AI in audit, and Big 4 firms are launching AI-assurance service lines.
- The Big 4 are simultaneously **cutting graduate intakes** and retooling existing staff — the junior-pipeline paradox (Part II §13) arriving in the profession that feeds bank finance teams.

Implication: within a few years, a bank's auditor will test 100% of its journals with agents. A finance function still doing 5% sample-based internal control testing will be **less assured than its own external audit** — an untenable position. AI-enabled control testing stops being an innovation and becomes table stakes for credibility with the auditor and APRA alike.

### 6. What is deliberately *not* happening yet

Banks are keeping gen AI out of three places, and the restraint is correct today:

- **The ledger calculation path** — postings, capital calculations, ECL model math remain deterministic and validated. Gen AI proposes; deterministic systems compute; humans approve.
- **Externally reported numbers without human sign-off** — commentary is AI-drafted but human-owned; the accountability chain (CFO attestation, CPS 511-style accountability) is unchanged.
- **Provisioning judgement** — IFRS 9 overlays and forward-looking scenario weights remain human decisions, with AI supporting the evidence assembly and documentation. Supervisors (ECB, IMF, Basel guidance) are explicit that consistency and explainability expectations apply with full force.

---

## Part B — Impact map by sub-function

| Sub-function | Happening now | Direction of travel |
|---|---|---|
| **Financial control & close** | Reconciliation agents, accrual/journal agents (review-and-approve), close-checklist automation, anomaly detection on the GL | Continuous close: matching runs all month; month-end becomes confirmation; controllers manage an exception queue, not a task list |
| **Regulatory & statutory reporting** | Drafting assistance for notes and disclosures; regulatory-change tracking and impact triage (a sensible first AI step per Wolters Kluwer and peers); data-lineage tooling | Report production as a governed pipeline: policy-as-code rules, automated lineage from source to submitted return, explainability that lets supervisors trace every figure — turning APRA's expectations into an architecture |
| **Planning & analysis (FP&A)** | AI-drafted variance commentary and board-pack narrative; natural-language query over management data | Rolling driver-based forecasts continuously updated; scenario modelling on demand; FP&A analysts become challengers of AI-generated baselines rather than assemblers of packs |
| **Provisioning & credit finance** | Evidence assembly, documentation drafting, model-monitoring summaries for IFRS 9/AASB 9 | AI-assisted scenario narrative and overlay documentation with judgement retained by committees; full-population early-warning analytics feeding the ECL process |
| **Treasury & ALM** | Market/funding commentary drafting; policy Q&A; data assembly for ALCO packs | Scenario simulation on demand ("50bp move → funding position in minutes"); live liquidity dashboards replacing periodic packs |
| **Tax** | Research assistance, return preparation acceleration (50–70% time reduction reported for standard work in the profession) | Continuous tax position monitoring; exception-based review |
| **Internal audit & controls** | AI-assisted testing, workpaper drafting; early full-population control testing | 100% control coverage as the default; internal audit assurance over the bank's *own AI agents* becomes a core mandate — auditing the agent workforce |
| **Accounts payable / procurement** | The most mature automation domain (37% adoption); invoice extraction and matching | Fully exception-based AP; agent-to-agent invoicing as agentic-commerce standards (AP2 et al.) reach B2B |

---

## Part C — Future direction

### The three-horizon view for a bank finance function

**Horizon 1 (now–12 months): augment everything, redesign nothing yet.**
Copilots for every finance professional (policy Q&A, drafting, analysis); AI-drafted commentary with human ownership; reconciliation and anomaly agents in review-and-approve mode; regulatory-change triage. Value is real but individual — hours saved per person. The risk at this horizon is stopping here: this is precisely the "isolated productivity gains" trap of Part I §7, and the 84%-implemented/7%-impact statistic is what stopping here looks like.

**Horizon 2 (1–3 years): redesign the close and the reporting pipeline.**
The first end-to-end redesigns: continuous reconciliation collapsing the close calendar; exception-based controls with 100% screening; the regulatory return produced by a governed, lineage-complete pipeline rather than a spreadsheet relay; agentic orchestration of the close (a supervising workflow that dispatches task agents and escalates exceptions — the multi-agent pattern arriving in controllership). Finance data products with named owners become the constraint and the priority — the Adyen lesson that data unification precedes AI leverage. Internal control testing reaches parity with the external auditor's full-population approach.

**Horizon 3 (3–5+ years): finance as the bank's decision-intelligence and assurance layer.**
The scorekeeping is substantially machine-run inside a deterministic, auditable core, with agents handling assembly and humans owning judgement, attestation, and exceptions. Finance's identity completes the shift from reporter to decision-intelligence partner (§4) — and takes on a genuinely new mandate: **assurance over the bank's agent workforce**. Someone must attest that the thousands of agents acting across the bank did what they should, within mandate, with evidence. That is a controls-and-attestation discipline — finance and audit's home ground. The function that today counts money becomes the function that counts and certifies machine work.

### The shape of the function

Every credible projection points the same way: smaller transactional teams, flatter pyramids, and a higher ratio of judgement roles to processing roles. The OpenAI 22%-of-comparable-headcount data point is an extreme (young company, no legacy), but the direction is not disputed — and Part II's angles apply with full force:

- **The pipeline paradox is acute in accounting.** The profession already faces a projected shortage of **340,000 CPAs by 2030** (75% of the current CPA workforce retiring within 15 years), and Big 4 firms are cutting the graduate intakes that historically trained bank finance talent. AI is simultaneously the relief valve for the shortage and a threat to the apprenticeship that produces senior judgement. A bank that wants controllers in 2032 needs a deliberate answer now: simulation-based training, junior-senior pairing on exception queues, time-to-competence tracking (§13).
- **The trust bargain applies** (§12): finance staff will not surface the workflow they automated if the reward is a restructure. Value-sharing and redeployment-first commitments matter here as much as in the contact centre.
- **Attrition-led reshaping is available** (§18): finance functions have steady natural turnover; five years of deliberate hiring-into-the-new-shape beats one redundancy round.

### New skills, new roles

- **Reviewer literacy**: evaluating AI-produced reconciliations, journals, and commentary — knowing where the jagged frontier runs through accounting work — becomes the core professional skill.
- **Finance engineers / data-product owners**: the people who build and own the pipelines, prompts, evals, and agents — a role that barely exists in bank finance today and will be its scarcest talent.
- **Exception judgement as the job**: the work that remains human is by construction the hardest work — ambiguous, precedent-poor, consequential. Role design and performance management must reflect that the "easy volume" that used to buffer workloads is gone.
- **AI-assurance specialists**: controls professionals who test agents, not just processes.

### Risks and constraints specific to finance

1. **Hallucination is intolerable in numbers.** The mitigation pattern is architectural, not aspirational: LLMs orchestrate and explain; deterministic tools calculate; retrieval grounds every claim; nothing posts without a validated calculation path. Stanford's finding that inaccuracy is now the #1 cited AI risk (74%) is finance's reality check.
2. **Explainability is a supervisory requirement, not a preference.** For capital, provisioning, and regulatory reporting, supervisors expect every figure traceable to data and rule logic. This is why policy-as-code and lineage-complete pipelines (§2) are the enabling investment — they make AI *more* auditable than the spreadsheet chains it replaces, which is the argument that wins with APRA and the external auditor.
3. **Accountability is non-delegable.** CFO attestation and executive accountability regimes are unchanged by automation. The sign-off chain must be redesigned so humans attest to *systems and exceptions* (is the pipeline controlled, were the exceptions dispositioned) rather than pretending to have re-performed the work — attesting to what they actually supervised.
4. **Vendor-embedded AI arrives uninvited.** With ERP and close-platform vendors embedding agents by default, finance will acquire AI through upgrades whether governed or not. The AI inventory APRA expects must cover the vendor stack, not just in-house builds.
5. **The auditor is a stakeholder in the design.** Early engagement with the external auditor on AI-assisted processes avoids the year-end discovery that an AI-touched control isn't reliance-worthy.

### What good looks like by 2028 — a checkpoint list

- Close calendar shortened by half or more, with reconciliation running continuously and controllers working an exception queue.
- 100% transaction screening and full-population internal control testing — at least matching the external auditor's coverage.
- Regulatory returns produced from a lineage-complete pipeline; any figure traceable to source and rule in minutes.
- Every finance professional using an AI workbench daily (the JPMorgan bar: ~50% daily active use).
- Commentary, variance analysis, and pack narrative AI-drafted by default, human-owned always.
- A finance data-product catalogue with named owners; the top ten data products powering both reporting and AI.
- A working eval harness and promotion ladder for every finance agent (assist → recommend → act-with-review), with the agent inventory feeding APRA reporting.
- Graduate program redesigned around deliberate practice; time-to-competence measured and improving.
- Headcount shape shifting via attrition and redeployment against a published five-year plan — no surprise restructures.

---

## Discussion questions for the finance conversation

1. Our external auditor will soon test 100% of our journals with AI. How long can our own control testing stay sample-based?
2. Which comes first for us: more finance AI, or the finance data core that makes it compound? (The Adyen lesson.)
3. When an agent proposes journals and a human approves at volume, what does the approval actually attest — and is that the control we think it is?
4. If the transactional pyramid shrinks, where do our future financial controllers learn judgement — and whose job is it to make sure they do?
5. What would it take for our CFO to sign off on a continuous close — and which of those blockers are technology, and which are trust?
6. Is finance ready to become the assurance function for the bank's agent workforce — and if not finance, who?

# Submission: Pipeline Triage Agent

*Fill in each section below with concise, well-considered details. Focus on the 'why' behind your choices and the trade-offs you considered. Honest, specific reasoning is key.*

---

## 1. Architecture Decisions

**How did you structure the agent(s)?**
<!-- Describe your overall design: single agent vs. multi-agent, tool use, state management, orchestration framework. What does the control flow look like? -->

**What did you consider and reject?**
<!-- What alternative approaches did you evaluate? Why did you land where you did? -->

**Why this framework/library?**
<!-- What did you choose (LangGraph, ADK, CrewAI, raw SDK calls, etc.) and why was it the right fit for this problem? What would you do differently with a different framework? -->

---

## 2. Handling Ambiguity & Escalation

**How does your system decide when to escalate?**
<!-- Describe your escalation logic. Is it rule-based, model-driven, or a hybrid? What signals trigger it? -->

**Walk through the ambiguous scenario.**
<!-- Scenario 06: report_export succeeded with 0 rows. Explain what your agent investigates, what it concludes, and why. -->

---

## 3. Live Resolution Agent (`/resolve`)

**What tools did you give your agent?**
<!-- List the tools your /resolve agent can use (e.g. get_dag_info, read_task_logs, trigger_run, patch_dag_file). Why did you choose these, and what did you deliberately leave out? -->

**How does the agent decide when it has enough information to act?**
<!-- What's the stopping condition? How do you prevent the agent from over-querying the Airflow API, and how do you prevent under-investigation before a high-stakes action like a retry on a critical pipeline? -->

**How does your agent handle a code bug vs. a transient failure?**
<!-- Walk through how the agent would diagnose and respond to: (a) a column-not-found SQL error in a DAG, and (b) a Salesforce API 504. What's different about the tool use and decision path? -->

---

## 4. Production Gaps

**What corners did you cut under time pressure?**
<!-- Be direct about what's missing. This matters more than a polished answer. -->

**What would you add before shipping this to a real client?**
<!-- Prioritise the top 3–4 things. Why are these the highest priority? -->

**How did you think about the $3/day cost constraint?**
<!-- What decisions did this drive? What levers would you pull if the budget tightened further? -->

---

## 5. Evaluation

**What does your eval test?**
<!-- Describe your eval script/approach. What does it verify? -->

**What does your eval miss?**
<!-- Be honest about the limits of your approach. What would a production-grade eval harness look like? -->

**How would you define "correct" for the ambiguous events?**
<!-- There is no single right answer for ambiguous scenarios like report_export with 0 rows. How would you build ground truth for these over time? -->

---

## 6. Deployment Sketch

**How would you deploy this to GCP (or Azure, AWS)?**
<!-- A paragraph or bullet list is fine. What services would you use? What would the infrastructure look like? -->

**What observability would you wire up?**
<!-- What would you monitor, log, and alert on in production? -->

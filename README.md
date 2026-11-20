# Battery Energy Savings 

## Overview
This project asks you to analyse the real energy consumption of eleven residential customers in Portugal and determine how much money each customer could save by installing a home battery.

Energy costs in Portugal depend on which tariff a customer has contracted. The three available tariffs — Simples, Bi-Horária, and Tri-Horária — price electricity differently depending on the time of day. A well-sized battery can arbitrage these price differences by charging when electricity is cheap and discharging when it is expensive.

The analysis should be grounded in real metered data and based on Portuguese tariff rules. Your conclusions must be supported by the data, not by assumptions.

## Datasets
You will work with the following information:

Energy meter readings — provided as a single Parquet file containing all 11 customers. Each row records a date, an energy counter value in kWh, and a customer identifier. The counter is monotonically increasing; the energy consumed between two readings is the difference of their counter values. Readings are irregularly spaced and some samples are missing, so for each 15-minute billing interval you must estimate the energy consumed using linear interpolation between the two nearest available readings.

Simplified Portuguese energy tariffs:

| Tariff | Description |
|--------|-------------|
|Simples (Simple) |Same price at all hours|
|Bi-Horária (Two period) | Two prices: cheaper late night when the demand is lower.|
|Tri-Horária (Three Period) | Three prices: Similar to Bi-Horária but very expensive around times of highest demand (lunch/dinner); This tariff is mandatory for meters above a certain power rating.|

## Pricing

|Prices of active energy|Time period|€/kWh|
|-----------------------|-----------|-----|
|Simples		|-|0.1654|
|Bi-Horária	| Standard hours |0.1988|
|Bi-Horária	| Off-peak |0.1087|
|Tri-Horária	| Peak |0.2495|
|Tri-Horária	| Mid-Peak |	0.1690|
|Tri-Horária	| Off-peak |	0.1087|

## Time periods

|| Off-peak	| Standard hours |
|-|-----|--------|
|Bi-Horária	|22:00 às 08:00|	08:00 às 22:00|

|| Off-peak | Mid-peak | Peak |
|-|-----|-----|----|
|Tri-Horária|	22:00 às 08:00	|08:00 às 10:30	|10:30 às 13:00|
|||13:00 às 19:30	|19:30 às 21:00|
|||21:00 às 22:00	||

Note: For simplicity, we will consider that all prices will stay constant and apply uniformly across everyday of the week. We also ignore that there are different time-periods boundaries for winter and summer.

## Core Objectives

The project has two main objectives and an open-ended extension track.

### Objective 1 — Tariff Comparison & Customer Segmentation

For each of the 11 customers, use Tri-Horária as the baseline tariff and compute their annual electricity cost under it. Then compute the same cost under Simples and Bi-Horária and express each as a saving or surcharge relative to the Tri-Horária bill. This framing reflects the real constraint that Tri-Horária is mandatory for customers above a certain meter power rating — for those customers the alternative tariffs are not available, so the relative figures quantify what they cannot save; for the remaining customers, the comparison reveals whether switching away from Tri-Horária would be worthwhile.

As part of this objective, characterise the consumption patterns of the 11 customers and determine whether they fall into natural groups (e.g. daytime-heavy vs. night-heavy, peaky vs. flat, high vs. low overall consumption). Use these groups to explain why certain customers benefit more from time-of-use tariffs than others, and to set up a meaningful comparison for the battery analysis in Objective 2.

### Objective 2 — Battery Sizing

Model a battery storage system for each customer. The battery charges when the price of electricity is low (off-peak) and discharges to cover consumption when prices are high (peak or super-peak). Evaluate battery capacities in 5 kWh increments (e.g. 5, 10, 15, 20 kWh) and report the annual cost saving relative to the Tri-Horária baseline from Objective 1.

Use the following cost model to assess economic viability:

- **Total capital cost** = X + k × Y, where X = 1,250 € (inverter), Y = 850 € (cost per 5 kWh storage module), and k is the number of modules
- **Battery lifetime:** 10 years; ignore capacity degradation over that period
- **Payback period** = total capital cost ÷ annual saving
- **Charge/discharge power limit (kW):** 8
- **Round-trip efficiency:** 90%;
- **Useable Battery Capacity:** 80%
- 
For each customer, identify the capacity at which the marginal saving from adding one more module no longer reduces the payback period, and report the capacity that minimises payback time. Assume that the contracted tariff while evaluating the battery impact is Tri-Horária.

## Further Exploration (Optional)

After completing the two core objectives, students are encouraged to pursue one or more of the following directions. These are not exhaustive — well-motivated ideas beyond this list are welcome.

**Sensitivity analysis.** Electricity tariff prices and hardware costs change over time. How sensitive is the payback period to changes in energy prices or battery costs? Present a range of scenarios.

**Real-time spot prices.** The core analysis assumes fixed time-of-use tariff prices. In practice, Portugal's electricity market also exposes a spot price (OMIE/MIBEL), published at 15-minute resolution, that fluctuates with supply and demand. Rather than implementing a full analysis, discuss the strategies by which this information could be incorporated into the battery dispatch model — for example, threshold-based rules, day-ahead price forecasting, or optimisation approaches — and reflect on the trade-offs each strategy implies in terms of complexity, data requirements, and potential benefit. For context, explore the terms: Continuous Intraday Market (SIDC) and Grid Imbalance Market (15-Minute Settlement).

**Renter turnover detection.** Assuming the 11 customers are rental properties, produce a time-series chart of daily or weekly energy consumption for each property over the full period and discuss whether it is possible to identify moments where the occupant may have changed. A sustained drop to near-zero consumption followed by a return to normal levels is a candidate signal — however, care must be taken not to conflate a renter change with an extended absence such as a holiday, which can produce a similar pattern. Discuss what additional features of the consumption signal might help distinguish between the two cases, and reflect on the limitations of drawing such conclusions from meter data alone.

**Processing Frameworks.** For Objective 1, present solution for more than one processing framework: Spark dataframes, Spark SQl, DuckDB. Discuss the performance results obtained in line with possible production-scale implementation, targeting the full customer base of an energy provider, such as EDP Comercial.

## Under Specification

Where the problem is under-specified, students are expected to make reasonable simplifying assumptions. Each assumption must be clearly stated in the report and briefly justified.

## Deliverables

Groups submit the following.

**Code** — well-structured, runnable code (Jupyter Python notebook) covering data ingestion, the 15-minute interpolation, tariff cost calculation, and the battery simulation. A README must explain how to reproduce every result in the report. Should also include AI prompting used to develop the code or explore possible approaches.

**Analysis report** (at most 10 pages) — structured as follows:

1. Data exploration: summary statistics and visualisations of consumption patterns across customers.
2. Tariff comparison: per-customer results with explanation of which consumption features drive the best tariff choice.
3. Battery analysis: cost savings vs. battery size, with a recommendation per customer.
4. Optional extension: if pursued, a focused section presenting methodology, findings, and limitations.
5. Conclusions: key findings, limitations of the models, and suggestions for a production-scale implementation, targeting the full customer base of an energy provider, such as EDP Comercial.

## Assessment Criteria

| Criterion |	Weight |	What we look for |
|-----------|--------|-------------------|
|Data engineering|	15%|	Correct interpolation; handling of missing samples; reproducible pipeline|
|Tariff analysis|	25%|	Accurate cost calculation; clear explanation of per-customer differences|
|Battery modelling|	30%	|Sound simulation logic; appropriate range of configurations evaluated; honest treatment of assumptions|
|Visualisation & communication|	10%|	Charts that make the findings legible; report and presentation accessible to a non-specialist|
|Critical thinking	|20%	|Limitations acknowledged; results questioned; optional extension or original angle pursued with rigour|

Groups will be penalised for hardcoded results that cannot be reproduced by running the submitted code, and for conclusions that contradict the data without explanation.

## Group Composition

Groups can have a maximum of 3 members, no exceptions, where at most 2 are human. Groups are free to use AI tooling as a member of the team, but must declare it and report the prompting used. Group composition should be credible given the scope and quality of the work presented. Using AI is not a detrimental factor per se. Some groups may be called to Q&A session as part of the grading process.

## Deadline

**December 9, 23:59**

Submissions after the deadline will incur a penalty of 0.25 points (on a 20-point scale) per day late. The penalty accumulates until the grade reaches 10.0, at which point no further deduction is applied. The hard deadline, beyond which no submission will be accepted, is **January 4, 2027**.

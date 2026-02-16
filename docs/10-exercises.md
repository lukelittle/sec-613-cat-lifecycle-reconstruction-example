# Student Exercises

## Exercise 1: Add a New Event Type (Beginner)

**Goal**: Extend the system to handle REJECT events (when a venue rejects an order).

**Tasks**:
1. Add REJECT to the event schema
2. Update linkage construction to create REJECT edges
3. Update lifecycle materialization to track rejections
4. Add validation rule: "Route must have outcome (ACK or REJECT)"

**Expected outcome**: System processes REJECT events and detects routes with no outcome.

**Hints**:
- Modify `spark/lifecycle_job/lifecycle_streaming.py`
- Add REJECT case to `construct_edges()` function
- Update lifecycle status logic

## Exercise 2: Tune Watermark Delay (Intermediate)

**Goal**: Understand the trade-off between latency and late data handling.

**Tasks**:
1. Run generator in `late` mode with 30-second watermark
2. Count late events and reconciliations
3. Change watermark to 10 seconds
4. Compare late event rates and exception counts
5. Change watermark to 60 seconds
6. Compare again

**Questions to answer**:
- How does watermark delay affect late event detection?
- What's the impact on processing latency?
- What's the optimal delay for this workload?

**Deliverable**: Report with graphs showing late event rate vs. watermark delay.

## Exercise 3: Implement Lifecycle Finalization (Intermediate)

**Goal**: Add two-phase lifecycle concept (provisional vs. finalized).

**Tasks**:
1. Mark lifecycles as "provisional" within watermark window
2. Mark lifecycles as "finalized" after watermark passes
3. Emit finalization events to audit topic
4. Prevent updates to finalized lifecycles (or log as exceptions)

**Expected outcome**: Clear distinction between provisional and finalized states.

**Hints**:
- Use watermark timestamp to determine finalization
- Add `is_finalized` field to lifecycle schema
- Emit audit event when lifecycle transitions to finalized

## Exercise 4: Build Exception Dashboard (Intermediate)

**Goal**: Visualize data quality exceptions.

**Tasks**:
1. Consume `cat.exceptions.v1` topic
2. Aggregate exceptions by type and time window
3. Create dashboard showing:
   - Exception count by type (bar chart)
   - Exception rate over time (line chart)
   - Top 10 orders with most exceptions
4. Use tool of choice (Grafana, Kibana, custom web app)

**Deliverable**: Dashboard screenshot and setup instructions.

## Exercise 5: Add Sequence Validation (Advanced)

**Goal**: Detect temporal sequence violations.

**Tasks**:
1. Add validation: FILL timestamp must be >= ACK timestamp
2. Add validation: ACK timestamp must be >= ROUTE timestamp
3. Add validation: ROUTE timestamp must be >= NEW timestamp
4. Emit exceptions for violations
5. Test with chaos mode generator

**Expected outcome**: System detects and reports sequence violations.

**Hints**:
- Store event timestamps in lifecycle state
- Compare timestamps during validation
- Handle missing events gracefully

## Exercise 6: Implement Comparative Review (Advanced)

**Goal**: Reconcile CAT data with "internal" broker records.

**Tasks**:
1. Generate synthetic "broker internal" events (slightly different from CAT events)
2. Join CAT lifecycles with internal records by customer_order_id
3. Detect discrepancies:
   - Quantity mismatches
   - Price differences
   - Missing events
4. Emit reconciliation exceptions

**Expected outcome**: System identifies differences between CAT and internal data.

**Deliverable**: Reconciliation report showing match rate and discrepancy types.

## Exercise 7: Optimize Spark Performance (Advanced)

**Goal**: Improve throughput and reduce latency.

**Tasks**:
1. Baseline: Measure current throughput (events/sec) and latency (p95, p99)
2. Tune shuffle partitions
3. Tune batch interval
4. Tune state store configuration
5. Measure again and compare

**Metrics to track**:
- Input rate (events/sec)
- Processing rate (events/sec)
- Batch duration
- State memory usage
- Checkpoint duration

**Deliverable**: Performance tuning report with before/after metrics.

## Exercise 8: Add ALLOCATE Event Type (Advanced)

**Goal**: Handle post-execution allocation to sub-accounts.

**Background**: After execution, a broker may allocate fills to multiple sub-accounts.

**Tasks**:
1. Define ALLOCATE event schema (references exec_id, specifies sub-account and qty)
2. Update linkage construction
3. Update lifecycle to track allocations
4. Add validation: Sum of allocations must equal execution quantity

**Expected outcome**: System tracks allocations and validates totals.

## Exercise 9: Implement Idempotency Testing (Advanced)

**Goal**: Verify duplicate event handling.

**Tasks**:
1. Run generator in `duplicate` mode
2. Verify duplicates are dropped (check Spark metrics)
3. Verify lifecycle snapshots are correct (no double-counting)
4. Inject duplicates with different ts_ingest but same event_id
5. Verify behavior

**Deliverable**: Test report confirming idempotency guarantees.

## Exercise 10: Build Lifecycle Query API (Advanced)

**Goal**: Provide REST API for lifecycle queries.

**Tasks**:
1. Create Lambda function that queries DynamoDB
2. Implement endpoints:
   - GET /lifecycle/{customer_order_id}
   - GET /lifecycle?symbol={symbol}&date={date}
   - GET /lifecycle?status={status}
3. Add pagination for list queries
4. Add caching (optional)

**Expected outcome**: Working API with sub-second query latency.

**Deliverable**: API documentation and example queries.

## Bonus Challenge: Multi-Day Lifecycle

**Goal**: Handle orders that span multiple days.

**Scenario**: Order placed at 3:59 PM, partially filled, then filled next day at 9:31 AM.

**Tasks**:
1. Modify partitioning strategy (currently by date)
2. Handle date boundaries in lifecycle aggregation
3. Test with synthetic multi-day events

**Expected outcome**: Correct lifecycle reconstruction across date boundaries.

## Submission Guidelines

For each exercise:
1. **Code changes**: Submit modified files or patches
2. **Documentation**: Explain your approach and design decisions
3. **Testing**: Show test results or screenshots
4. **Reflection**: What did you learn? What was challenging?

## Grading Rubric

- **Correctness** (40%): Does it work as specified?
- **Code quality** (20%): Clean, readable, well-structured?
- **Testing** (20%): Adequate test coverage and validation?
- **Documentation** (10%): Clear explanations?
- **Insights** (10%): Thoughtful analysis and reflection?

## Getting Help

- Review documentation in `docs/`
- Check Spark Structured Streaming guide
- Ask questions in class discussion forum
- Office hours: [Schedule TBD]

## Recommended Order

1. Start with Exercise 1 (new event type) to understand the codebase
2. Do Exercise 2 (watermark tuning) to understand late data
3. Pick exercises based on your interests (dashboards, performance, APIs)
4. Attempt advanced exercises after mastering intermediate ones

Good luck and have fun exploring streaming event sourcing!

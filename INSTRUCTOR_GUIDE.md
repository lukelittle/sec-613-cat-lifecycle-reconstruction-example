# Instructor Guide

## Course Integration

This project is designed for graduate-level computer science courses covering:
- Cloud computing and distributed systems
- Streaming data processing
- Event-driven architecture
- Infrastructure as code
- Real-world system design

**Recommended Prerequisites**:
- Basic Python programming
- Understanding of databases and SQL
- Familiarity with cloud concepts (VMs, storage, networking)
- Command-line proficiency

**Estimated Time Commitment**:
- Initial setup: 2-3 hours
- Core exercises: 10-15 hours
- Advanced exercises: 15-20 hours
- Total: 25-40 hours (suitable for semester project)

## Learning Outcomes

By completing this project, students will be able to:

1. **Explain** event sourcing principles and their benefits for audit trails
2. **Design** streaming architectures using Kafka and Spark
3. **Implement** idempotency and late data handling in distributed systems
4. **Deploy** serverless infrastructure using Terraform
5. **Validate** data quality in streaming pipelines
6. **Optimize** streaming job performance
7. **Troubleshoot** distributed system issues
8. **Estimate** and manage cloud costs

## Course Schedule Suggestions

### Week 1-2: Concepts and Setup
- **Lecture**: Event sourcing, CAT overview, regulatory context
- **Reading**: docs/00-overview.md through docs/04-streaming-architecture.md
- **Lab**: Local setup with Docker Compose
- **Assignment**: Run demo locally, answer conceptual questions

### Week 3-4: Implementation Deep Dive
- **Lecture**: Spark Structured Streaming, watermarks, state management
- **Reading**: docs/05-spark-lifecycle-job.md, blog post
- **Lab**: Trace event flow through pipeline
- **Assignment**: Exercise 1 (Add REJECT event type)

### Week 5-6: Late Data and Reconciliation
- **Lecture**: Event time vs. processing time, watermarks, reconciliation
- **Reading**: docs/02-event-sourcing-for-cat.md (review)
- **Lab**: Experiment with watermark delays
- **Assignment**: Exercise 2 (Tune watermark delay)

### Week 7-8: Data Quality
- **Lecture**: Validation rules, exception handling, comparative review
- **Reading**: docs/09-data-quality-controls.md
- **Lab**: Inject chaos mode events, observe exceptions
- **Assignment**: Exercise 5 (Add sequence validation)

### Week 9-10: AWS Deployment
- **Lecture**: Serverless architecture, Terraform, cost management
- **Reading**: docs/06-deploy-aws.md, docs/12-cost-and-cleanup.md
- **Lab**: Deploy to AWS, monitor costs
- **Assignment**: Deploy and run demo on AWS

### Week 11-12: Performance and Optimization
- **Lecture**: Spark tuning, scalability, monitoring
- **Reading**: docs/05-spark-lifecycle-job.md (performance section)
- **Lab**: Profile Spark job, identify bottlenecks
- **Assignment**: Exercise 7 (Optimize Spark performance)

### Week 13-14: Advanced Topics
- **Lecture**: Production considerations, comparative review, APIs
- **Reading**: docs/08-observe-and-query.md
- **Lab**: Build query API or dashboard
- **Assignment**: Exercise 10 (Build lifecycle query API)

### Week 15: Final Presentations
- Students present their implementations
- Demo advanced features they added
- Discuss challenges and learnings

## Grading Rubric

### Project Components (100 points total)

**Setup and Basic Understanding (20 points)**
- Local environment setup: 5 points
- Successfully run demo: 5 points
- Conceptual questions answered: 10 points

**Core Exercises (40 points)**
- Exercise 1 (Add event type): 10 points
- Exercise 2 (Tune watermark): 10 points
- Exercise 5 (Sequence validation): 10 points
- Exercise 9 (Idempotency testing): 10 points

**AWS Deployment (20 points)**
- Infrastructure deployed: 10 points
- Cost management demonstrated: 5 points
- Monitoring configured: 5 points

**Advanced Exercise (15 points)**
- Choose one: Exercise 6, 7, or 10
- Correctness: 10 points
- Documentation: 5 points

**Final Presentation (5 points)**
- Clear explanation: 3 points
- Demo quality: 2 points

### Detailed Rubric per Exercise

**Correctness (50%)**
- Code works as specified
- Handles edge cases
- No breaking changes

**Code Quality (20%)**
- Clean, readable code
- Proper error handling
- Follows project conventions

**Testing (15%)**
- Adequate test coverage
- Tests pass
- Edge cases tested

**Documentation (10%)**
- Clear explanations
- Examples provided
- Updated relevant docs

**Insights (5%)**
- Thoughtful analysis
- Lessons learned
- Trade-offs discussed

## AWS Account Setup

### Option 1: AWS Academy (Recommended)
- Free for educational institutions
- Pre-configured accounts for students
- Built-in cost controls
- Request access: https://aws.amazon.com/education/awseducate/

### Option 2: AWS Educate
- Free credits for students
- Self-service account creation
- Apply: https://aws.amazon.com/education/awseducate/

### Option 3: Personal Accounts
- Students use personal AWS accounts
- Provide cost estimates upfront
- Require billing alerts
- Consider reimbursement policy

### Cost Management for Students

**Recommended Budget**: $300-400 per student for semester

**Cost Controls**:
1. Require `low_cost_mode = true` in Terraform
2. Set billing alerts at $50, $100, $150
3. Require daily cost checks
4. Mandate cleanup after each session
5. Provide cleanup checklist

**Sample Billing Alert**:
```bash
aws cloudwatch put-metric-alarm \
  --alarm-name student-billing-alert \
  --alarm-description "Alert at $100" \
  --metric-name EstimatedCharges \
  --namespace AWS/Billing \
  --statistic Maximum \
  --period 21600 \
  --evaluation-periods 1 \
  --threshold 100 \
  --comparison-operator GreaterThanThreshold
```

## Teaching Tips

### Lecture Suggestions

**Week 1: Hook Students with Real-World Context**
- Start with Flash Crash video
- Explain why regulators need CAT
- Show scale: billions of events per day
- Connect to distributed systems concepts

**Week 3: Live Coding Session**
- Walk through Spark job code
- Explain watermarking with visual timeline
- Show state management in action
- Debug a simple issue together

**Week 5: Guest Speaker (Optional)**
- Invite fintech engineer
- Discuss real CAT implementation challenges
- Q&A about production systems

### Lab Session Structure

1. **Introduction (10 min)**: Explain lab goals
2. **Guided Walkthrough (20 min)**: Demonstrate key steps
3. **Independent Work (50 min)**: Students work on exercises
4. **Troubleshooting (15 min)**: Address common issues
5. **Wrap-up (5 min)**: Preview next week

### Common Student Questions

**Q: Why not use a traditional database?**
A: Event sourcing provides complete audit trail, time travel, and handles late data naturally. Traditional databases lose history.

**Q: What's the difference between event time and processing time?**
A: Event time is when event occurred (source timestamp). Processing time is when we process it. They differ due to network delays, failures, etc.

**Q: Why do we need watermarks?**
A: Watermarks let us bound state size and decide when to finalize results. Without them, we'd wait forever for late events.

**Q: How does idempotency work?**
A: We deduplicate on event_id. Spark maintains a hash set of seen IDs within the watermark window.

**Q: Why is this so expensive on AWS?**
A: Streaming systems run 24/7. Use low_cost_mode and stop when not in use. Real production systems justify the cost.

## Assessment Ideas

### Quizzes
- Event sourcing concepts
- Kafka topic design
- Watermark semantics
- Data quality rules

### Coding Challenges
- Implement new event type in 30 minutes
- Debug broken Spark job
- Optimize slow query
- Fix validation rule

### Design Exercises
- Design schema for new requirement
- Propose scaling strategy
- Plan disaster recovery
- Estimate costs for production

### Written Assignments
- Compare event sourcing vs. CRUD
- Analyze CAT regulatory requirements
- Propose improvements to system
- Reflect on challenges faced

## Extension Projects

For advanced students:

1. **Real-Time Dashboard**: Build web UI showing live metrics
2. **Machine Learning**: Detect anomalous trading patterns
3. **Multi-Region**: Deploy across multiple AWS regions
4. **Performance Testing**: Load test with millions of events
5. **Compliance Reporting**: Generate regulatory reports
6. **API Development**: Build REST API for lifecycle queries
7. **Visualization**: Create lifecycle graph visualizer
8. **Alerting**: Implement PagerDuty/Slack alerts
9. **Cost Optimization**: Implement auto-scaling policies
10. **Security**: Add encryption, authentication, authorization

## Resources for Instructors

### Recommended Reading
- "Designing Data-Intensive Applications" by Martin Kleppmann
- "Streaming Systems" by Tyler Akidau et al.
- "Building Event-Driven Microservices" by Adam Bellemare
- Spark Structured Streaming documentation
- AWS Well-Architected Framework

### Video Resources
- Spark Summit talks on Structured Streaming
- AWS re:Invent sessions on MSK and EMR
- Martin Kleppmann's talks on event sourcing

### Related Projects
- Kafka Streams examples
- Flink streaming examples
- Real-time analytics pipelines

## Support and Community

### Office Hours Topics
- Terraform debugging
- Spark performance tuning
- AWS cost optimization
- Exercise guidance

### Discussion Forum
- Create dedicated forum/Slack channel
- Encourage peer help
- Post common issues and solutions
- Share interesting findings

### TA Responsibilities
- Review exercise submissions
- Help with AWS setup
- Monitor student costs
- Grade presentations

## Continuous Improvement

### Collect Feedback
- Mid-semester survey
- End-of-semester survey
- Exercise difficulty ratings
- Time spent tracking

### Iterate
- Adjust exercise difficulty
- Update cost estimates
- Improve documentation
- Add clarifying examples

### Track Metrics
- Exercise completion rates
- Average time per exercise
- Common errors
- AWS costs per student

## Legal and Compliance

### Disclaimers
- Emphasize educational nature
- Not production CAT system
- Not legal/compliance advice
- Synthetic data only

### Data Privacy
- No real market data
- No PII in examples
- Secure AWS accounts
- Follow university policies

### Academic Integrity
- Encourage collaboration on concepts
- Individual work on exercises
- Cite sources in reports
- Use plagiarism detection

## Contact

For questions about this project:
- Open GitHub issue
- Email course instructor
- Post in course forum

## Acknowledgments

This project was created for UNC Charlotte graduate students. Contributions from students and instructors are welcome!

---

**Ready to teach streaming systems?** Start with the [README](README.md) and [docs/00-overview.md](docs/00-overview.md)!

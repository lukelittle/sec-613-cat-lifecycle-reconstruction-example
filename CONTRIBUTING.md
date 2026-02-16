# Contributing to CAT Demo Project

Thank you for your interest in improving this educational project! This guide will help you contribute effectively.

## Types of Contributions

We welcome:
- Bug fixes
- Documentation improvements
- New exercises for students
- Performance optimizations
- Additional event types
- Enhanced validation rules
- Visualization tools
- Test coverage improvements

## Getting Started

### 1. Fork and Clone

```bash
git clone https://github.com/your-username/cat-demo.git
cd cat-demo
```

### 2. Set Up Local Environment

```bash
# Start local Kafka and Spark
make local-up

# Create topics
make local-topics
```

### 3. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/your-bug-fix
```

## Development Workflow

### Testing Locally

```bash
# Test Spark job
make local-spark

# Test event generator
make local-generate

# Verify output
./tools/tail_topics.sh cat.lifecycle.v1
```

### Code Style

**Python**:
- Follow PEP 8
- Use type hints where appropriate
- Add docstrings to functions
- Keep functions focused and small

**Terraform**:
- Use consistent naming conventions
- Add comments for complex logic
- Use variables for configurable values
- Tag all resources

### Documentation

When adding features:
- Update relevant docs in `docs/`
- Add examples to README if applicable
- Include Mermaid diagrams for complex flows
- Update blog post if conceptually relevant

## Submitting Changes

### 1. Commit Your Changes

```bash
git add .
git commit -m "feat: Add REJECT event type support"
```

Use conventional commit messages:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `test:` Test additions/changes
- `refactor:` Code refactoring
- `perf:` Performance improvements

### 2. Push to Your Fork

```bash
git push origin feature/your-feature-name
```

### 3. Create Pull Request

- Go to GitHub and create a pull request
- Describe what you changed and why
- Reference any related issues
- Include screenshots if UI changes
- Ensure all checks pass

## Pull Request Checklist

- [ ] Code follows project style guidelines
- [ ] Documentation updated
- [ ] Tests added/updated (if applicable)
- [ ] Local testing completed
- [ ] No breaking changes (or clearly documented)
- [ ] Commit messages are clear
- [ ] PR description is complete

## Adding New Event Types

Example: Adding REJECT event type

1. **Update Event Schema** (`spark/lifecycle_job/lifecycle_streaming.py`):
```python
# Add to EVENT_SCHEMA
StructField("reject_reason", StringType(), True)
```

2. **Update Linkage Construction**:
```python
elif event.event_type == "REJECT":
    edges.append({
        "edge_type": "REJECT",
        "source_id": event.route_id,
        "target_id": f"REJECT-{event.route_id}",
        ...
    })
```

3. **Update Lifecycle Materialization**:
```python
elif event["event_type"] == "REJECT":
    lifecycle["status"] = "REJECTED"
    lifecycle["reject_reason"] = event["reject_reason"]
```

4. **Update Generator** (`services/event_generator/generator.py`):
```python
def generate_reject_event(self, ...):
    # Implementation
```

5. **Add Documentation**:
- Update `docs/03-identifiers-and-linkages.md`
- Add example to `docs/07-run-demo.md`
- Create exercise in `docs/10-exercises.md`

6. **Test**:
```bash
# Generate events with REJECT
python services/event_generator/generator.py --mode chaos

# Verify processing
./tools/tail_topics.sh cat.lifecycle.v1 | grep REJECTED
```

## Adding New Validation Rules

Example: Validate execution price within limit price range

1. **Add Validation Function** (`spark/lifecycle_job/lifecycle_streaming.py`):
```python
def validate_execution_price(lifecycle):
    exceptions = []
    
    if lifecycle.limit_price:
        for execution in lifecycle.executions:
            if execution.price > lifecycle.limit_price * 1.05:
                exceptions.append({
                    "type": "PRICE_ABOVE_LIMIT",
                    "severity": "WARNING"
                })
    
    return exceptions
```

2. **Integrate into Pipeline**:
```python
exceptions = lifecycles.flatMap(validate_execution_price)
```

3. **Document** in `docs/09-data-quality-controls.md`

4. **Test**:
```bash
# Generate events with price violations
# Verify exceptions appear in cat.exceptions.v1
```

## Adding New Exercises

1. Create exercise in `docs/10-exercises.md`:
```markdown
## Exercise X: Your Exercise Title (Difficulty)

**Goal**: Clear learning objective

**Tasks**:
1. Step 1
2. Step 2
3. Step 3

**Expected outcome**: What students should achieve

**Hints**:
- Helpful tip 1
- Helpful tip 2

**Deliverable**: What to submit
```

2. Provide solution in separate file (optional):
```
solutions/exercise-X-solution.md
```

## Improving Documentation

- Fix typos and grammar
- Add clarifying examples
- Improve diagrams
- Add troubleshooting tips
- Expand explanations

## Performance Improvements

When optimizing:
- Measure before and after
- Document trade-offs
- Update configuration examples
- Add to `docs/05-spark-lifecycle-job.md`

## Testing Guidelines

### Unit Tests

```python
def test_construct_edges_for_new_event():
    event = {
        "event_type": "NEW",
        "customer_order_id": "COID-123",
        "firm_order_id": "FOID-456",
        "ts_event": 1710000000000
    }
    
    edges = construct_edges(**event)
    
    assert len(edges) == 1
    assert edges[0]["edge_type"] == "ROOT"
```

### Integration Tests

```python
def test_end_to_end_lifecycle():
    # Generate events
    events = generate_test_lifecycle()
    
    # Process through pipeline
    produce_to_kafka(events)
    
    # Wait for processing
    time.sleep(30)
    
    # Verify lifecycle
    lifecycle = query_lifecycle(events[0]["customer_order_id"])
    assert lifecycle["status"] == "FILLED"
```

## Code Review Process

Maintainers will review for:
- Correctness
- Code quality
- Documentation completeness
- Test coverage
- Performance impact
- Educational value

## Questions?

- Open an issue for discussion
- Ask in course forum
- Email instructor

## Recognition

Contributors will be acknowledged in:
- README.md contributors section
- Release notes
- Course materials (with permission)

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

Thank you for helping make this project better for students!

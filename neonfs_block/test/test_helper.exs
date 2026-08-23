# Benchmarks are opt-in: they report numbers rather than asserting a
# threshold a slower runner would fail, so they are noise in a normal run.
ExUnit.start(exclude: [:benchmark])
